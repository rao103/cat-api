using Centralize.Api.Interfaces;
using Centralize.Api.Security;
using Centralize.Api.SourceTypes.Production.Contracts;
using Centralize.Core.Interfaces;
using Centralize.Core.Models;
using Centralize.Core.Validators;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Centralize.Api.Controllers.v1;

[ApiController]
[Route("v{version:apiVersion}")]
[ApiVersion("1.0")]
[Authorize]
public class RefreshController(IOperationRepository repository, IIngestionJobTrigger jobTrigger,
    ILogger<RefreshController> logger) : ControllerBase
{
    [HttpPost("windfarms/{windfarmId:int}/refresh")]
    public Task<IActionResult> PostAll(int windfarmId, RefreshDates request, CancellationToken cancellationToken) =>
        TriggerAsync(windfarmId, null, request, cancellationToken);

    [HttpPost("windfarms/{windfarmId:int}/sources/{sourceType}/refresh")]
    public Task<IActionResult> PostSource(int windfarmId, string sourceType, RefreshDates request, CancellationToken cancellationToken) =>
        TriggerAsync(windfarmId, sourceType, request, cancellationToken);

    [HttpPost("refresh")]
    public Task<IActionResult> Post(RefreshRequestDto request, CancellationToken cancellationToken)
    {
        if (!int.TryParse(request.WindfarmId, out var farm))
            return Task.FromResult<IActionResult>(BadRequest(new { Error = "WindfarmId must be a positive integer." }));
        return TriggerAsync(farm, "Production", new(request.StartDate, request.EndDate), cancellationToken, request.Reason);
    }

    private async Task<IActionResult> TriggerAsync(int farm, string? source, RefreshDates dates,
        CancellationToken cancellationToken, string? reason = null)
    {
        var caller = User.GetObjectId();
        if (string.IsNullOrWhiteSpace(caller)) return StatusCode(403);
        OperationRequestModel request;
        IReadOnlyList<RefreshSourceModel> sources;
        try
        {
            request = OperationRequestValidator.Normalize(new(farm, dates.StartDate, dates.EndDate, caller, source, reason));
            sources = await repository.GetRefreshSourcesAsync(request, cancellationToken);
        }
        catch (ArgumentException error) { return BadRequest(new { Error = error.Message }); }
        catch (KeyNotFoundException error) { return NotFound(new { Error = error.Message }); }

        // Validate every target before creating the identity or submitting any Jobs.
        jobTrigger.ValidateSources(sources);
        var operation = await repository.CreateAsync(caller, cancellationToken);
        var jobs = new List<RefreshJobResult>();
        foreach (var target in sources)
        {
            var runId = Guid.NewGuid().ToString("N");
            try
            {
                var name = await jobTrigger.TriggerAsync(request, target, operation.OperationId, runId, cancellationToken);
                jobs.Add(new(target.SourceType, runId, name, null));
            }
            catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                logger.LogError(error, "Job submission failed for operation {OperationId}, source {SourceType}, run {RunId}.",
                    operation.OperationId, target.SourceType, runId);
                jobs.Add(new(target.SourceType, runId, null, "Job submission failed; check API logs before retrying."));
            }
        }
        var response = new
        {
            operationId = operation.OperationId,
            sourceTypes = sources.Select(item => item.SourceType).ToArray(),
            jobs,
            statusUrl = $"/v1/operations/{operation.OperationId}"
        };
        if (jobs.Any(job => job.Error is not null)) return StatusCode(502, response);
        return Accepted(response.statusUrl, response);
    }
}

public sealed record RefreshDates(DateOnly StartDate, DateOnly EndDate);
public sealed record RefreshJobResult(string SourceType, string RunId, string? JobName, string? Error);
