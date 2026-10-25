using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ProductionIngestor.Api.Contracts;
using ProductionIngestor.Api.Results;
using ProductionIngestor.Api.Interfaces;
using ProductionIngestor.Api.Monitoring;
using ProductionIngestor.Api.Security;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Refresh;
using k8s.Autorest;
using System.Net;
using System.Security.Claims;

namespace ProductionIngestor.Api.Controllers.v1;

[ApiController]
[Route("v{version:apiVersion}/refresh")]
[ApiVersion("1.0")]
[Authorize]
public class RefreshController : ControllerBase
{
    private readonly ProductionRefreshPlanner _planner;
    private readonly IIngestionJobTrigger _jobTrigger;
    private readonly ITelemetry<RefreshController> _telemetry;

    public RefreshController(ProductionRefreshPlanner planner, IIngestionJobTrigger jobTrigger, ITelemetry<RefreshController> telemetry)
    {
        _planner = planner;
        _jobTrigger = jobTrigger;
        _telemetry = telemetry;
    }

    [HttpPost]
    public async Task<JsonResult> Post([FromBody] RefreshRequestDto request, CancellationToken cancellationToken)
    {
        var callerId = GetCallerId();
        var runId = Guid.NewGuid().ToString("N");
        try
        {
            var oid = User.GetObjectId();
            if (string.IsNullOrWhiteSpace(oid))
            {
                _telemetry.Error("Refresh request rejected because the caller OID is missing.",
                    new UnauthorizedAccessException("An authenticated caller OID is required for refresh audit."),
                    runId, callerId);
                return new JsonResult(new { Error = "An authenticated caller OID is required for refresh audit." }) { StatusCode = 403 };
            }

            var refreshRequest = new RefreshRequestModel(request.WindfarmId, request.WindfarmName,
                request.StartDate, request.EndDate, oid, request.Reason, runId);

            // Validates the request and fails fast before any Job is created.
            _planner.Plan(refreshRequest);

            var jobName = await _jobTrigger.TriggerAsync(refreshRequest, cancellationToken);
            _telemetry.Information("Refresh job accepted.", runId, jobName, callerId);
            return new JsonResult(new Result(new { JobName = jobName, RunId = runId,
                StatusUrl = $"/v1/status/requests/{runId}" })) { StatusCode = 202 };
        }
        catch (ArgumentException ex)
        {
            _telemetry.Error("Refresh request rejected.", ex, runId, callerId);
            return new JsonResult(new Result(ex)) { StatusCode = 400 };
        }
        catch (HttpOperationException error) when (error.Response.StatusCode == HttpStatusCode.Conflict)
        {
            _telemetry.Error("Refresh request conflicted with an existing run.", error, runId, callerId);
            return new JsonResult(new { Error = "The generated refresh run ID already exists." }) { StatusCode = 409 };
        }
        catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _telemetry.Error("Refresh job submission failed.", error, runId, callerId);
            throw;
        }
    }

    private string? GetCallerId() => User.GetCallerId();
}
