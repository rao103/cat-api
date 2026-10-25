using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ProductionIngestor.Api.Interfaces;
using ProductionIngestor.Api.Results;
using ProductionIngestor.Api.Security;
using ProductionIngestor.Api.Triggering;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using System.Globalization;
using System.Security.Claims;

namespace ProductionIngestor.Api.Controllers.v1;

[ApiController]
[Route("v{version:apiVersion}/status")]
[ApiVersion("1.0")]
[Authorize]
public class StatusController : ControllerBase
{
    private readonly Interfaces.ITelemetry<StatusController> _telemetry;

    private readonly IExtractionCursorRepository _cursorRepository;

    public StatusController(IExtractionCursorRepository cursorRepository,
        Interfaces.ITelemetry<StatusController> telemetry)
    {
        _cursorRepository = cursorRepository;
        _telemetry = telemetry;
    }

    [HttpGet("{windfarmId}/{sourceType}")]
    public async Task<JsonResult> Get(string windfarmId, string sourceType, CancellationToken cancellationToken)
    {
        var callerId = GetCallerId();
        if(!int.TryParse(windfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedWindfarmId) || parsedWindfarmId <= 0)
        {
            _telemetry.Error("Cursor status lookup rejected because the windfarm ID is invalid.",
                new ArgumentException("Windfarm Id should be positive integer."), callerId: callerId);
            return new JsonResult(new { Error = "Windfarm Id should be positive integer" }) { StatusCode = 404 };
        }

        ExtractionLeaseModel? current;
        try
        {
            current = await _cursorRepository.GetCurrentAsync(parsedWindfarmId, sourceType, cancellationToken);
        }
        catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _telemetry.Error("Cursor status lookup failed.", error, callerId: callerId);
            throw;
        }

        if (current is null)
        {
            _telemetry.Error("Cursor status lookup returned no cursor.",
                new KeyNotFoundException($"No cursor found for {windfarmId}/{sourceType}."), callerId: callerId);
            return new JsonResult(new Result(new KeyNotFoundException($"No cursor found for {windfarmId}/{sourceType}."))) { StatusCode = 404 };
        }

        _telemetry.Information("Cursor status lookup completed.", callerId: callerId);
        return new JsonResult(new Result(current));
    }

    [HttpGet("requests/{runId}")]
    public async Task<IActionResult> GetRequest(string runId, [FromServices] RefreshStatusService service,
        CancellationToken cancellationToken)
    {
        var callerId = GetCallerId();
        try
        {
            var result = await service.GetAsync(new ExtractionHistoryQueryModel(runId), cancellationToken);
            _telemetry.Information("Request status lookup completed.", runId, callerId: callerId);
            return Ok(result);
        }
        catch (ArgumentException error)
        {
            _telemetry.Error("Request status lookup rejected.", error, runId, callerId);
            return BadRequest(new { Error = error.Message });
        }
        catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _telemetry.Error("Request status lookup failed.", error, runId, callerId);
            throw;
        }
    }

    private string? GetCallerId() => User.GetCallerId();
}
