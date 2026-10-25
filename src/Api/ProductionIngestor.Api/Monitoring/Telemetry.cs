using ProductionIngestor.Api.Interfaces;

namespace ProductionIngestor.Api.Monitoring;

public sealed class Telemetry<TCategory>(ILogger<TCategory> logger) : ITelemetry<TCategory>
{
    public void Information(string message, string? runId = null, string? jobName = null, string? callerId = null) =>
        logger.LogInformation("{Message} RunId={RunId}, JobName={JobName}, CallerId={CallerId}.", message, runId, jobName, callerId);

    public void Error(string message, Exception error, string? runId = null, string? callerId = null) =>
        logger.LogError(error, "{Message} RunId={RunId}, CallerId={CallerId}.", message, runId, callerId);
}