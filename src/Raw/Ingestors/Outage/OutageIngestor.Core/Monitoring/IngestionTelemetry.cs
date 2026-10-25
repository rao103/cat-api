using Microsoft.Extensions.Logging;
using OutageIngestor.Interfaces;

namespace OutageIngestor.Monitoring;

public sealed class IngestionTelemetry<TCategory>(ILogger<TCategory> logger) : IIngestionTelemetry<TCategory>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => logger.BeginScope(state);

    public void Information(string message, string? runId = null, string? jobName = null, string? callerId = null) =>
        logger.LogInformation("{Message} RunId={RunId}, JobName={JobName}, CallerId={CallerId}.",
            message, runId, jobName, callerId);

    public void Warning(string message, string? runId = null, string? jobName = null, string? callerId = null) =>
        logger.LogWarning("{Message} RunId={RunId}, JobName={JobName}, CallerId={CallerId}.",
            message, runId, jobName, callerId);

    public void Error(string message, Exception error, string? runId = null, string? callerId = null) =>
        logger.LogError(error, "{Message} RunId={RunId}, CallerId={CallerId}.", message, runId, callerId);

    public void IngestionInformation(string windfarmName, string sourceType, string message, string callerMemberName = "") =>
        logger.LogInformation(
            "{CallerMemberName}: {Message} WindfarmName={WindfarmName}, SourceType={SourceType}.",
            callerMemberName, message, windfarmName, sourceType);

    public void SourceContactSucceeded(string windfarmName, string sourceType) =>
        logger.LogInformation("Source contact succeeded for {WindfarmName} and {SourceType}.",
            windfarmName, sourceType);

    public void SourceContactFailed(string windfarmName, string sourceType, Exception error) =>
        logger.LogError(error, "Source contact failed for {WindfarmName} and {SourceType}.",
            windfarmName, sourceType);

    public void CursorAdvanced(string windfarmName, string sourceType, DateTime newLastUpdatedUtc, string newLastCommittedId) =>
        logger.LogInformation(
            "Extraction cursor advanced for {WindfarmName} and {SourceType} to {LastUpdatedUtc} / {LastCommittedId}.",
            windfarmName, sourceType, newLastUpdatedUtc, newLastCommittedId);

    public void BatchWritten(string windfarmName, int eventCount, int quarantinedCount) =>
        logger.LogInformation(
            "Outage batch written for {WindfarmName}: {EventCount} events, {QuarantinedCount} quarantined.",
            windfarmName, eventCount, quarantinedCount);
}
