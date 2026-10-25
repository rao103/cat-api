using Microsoft.Extensions.Logging;
using ProductionIngestor.Interfaces;

namespace ProductionIngestor.Monitoring;

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

    public void IngestionInformation(string windfarmId, string sourceType, string message, string callerMemberName = "") =>
        logger.LogInformation(
            "{CallerMemberName}: {Message} WindfarmId={WindfarmId}, SourceType={SourceType}.",
            callerMemberName, message, windfarmId, sourceType);

    public void SourceContactSucceeded(string windfarmId, string sourceType) =>
        logger.LogInformation("Source contact succeeded for {WindfarmId} and {SourceType}.",
            windfarmId, sourceType);

    public void SourceContactFailed(string windfarmName, string sourceType, Exception error) =>
        logger.LogError(error, "Source contact failed for {WindfarmId} and {SourceType}.",
            windfarmName, sourceType);

    public void CursorAdvanced(string windfarmName, string sourceType, DateTime newLastUpdatedUtc, long newLastCommittedId) =>
        logger.LogInformation(
            "Extraction cursor advanced for {WindfarmId} and {SourceType} to {LastUpdatedUtc} / {LastCommittedId}.",
            windfarmName, sourceType, newLastUpdatedUtc, newLastCommittedId);

    public void DayWritten(string windfarmName, DateOnly productionDate, int rowCount, int quarantinedCount) =>
        logger.LogInformation(
            "Production day written for {WindfarmId} on {ProductionDate}: {RowCount} rows, {QuarantinedCount} quarantined.",
            windfarmName, productionDate, rowCount, quarantinedCount);
}
