using System.Runtime.CompilerServices;

namespace ProductionIngestor.Interfaces;

public interface ITelemetry<TCategory>
{
    IDisposable? BeginScope<TState>(TState state) where TState : notnull;

    void Information(string message, string? runId = null, string? jobName = null, string? callerId = null);

    void Warning(string message, string? runId = null, string? jobName = null, string? callerId = null);

    void Error(string message, Exception error, string? runId = null, string? callerId = null);
}

/// <summary>
/// Extraction-time observations only (source contact success/failure, rows retrieved, cursor
/// advancement). Does not decide authoritative Item-Day completeness — that belongs to the
/// downstream Harmonizer.
/// </summary>
public interface IIngestionTelemetry<TCategory> : ITelemetry<TCategory>
{
    void IngestionInformation(string windfarmName, string sourceType, string message,
        [CallerMemberName] string callerMemberName = "");

    void SourceContactSucceeded(string windfarmName, string sourceType);

    void SourceContactFailed(string windfarmName, string sourceType, Exception error);

    void CursorAdvanced(string windfarmName, string sourceType, DateTime newLastUpdatedUtc, long newLastCommittedId);

    void DayWritten(string windfarmName, DateOnly productionDate, int rowCount, int quarantinedCount);
}
