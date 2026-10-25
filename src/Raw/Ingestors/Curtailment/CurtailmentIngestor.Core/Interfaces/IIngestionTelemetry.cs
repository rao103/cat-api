namespace CurtailmentIngestor.Interfaces;

/// <summary>
/// Extraction-time observations only (source contact success/failure, events retrieved, cursor
/// advancement). Does not decide authoritative Item-Day completeness — that belongs to the
/// downstream Harmonizer/Event Lifecycle Coordinator.
/// </summary>
public interface IIngestionTelemetry
{
    void SourceContactSucceeded(string windfarmId, string sourceType);

    void SourceContactFailed(string windfarmId, string sourceType, Exception error);

    void CursorAdvanced(string windfarmId, string sourceType, DateTime newLastUpdatedDate, long newLastCommittedId);

    void BatchWritten(string windfarmId, int eventCount, int quarantinedCount);
}
