namespace AlarmIngestor.Interfaces;

public interface IIngestionTelemetry
{
    void SourceContactSucceeded(string windfarmId, string sourceType);

    void SourceContactFailed(string windfarmId, string sourceType, Exception error);

    void BatchWritten(string windfarmId, int rowCount, int quarantinedCount);

    void CursorAdvanced(string windfarmId, long lastCommittedAlarmId);
}
