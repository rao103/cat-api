using AlarmIngestor.Interfaces;

namespace AlarmIngestor.Monitoring;

public sealed class NullIngestionTelemetry : IIngestionTelemetry
{
    public void SourceContactSucceeded(string windfarmId, string sourceType) { }

    public void SourceContactFailed(string windfarmId, string sourceType, Exception error) { }

    public void BatchWritten(string windfarmId, int rowCount, int quarantinedCount) { }

    public void CursorAdvanced(string windfarmId, long lastCommittedAlarmId) { }
}
