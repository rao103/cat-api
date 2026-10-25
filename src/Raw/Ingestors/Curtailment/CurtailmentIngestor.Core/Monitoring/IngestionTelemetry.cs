using CurtailmentIngestor.Interfaces;

namespace CurtailmentIngestor.Monitoring;

/// <summary>No-op telemetry sink; a real implementation wires into the availability tables.</summary>
public sealed class NullIngestionTelemetry : IIngestionTelemetry
{
    public void SourceContactSucceeded(string windfarmId, string sourceType) { }

    public void SourceContactFailed(string windfarmId, string sourceType, Exception error) { }

    public void CursorAdvanced(string windfarmId, string sourceType, DateTime newLastUpdatedDate, long newLastCommittedId) { }

    public void BatchWritten(string windfarmId, int eventCount, int quarantinedCount) { }
}
