namespace OutageIngestor.Api.Interfaces;

public interface ITelemetry<TCategory>
{
    void Information(string message, string? runId = null, string? jobName = null, string? callerId = null);

    void Error(string message, Exception error, string? runId = null, string? callerId = null);
}