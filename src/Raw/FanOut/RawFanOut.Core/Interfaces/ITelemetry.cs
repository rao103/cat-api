using System;

namespace CAT.RawFanOut.Interfaces;

public interface ITelemetry<TCategory>
{
    IDisposable? BeginScope<TState>(TState state) where TState : notnull;

    void Information(string message, string? runId = null, string? jobName = null, string? callerId = null);

    void Warning(string message, string? runId = null, string? jobName = null, string? callerId = null);

    void Error(string message, Exception error, string? runId = null, string? callerId = null);
}
