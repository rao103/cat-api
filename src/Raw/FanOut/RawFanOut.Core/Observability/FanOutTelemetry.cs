using System;
using Microsoft.Extensions.Logging;
using CAT.RawFanOut.Interfaces;

namespace CAT.RawFanOut.Observability;

public sealed class FanOutTelemetry<TCategory>(ILogger<TCategory> logger) : ITelemetry<TCategory>
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
}
