using Harmonizer.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace Harmonizer.Core.Observability;

/// <summary>
/// Forwards structured fields and exceptions to the host's logging providers.
/// Timestamps and trace correlation are assigned by those providers, not embedded in messages.
/// </summary>
public sealed class HarmonizerTelemetry<TCategory>(ILogger<TCategory> logger) : ITelemetry<TCategory>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => logger.BeginScope(state);

    public void Debug(string messageTemplate, params object?[] arguments) =>
        logger.LogDebug(messageTemplate, arguments);

    public void Information(string messageTemplate, params object?[] arguments) =>
        logger.LogInformation(messageTemplate, arguments);

    public void Warning(string messageTemplate, params object?[] arguments) =>
        logger.LogWarning(messageTemplate, arguments);

    public void Warning(Exception exception, string messageTemplate, params object?[] arguments) =>
        logger.LogWarning(exception, messageTemplate, arguments);

    public void Error(Exception exception, string messageTemplate, params object?[] arguments) =>
        logger.LogError(exception, messageTemplate, arguments);
}
