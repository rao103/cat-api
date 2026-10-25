namespace Harmonizer.Core.Interfaces;

/// <summary>Application logging with structured templates and operation-scoped correlation.</summary>
public interface ITelemetry<TCategory>
{
    IDisposable? BeginScope<TState>(TState state) where TState : notnull;
    void Debug(string messageTemplate, params object?[] arguments);
    void Information(string messageTemplate, params object?[] arguments);
    void Warning(string messageTemplate, params object?[] arguments);
    void Warning(Exception exception, string messageTemplate, params object?[] arguments);
    void Error(Exception exception, string messageTemplate, params object?[] arguments);
}
