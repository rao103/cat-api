using System.Collections.Concurrent;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Observability;
using Microsoft.Extensions.Logging.Abstractions;

namespace Harmonizer.IntegrationTests;

internal static class TestTelemetry
{
    public static HarmonizerTelemetry<T> For<T>() => new(NullLogger<T>.Instance);
}

internal sealed class RecordingTelemetry<T> : ITelemetry<T>
{
    public ConcurrentQueue<(string Level, string Template)> Events { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public void Debug(string messageTemplate, params object?[] arguments) => Events.Enqueue(("Debug", messageTemplate));
    public void Information(string messageTemplate, params object?[] arguments) => Events.Enqueue(("Information", messageTemplate));
    public void Warning(string messageTemplate, params object?[] arguments) => Events.Enqueue(("Warning", messageTemplate));
    public void Warning(Exception exception, string messageTemplate, params object?[] arguments) => Events.Enqueue(("Warning", messageTemplate));
    public void Error(Exception exception, string messageTemplate, params object?[] arguments) => Events.Enqueue(("Error", messageTemplate));
}
