using Microsoft.Extensions.Logging;
using OutageIngestor.Monitoring;

namespace OutageIngestor.UnitTests;

public sealed class IngestionTelemetryTests
{
    [Fact]
    public void AllIngestionObservationsUseStructuredWindfarmNameAndPreserveBatchCounts()
    {
        var logger = new TestIngestionLogger<IngestionTelemetryTests>();
        var telemetry = new IngestionTelemetry<IngestionTelemetryTests>(logger);

        telemetry.IngestionInformation("CHW01", "Outage", "Starting");
        telemetry.SourceContactSucceeded("CHW01", "Outage");
        telemetry.SourceContactFailed("CHW01", "Outage", new InvalidOperationException("source unavailable"));
        telemetry.CursorAdvanced("CHW01", "Outage", DateTime.UtcNow, "OC-string-id");
        telemetry.BatchWritten("CHW01", 7, 2);

        Assert.Equal(5, logger.Entries.Count);
        Assert.All(logger.Entries, entry =>
        {
            Assert.Equal("CHW01", entry["WindfarmName"]);
            Assert.False(entry.ContainsKey("WindfarmId"));
        });
        var batch = Assert.Single(logger.Entries, entry => entry.ContainsKey("EventCount"));
        Assert.Equal(7, batch["EventCount"]);
        Assert.Equal(2, batch["QuarantinedCount"]);
        Assert.Equal("OC-string-id", Assert.Single(logger.Entries, entry => entry.ContainsKey("LastCommittedId"))["LastCommittedId"]);
    }
}

internal sealed class TestIngestionLogger<T> : ILogger<T>
{
    public List<Dictionary<string, object?>> Entries { get; } = [];
    public List<Dictionary<string, object?>> Scopes { get; } = [];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        if (state is IEnumerable<KeyValuePair<string, object?>> values)
            Scopes.Add(values.ToDictionary(pair => pair.Key, pair => pair.Value));
        return null;
    }

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (state is IEnumerable<KeyValuePair<string, object?>> values)
            Entries.Add(values.ToDictionary(pair => pair.Key, pair => pair.Value));
    }
}