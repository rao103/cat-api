using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.FanOut;
using CAT.RawFanOut.Operations.FanOut.Strategies;
using ProductionIngestor.Contracts;
namespace CAT.RawFanOut.UnitTests;
internal static class Fixtures
{
    public static string Body(string source = "Production", string? date = "2026-06-03", string[]? shards = null,
        bool full = false) => JsonSerializer.Serialize(new
        {
            SourceType = source, WindfarmId = "7", DataDate = date, ExtractionToken = 41L,
            BlobPath = shards is null ? "raw/test.parquet" : null, ShardPaths = shards,
            RunId = "run-1", TraceId = "trace-1", IsFullSnapshot = full,
            WindowSequence = 9, BatchKind = "Incremental"
        });
    public static RawIdentity Production(string wtgName) => new(7, null, null, new DateOnly(2026, 6, 3)) { WtgName = wtgName };
    public static string ExtractionReadyBody() => JsonSerializer.Serialize(new ProductionExtractionReadyV1(
        EnvelopeVersion: 2,
        MessageKind: "ExtractionReady",
        SourceType: "Production",
        WindfarmId: 7,
        WindfarmName: "WF01",
        ExtractionMode: "Refresh",
        PayloadKind: "ProductionSnapshot",
        PayloadSchemaVersion: 1,
        QueryAxis: "ProductionTimestamp",
        RequestId: "request-1",
        AttemptId: "attempt-1",
        ExtractionToken: 41,
        RequestedScope: [new("2026-06-03", "2026-06-04"), new("2026-06-06", "2026-06-06")],
        CompletedWindows: [new(1, DateTime.SpecifyKind(new DateTime(2026, 6, 3), DateTimeKind.Utc),
            DateTime.SpecifyKind(new DateTime(2026, 6, 7), DateTimeKind.Utc),
            [new("2026-06-03", "2026-06-04"), new("2026-06-06", "2026-06-06")], 3, 2, true)],
        Artifacts: [new("raw/part-1.parquet", 1, 1, 2), new("raw/part-2.parquet", 1, 2, 1)],
        TotalRowCount: 3,
        IsEmptyResult: false,
        ScopeWtgIds: ["T1", "T2"],
        SourceVersionEvidence: null));
    public static string OutageEventsBody(string? dataDate = null, params string[] paths)
    {
        var selectedPaths = paths.Length == 0 ? new[] { "raw/outage/events.parquet" } : paths;
        return JsonSerializer.Serialize(new
        {
            SourceType = "Outage",
            WindfarmId = "7",
            ExtractionToken = 41L,
            PayloadKind = "OutageEvents",
            BlobPath = selectedPaths.Length == 1 ? selectedPaths[0] : null,
            ShardPaths = selectedPaths.Length == 1 ? null : selectedPaths,
            RunId = "run-1",
            IsFullSnapshot = false,
            DataDate = dataDate
        });
    }
    public static string EmptyOutageSnapshotBody() => JsonSerializer.Serialize(new
    {
        SourceType = "Outage",
        WindfarmId = "7",
        ExtractionToken = 42L,
        PayloadKind = "OutageSnapshot",
        BlobPath = (string?)null,
        ShardPaths = Array.Empty<string>(),
        RunId = "snapshot-run",
        IsFullSnapshot = true,
        WindowStartUtc = new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero),
        WindowEndUtc = new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero),
        RowCount = 0,
        SnapshotAsOfUtc = new DateTimeOffset(2026, 7, 1, 0, 1, 0, TimeSpan.Zero)
    });
    public static string OutageSnapshotBody(string path, long rowCount) => JsonSerializer.Serialize(new
    {
        SourceType = "Outage",
        WindfarmId = "7",
        ExtractionToken = 43L,
        PayloadKind = "OutageSnapshot",
        BlobPath = path,
        RunId = "snapshot-run-2",
        IsFullSnapshot = true,
        WindowStartUtc = new DateTimeOffset(2026, 6, 1, 0, 0, 0, TimeSpan.Zero),
        WindowEndUtc = new DateTimeOffset(2026, 7, 1, 0, 0, 0, TimeSpan.Zero),
        RowCount = rowCount,
        SnapshotAsOfUtc = new DateTimeOffset(2026, 7, 1, 0, 1, 0, TimeSpan.Zero)
    });
    public static RawIdentity Production(string wtgName, DateOnly date, string path) =>
        new(7, null, null, date) { WtgName = wtgName, RawPath = path };
}
internal sealed class Rows(params RawIdentity[] rows) : IRawIdentityReader
{
    public bool FailAfterRows { get; set; }
    public async IAsyncEnumerable<RawIdentity> ReadAsync(RawEnvelope artifact, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var row in rows) { ct.ThrowIfCancellationRequested(); yield return row; }
        await Task.CompletedTask;
        if (FailAfterRows) throw new IOException("Second shard failed.");
    }
}
internal sealed class FakeFarmCatalog : IFarmCatalog
{
    public int WindfarmId { get; set; } = 7;
    public string WindfarmName { get; set; } = "WF01";
    public HashSet<string> MissingWtgNames { get; } = new(StringComparer.Ordinal);
    public FarmCatalogRequest? LastRequest { get; private set; }

    public Task<FarmCatalog> LoadAsync(FarmCatalogRequest request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        LastRequest = request;
        var turbines = new[] { ("T1", 101), ("T2", 102), ("T3", 103) }
            .Where(pair => !MissingWtgNames.Contains(pair.Item1))
            .Select(pair => new TurbineIdentity(pair.Item2, pair.Item1))
            .ToArray();
        return Task.FromResult(new FarmCatalog(WindfarmId, WindfarmName, turbines));
    }
}
internal static class TestPlanner
{
    public static FanOutPlanner Create(IRawIdentityReader reader, FanOutOptions? options = null, IFarmCatalog? catalog = null)
    {
        var resolvedOptions = options ?? new();
        return new FanOutPlanner(reader, catalog ?? new FakeFarmCatalog(), resolvedOptions,
        [
            new ProductionFanOutStrategy(resolvedOptions),
            new CanonicalEventFanOutStrategy("Alarm"),
            new CanonicalEventFanOutStrategy("Outage"),
            new CanonicalEventFanOutStrategy("Curtailment"),
            new CanonicalEventFanOutStrategy("Adjustment"),
        ], new NullTelemetry<FanOutPlanner>());
    }
}
internal sealed class Transport : IBatchTransport
{
    public List<OutgoingMessage[]> Calls { get; } = [];
    public Func<IReadOnlyList<OutgoingMessage>, PublishResult>? Respond { get; set; }
    public Task<PublishResult> SendAsync(IReadOnlyList<OutgoingMessage> messages, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested(); Calls.Add(messages.ToArray());
        return Task.FromResult(Respond?.Invoke(messages) ?? new PublishResult(messages.Select(x => x.Id).ToArray(), []));
    }
}
internal sealed class Queue : IInputQueue
{
    public int Deletes { get; private set; }
    public bool RenewalFails { get; set; }
    public bool DeleteFails { get; set; }
    public int Renewals { get; private set; }
    public Task<QueueDelivery?> ReceiveAsync(CancellationToken ct) => Task.FromResult<QueueDelivery?>(null);
    public Task RenewAsync(QueueDelivery delivery, CancellationToken ct)
    {
        Renewals++;
        return RenewalFails ? Task.FromException(new IOException("Renewal failed.")) : Task.CompletedTask;
    }
    public Task DeleteAsync(QueueDelivery delivery, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (DeleteFails) throw new IOException("Delete failed.");
        Deletes++; return Task.CompletedTask;
    }
}
internal sealed class DeadLetters : IDeadLetterStore
{
    public string? RecordedBody { get; private set; }
    public RawEnvelope? Artifact { get; private set; }
    public string? ErrorType { get; private set; }
    public string? LastError { get; private set; }
    public bool Resolved { get; private set; }
    public bool ReplayRequested { get; private set; }
    public ResolveIncidentRequest? Resolution { get; private set; }
    public bool FailRecord { get; set; }
    public Task<DeadLetterArchiveResult> RecordAsync(DeadLetterArchiveRequest request, CancellationToken ct)
    {
        if (FailRecord) throw new IOException("Archive failed.");
        RecordedBody = request.Delivery.Body; Artifact = request.Artifact;
        ErrorType = request.ErrorType; LastError = request.LastError;
        return Task.FromResult(new DeadLetterArchiveResult(Guid.NewGuid(), true));
    }
    public Task<DeadLetterIncident> LoadAsync(Guid id, CancellationToken ct) =>
        Task.FromResult(new DeadLetterIncident(id, RecordedBody ?? Fixtures.Body(), Resolved));
    public Task MarkReplayRequestedAsync(Guid id, CancellationToken ct) { ReplayRequested = true; return Task.CompletedTask; }
    public Task ResolveAsync(ResolveIncidentRequest request, CancellationToken ct)
    { Resolved = true; Resolution = request; return Task.CompletedTask; }
}
internal sealed class Processor(bool fail = false) : IMessageProcessor
{
    public Task ProcessAsync(string body, CancellationToken ct) => fail ?
        Task.FromException(new IOException("Publish failed.")) : Task.CompletedTask;
}
internal sealed class NullTelemetry<TCategory> : ITelemetry<TCategory>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public void Information(string message, string? runId = null, string? jobName = null, string? callerId = null) { }
    public void Warning(string message, string? runId = null, string? jobName = null, string? callerId = null) { }
    public void Error(string message, Exception error, string? runId = null, string? callerId = null) { }
}
internal sealed class FakeRetentionCleanupStore : IArtifactRetentionCleanupStore
{
    public Queue<RetentionCleanupBatchResult> Results { get; } = new();
    public List<(DateTimeOffset CutoffUtc, int MaxParentsPerBatch)> Calls { get; } = [];
    public bool FailNextCall { get; set; }

    public Task<RetentionCleanupBatchResult> DeleteEligibleBatchAsync(
        DateTimeOffset cutoffUtc, int maxParentsPerBatch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Calls.Add((cutoffUtc, maxParentsPerBatch));
        if (FailNextCall) { FailNextCall = false; throw new IOException("Cleanup batch failed."); }
        return Task.FromResult(Results.Count > 0 ? Results.Dequeue() : new RetentionCleanupBatchResult(0, 0));
    }
}

