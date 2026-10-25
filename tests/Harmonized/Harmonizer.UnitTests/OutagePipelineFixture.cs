using System.Security.Cryptography;
using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Operations;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Adapters;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Harmonizer.Core.SourceTypes.Outage.Operations;
using Harmonizer.Core.SourceTypes.Outage.Serialization;
using Microsoft.Extensions.Options;
using Parquet.Serialization;
using TurbineIdentity = Harmonizer.Core.Models.TurbineIdentity;

namespace Harmonizer.UnitTests;

internal sealed class OutagePipelineFixture
{
    internal readonly MutableClock Clock = new();
    internal readonly MemoryOutageObjects Objects = new();
    internal readonly MemoryOutageControl Control = new();
    internal readonly IOptions<HarmonizerOptions> Options;
    internal readonly ParquetOutageArtifacts Artifacts;
    internal readonly OutageWorkReader Reader;
    internal readonly OutageBatchProcessor Processor;
    internal readonly OutageLifecycleProcessor Lifecycle;
    internal static readonly WindfarmIdentity Farm = new(2, "WF01");
    internal static readonly TurbineIdentity[] Turbines = [new(11, "T001"), new(12, "T002")];
    internal static readonly DateTime Start = new(2026, 7, 18, 4, 57, 29, DateTimeKind.Utc);
    private int nextBatch;

    internal OutagePipelineFixture(int daysPerPass = 31)
    {
        Options = Microsoft.Extensions.Options.Options.Create(new HarmonizerOptions
        { EventLifecycle = new() { DaysPerPass = daysPerPass } });
        Artifacts = new(Objects, Objects, new(), new(), Clock, Options, Fixtures.Telemetry<ParquetOutageArtifacts>());
        Reader = new(new MemoryOutageIdentityCatalog(), Options, Fixtures.Telemetry<OutageWorkReader>());
        var canonical = new OutageCanonicalReader(Artifacts, Options, Fixtures.Telemetry<OutageCanonicalReader>());
        Lifecycle = new(Control, Artifacts, canonical, new(), new(), new(), Clock, Options,
            Fixtures.Telemetry<OutageLifecycleProcessor>(), Fixtures.Telemetry<AsyncHeartbeat>());
        Processor = new(Control, Artifacts, canonical, new(), new(), Lifecycle, new(), Clock, Options,
            Fixtures.Telemetry<OutageBatchProcessor>(), Fixtures.Telemetry<AsyncHeartbeat>());
    }

    internal async Task<OutageWork> WorkAsync(RawOutageTestRow[] rows, bool snapshot = false, long? token = null)
    {
        var ordinal = ++nextBatch;
        var path = $"raw/outage/part-{ordinal}.parquet";
        using var stream = new MemoryStream();
        await ParquetSerializer.SerializeAsync(rows, stream);
        Objects.Raw[path] = stream.ToArray();
        var members = rows.GroupBy(row => (row.EventId, row.WtgId)).Select(group =>
        {
            var id = Turbines.Single(turbine => turbine.WtgName == group.Key.WtgId).WtgId;
            var eventKey = Hash(new { SourceType = "Outage", WindfarmId = 2, group.Key.EventId });
            return new EventWorkMemberV3(eventKey, Hash(new { EventKey = eventKey, WtgId = (int?)id }), group.Key.EventId,
                id, group.Key.WtgId, [path]);
        }).ToArray();
        var message = new EventWorkV3
        {
            WorkKind = "CanonicalEvents", ArtifactId = Hash($"artifact-{ordinal}"), BatchId = Hash($"batch-{ordinal}"),
            SourceType = "Outage", PayloadKind = snapshot ? "OutageSnapshot" : rows[0].IsTombstone ? "OutageDeletedEvents" : "OutageEvents",
            WindfarmId = 2, WindfarmName = "WF01", ExtractionToken = token ?? ordinal, RunId = "run",
            OperationId = "operation-1", ExtractionMode = snapshot ? "Refresh" : "Incremental",
            IsFullSnapshot = snapshot, SnapshotScope = snapshot ? Scope() : null,
            Members = members, EventIds = members.Select(member => member.EventId).Distinct().ToArray(),
            RawPaths = [path], OriginalMessageBody = "{}"
        };
        return await Reader.ReadAsync(JsonSerializer.Serialize(message, HarmonizerJson.Options), default);
    }

    internal async Task<OutageWork> EmptySnapshotAsync(string[]? expected = null, long token = 100,
        bool authoritative = false, DateTimeOffset? sourceVersionCutoffUtc = null)
    {
        var ordinal = ++nextBatch;
        var message = new EventWorkV3
        {
            WorkKind = "EventSnapshotReconciliation", ArtifactId = Hash($"artifact-{ordinal}"), BatchId = Hash($"batch-{ordinal}"),
            SourceType = "Outage", PayloadKind = "OutageSnapshot", WindfarmId = 2, WindfarmName = "WF01",
            ExtractionToken = token, RunId = "run", ExtractionMode = "Refresh", IsFullSnapshot = true,
            ReconciliationMode = authoritative ? EventReconciliationModes.Authoritative : EventReconciliationModes.ReceiptOnly,
            SourceVersionCutoffUtc = sourceVersionCutoffUtc,
            SnapshotScope = Scope(), RawPaths = [], ExpectedEventBatchIds = expected ?? [], OriginalMessageBody = "{}"
        };
        return await Reader.ReadAsync(JsonSerializer.Serialize(message, HarmonizerJson.Options), default);
    }

    internal static EventSnapshotScopeV3 Scope() => new()
    { Kind = "EventsOverlappingInterval", StartInclusiveUtc = new(2026, 7, 18, 0, 0, 0, TimeSpan.Zero),
      EndExclusiveUtc = new(2026, 7, 21, 0, 0, 0, TimeSpan.Zero), WtgIds = [11, 12] };
    internal static RawOutageTestRow Row(string id = "classification-1", int version = 1, string wtg = "T001", bool deleted = false,
        DateTime? start = null, DateTime? end = null) => new()
    {
        EventId = id, WindfarmId = "WF01", WtgId = wtg, EventType = "Outage",
        OriginalStartUtc = start ?? Start, OriginalEndUtc = end, IsTombstone = deleted,
        LastUpdatedDate = Start.AddHours(version), SourceWatermarkUtc = Start.AddHours(version + 1),
        OutageId = "descriptive-id", AlarmCode = "0152310", Comment = null,
        Category = "Environmental & Routine Action", SubCategory = "Routine Action"
    };
    internal async Task<IReadOnlyList<OutageParquetRow>> DayAsync(int wtg = 11, string day = "2026-07-18")
    {
        var key = new WtgDayKey(2, wtg, DateOnly.Parse(day), "Outage");
        using var stream = new MemoryStream(Objects.Output[Control.Days[key].ParquetPath]);
        return (await ParquetSerializer.DeserializeAsync<OutageParquetRow>(stream)).Data.ToArray();
    }
    internal static string Hash<T>(T value) => Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(value)));
}

public sealed class RawOutageTestRow
{
    public string EventId { get; set; } = "";
    public string WindfarmId { get; set; } = "";
    public string WtgId { get; set; } = "";
    public string? EventType { get; set; }
    public DateTime OriginalStartUtc { get; set; }
    public DateTime? OriginalEndUtc { get; set; }
    public string? Status { get; set; }
    public string? SourceRevision { get; set; }
    public DateTime SourceWatermarkUtc { get; set; }
    public bool IsTombstone { get; set; }
    public DateTime LastUpdatedDate { get; set; }
    public string? OutageId { get; set; }
    public string? Comment { get; set; }
    public string? AlarmCode { get; set; }
    public string? AlarmText { get; set; }
    public string? Category { get; set; }
    public string? SubCategory { get; set; }
}

internal sealed class MutableClock : TimeProvider
{
    internal DateTimeOffset Now = new(2026, 7, 21, 12, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => Now;
}

internal sealed class MemoryOutageIdentityCatalog : IOutageIdentityCatalog
{
    public Task<IReadOnlyList<TurbineIdentity>> ReadAsync(WindfarmIdentity windfarm, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<TurbineIdentity>>(OutagePipelineFixture.Turbines);
}

internal sealed class MemoryOutageObjects : IRawObjectStore, IHarmonizedArtifactStore
{
    internal readonly Dictionary<string, byte[]> Raw = new(StringComparer.Ordinal);
    internal readonly Dictionary<string, byte[]> Output = new(StringComparer.Ordinal);
    public Task<Stream> OpenReadAsync(string path, CancellationToken cancellationToken) =>
        Task.FromResult<Stream>(new MemoryStream((path.StartsWith("raw/", StringComparison.Ordinal) ? Raw : Output)[path]));
    public async Task WriteNewAsync(string path, Stream content, string contentType, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        await content.CopyToAsync(stream, cancellationToken);
        if (!Output.TryAdd(path, stream.ToArray())) throw new InvalidOperationException("Immutable file already exists: " + path);
    }
}

internal sealed class MemoryOutageControl : IOutageControlRepository
{
    private long token;
    private Guid? owner;
    private long revision;
    internal string? CatalogPath;
    internal readonly Dictionary<BatchIdentity, OutageWork> Batches = [];
    internal readonly HashSet<BatchIdentity> Canonical = [];
    internal readonly HashSet<BatchIdentity> Completed = [];
    internal readonly Dictionary<string, OutageLifecycleUpdate> Lifecycle = new(StringComparer.Ordinal);
    internal readonly Dictionary<WtgDayKey, OutageDailyCandidate> Days = [];
    internal readonly List<(BatchIdentity Batch, string WorkId, long Generation)> Receipts = [];
    internal bool ExpectedReceiptsAvailable = true;
    internal int Failures;
    internal int Publications;

    public Task<bool> RegisterAsync(OutageWork work, CancellationToken cancellationToken)
    {
        if (Batches.TryGetValue(work.Batch, out var existing) && existing.WireBody != work.WireBody)
            throw new WorkValidationException("Frozen body changed.");
        Batches[work.Batch] = work;
        return Task.FromResult(Completed.Contains(work.Batch));
    }
    public Task RecordSourceFailureAsync(OutageWork work, string errorCode, CancellationToken cancellationToken)
    { Failures++; return Task.CompletedTask; }
    public Task RecordObservationAsync(OutageWork work, CancellationToken cancellationToken)
    { Canonical.Add(work.Batch); Completed.Add(work.Batch); return Task.CompletedTask; }
    public Task<bool> HasCanonicalReceiptAsync(BatchIdentity batch, CancellationToken cancellationToken) => Task.FromResult(Canonical.Contains(batch));
    public Task<bool> HasExpectedReceiptsAsync(OutageWork work, CancellationToken cancellationToken) => Task.FromResult(ExpectedReceiptsAvailable);
    public Task<OutageCatalogLease> ClaimAsync(WindfarmIdentity farm, CancellationToken cancellationToken)
    {
        if (owner is not null) throw new LeaseUnavailableException("Busy.");
        owner = Guid.NewGuid();
        return Task.FromResult(new OutageCatalogLease(farm, ++token, owner.Value, revision, CatalogPath));
    }
    public Task RenewAsync(OutageCatalogLease lease, CancellationToken cancellationToken) { Guard(lease); return Task.CompletedTask; }
    public Task ReleaseAsync(OutageCatalogLease lease, CancellationToken cancellationToken) { if (lease.Token == token && lease.Owner == owner) owner = null; return Task.CompletedTask; }
    public Task<IReadOnlyList<WindfarmIdentity>> GetDueFarmsAsync(int limit, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<WindfarmIdentity>>([OutagePipelineFixture.Farm]);
    public Task<IReadOnlyList<OutageLifecycleState>> ReadLifecycleAsync(int farmId, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<OutageLifecycleState>>(Lifecycle.Values.Select(update => update.State).OrderBy(state => state.DueUtc).ThenBy(state => state.WorkId).ToArray());
    public Task PublishAsync(OutageCatalogLease lease, OutageWork work, OutagePublication publication, CancellationToken cancellationToken)
    {
        Guard(lease);
        CatalogPath = publication.CatalogPath;
        revision++;
        Publications++;
        Canonical.Add(work.Batch);
        foreach (var update in publication.Lifecycle) Lifecycle[update.State.WorkId] = update;
        foreach (var member in publication.AppliedMemberKeys)
        {
            var workId = work.Message.Members.SingleOrDefault(item => item.MemberKey == member) is { } item
                ? OutageWorkKeys.Event(item.EventId) : OutageWorkKeys.Scope(work);
            Receipts.Add((work.Batch, workId, Lifecycle.GetValueOrDefault(workId)?.Plan.Generation ?? 0));
        }
        return Task.CompletedTask;
    }
    public Task<long> ReadDayRevisionAsync(WtgDayKey key, CancellationToken cancellationToken) => Task.FromResult(Days.GetValueOrDefault(key)?.Revision ?? 0);
    public Task AdvanceAsync(OutageCatalogLease lease, OutageLifecycleUpdate update, OutageDailyCandidate? candidate, CancellationToken cancellationToken)
    {
        Guard(lease);
        Lifecycle[update.State.WorkId] = update;
        if (candidate is not null) Days[candidate.Key] = candidate;
        return Task.CompletedTask;
    }
    public Task RecordFailureAsync(OutageCatalogLease lease, string errorCode, CancellationToken cancellationToken) { Guard(lease); Failures++; return Task.CompletedTask; }
    public async Task CompleteReadyBatchesAsync(int farmId, CancellationToken cancellationToken)
    { foreach (var batch in Batches.Keys.ToArray()) await CompleteBatchAsync(batch, cancellationToken); }
    public Task<bool> CompleteBatchAsync(BatchIdentity batch, CancellationToken cancellationToken)
    {
        var complete = Canonical.Contains(batch) && Receipts.Where(receipt => receipt.Batch == batch)
            .All(receipt => !Lifecycle.TryGetValue(receipt.WorkId, out var update) || update.Plan.Pending.Count == 0);
        if (complete) Completed.Add(batch);
        return Task.FromResult(complete);
    }
    private void Guard(OutageCatalogLease lease)
    { if (lease.Token != token || lease.Owner != owner) throw new LeaseLostException("Stale owner."); }
}
