using AlarmIngestor.Models;
using AlarmIngestor.Serialization;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="AlarmBatchWriter"/>: raw ends stay null with no duration cap, any malformed event
/// fails the entire batch closed (audited to a quarantine artifact, then throws
/// <see cref="InvalidDataException"/>) rather than silently omitting it from a partial-success
/// publication, oversized batches shard deterministically, and a write is only reported success
/// once existence is verified against the store.
/// </summary>
public sealed class AlarmBatchWriterTests
{
    private static readonly DateTime Start = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static AlarmEventModel MakeEvent(long alarmId, string turbine = "RDSPP-1", DateTime? end = null) => new(
        WindfarmId: "farm", TurbineRdspp: turbine, ItemId: "ITEM-1", AlarmId: alarmId,
        AlarmCode: "100", AlarmText: "text", StartTimeUtc: Start, EndTimeUtc: end,
        AlarmType: "Alarm", AlarmState: "Good", IsTombstone: false, ObservedAtUtc: Start, SequenceInRun: 0);

    [Fact]
    public async Task OpenAlarmWithNullEndTimeRoundTripsWithoutDurationCap()
    {
        var store = new InMemoryStore();
        var writer = new AlarmBatchWriter(store);

        var result = await writer.WriteBatchAsync("farm", 1, 0, [MakeEvent(1, end: null)], CancellationToken.None);

        Assert.Equal(1, result.RowCount);
        Assert.Empty(result.QuarantinedEvents);
        Assert.NotNull(result.BlobPath);
        Assert.True(await store.ExistsAsync(result.BlobPath!, CancellationToken.None));
    }

    [Fact]
    public void EventWithEndBeforeStartIsQuarantinedNotDropped()
    {
        var malformed = MakeEvent(2, end: Start.AddHours(-1));

        var (valid, quarantined) = AlarmBatchWriter.Validate([malformed]);

        Assert.Empty(valid);
        Assert.Single(quarantined);
        Assert.Equal("2", quarantined[0].AlarmId);
    }

    [Fact]
    public void EventMissingTurbineIdentityIsQuarantined()
    {
        var malformed = MakeEvent(3, turbine: "");

        var (valid, quarantined) = AlarmBatchWriter.Validate([malformed]);

        Assert.Empty(valid);
        Assert.Single(quarantined);
    }

    [Fact]
    public void BatchLargerThanShardLimitSplitsDeterministicallyByAlarmId()
    {
        var events = Enumerable.Range(1, 5).Select(i => MakeEvent(i)).ToList();

        var shards = AlarmBatchWriter.DetermineShards(events, maxEventsPerShard: 2);

        Assert.Equal(3, shards.Count);
        Assert.Equal(2, shards[0].Count);
        Assert.Equal(2, shards[1].Count);
        Assert.Single(shards[2]);
        Assert.Equal([1L, 2L], shards[0].Select(e => e.AlarmId));
    }

    [Fact]
    public async Task WriteThrowsWhenStoreCannotVerifyDurability()
    {
        var store = new InMemoryStore { SuppressExistsCheck = true };
        var writer = new AlarmBatchWriter(store);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => writer.WriteBatchAsync("farm", 1, 0, [MakeEvent(1)], CancellationToken.None));
    }

    [Fact]
    public async Task ParquetArtifactRoundTripsNullEndAndAllSourceFields()
    {
        var store = new InMemoryStore();
        var writer = new AlarmBatchWriter(store);
        var openEvent = MakeEvent(1, end: null);
        var closedEvent = MakeEvent(2, end: Start.AddHours(3));

        var result = await writer.WriteBatchAsync("farm", 1, 0, [openEvent, closedEvent], CancellationToken.None);

        var bytes = await store.DownloadAsync(result.BlobPath!, CancellationToken.None);
        using var stream = new MemoryStream(bytes);
        await using var reader = await Parquet.ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var schema = reader.Schema.GetDataFields();

        var alarmIds = new long[2];
        await group.ReadAsync<long>(schema.Single(f => f.Name == "AlarmId"), alarmIds);
        var endTimes = new DateTime?[2];
        await group.ReadAsync<DateTime>(schema.Single(f => f.Name == "EndTimeUtc"), endTimes);
        var turbines = new string[2];
        await group.ReadAsync(schema.Single(f => f.Name == "TurbineRdspp"), turbines);
        var codes = new string[2];
        await group.ReadAsync(schema.Single(f => f.Name == "AlarmCode"), codes);

        Assert.Equal([1L, 2L], alarmIds);
        Assert.Null(endTimes[0]);
        Assert.Equal(closedEvent.EndTimeUtc, endTimes[1]);
        Assert.All(turbines, t => Assert.Equal("RDSPP-1", t));
        Assert.All(codes, c => Assert.Equal("100", c));
    }

    [Fact]
    public async Task AnyQuarantinedEventFailsTheWholeBatchClosedWithNoRawPublication()
    {
        // AlarmId 1 (valid) is lower than AlarmId 2 (quarantined) — a partial-success return here
        // is exactly what let a caller checkpoint/enroll past the unresolved id 2. The batch must
        // throw and never produce a Raw artifact path for either event.
        var store = new InMemoryStore();
        var writer = new AlarmBatchWriter(store);
        var valid = MakeEvent(1);
        var malformed = MakeEvent(2, end: Start.AddHours(-1));

        await Assert.ThrowsAsync<InvalidDataException>(
            () => writer.WriteBatchAsync("farm", 1, 0, [valid, malformed], CancellationToken.None));

        Assert.Empty(store.UploadedPaths.Where(p => !p.Contains("quarantine", StringComparison.OrdinalIgnoreCase)));
    }

    [Fact]
    public async Task AllInvalidBatchFailsClosedAndWritesQuarantineAuditOnly()
    {
        var store = new InMemoryStore();
        var writer = new AlarmBatchWriter(store);
        var malformed = MakeEvent(9, end: Start.AddHours(-1));

        await Assert.ThrowsAsync<InvalidDataException>(
            () => writer.WriteBatchAsync("farm", 1, 0, [malformed], CancellationToken.None));

        Assert.Single(store.UploadedPaths, p => p.Contains("quarantine", StringComparison.OrdinalIgnoreCase));
    }

    private sealed class InMemoryStore : Interfaces.IRawObjectStore
    {
        private readonly Dictionary<string, byte[]> _objects = new();
        public bool SuppressExistsCheck { get; init; }
        public List<string> UploadedPaths { get; } = [];

        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            _objects[path] = content.ToArray();
            UploadedPaths.Add(path);
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken)
            => Task.FromResult(!SuppressExistsCheck && _objects.ContainsKey(path));

        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken) => Task.FromResult(_objects[path]);
    }
}
