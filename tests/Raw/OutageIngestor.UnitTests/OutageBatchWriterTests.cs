using Parquet;
using OutageIngestor.Enums;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Raw;
using OutageIngestor.Serialization;

namespace OutageIngestor.UnitTests;

/// <summary>
/// Real Parquet roundtrip coverage for <see cref="OutageBatchWriter"/>: an open outage (null
/// end) must serialize/deserialize with a true null rather than the legacy DateTime.MaxValue
/// sentinel, a closed outage keeps its end, a tombstone carries default start/null end, and
/// validation quarantines malformed rows without touching tombstones.
/// </summary>
public sealed class OutageBatchWriterTests
{
    private static readonly DateTime Start = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private static OutageEventModel Open(string eventId = "OC-1") => new(
        EventId: eventId, WindfarmId: "farm", ItemId: "wtg-1", EventType: "Outage",
        OriginalStartUtc: Start, OriginalEndUtc: null, Status: "Open",
        SourceRevision: null, SourceWatermarkUtc: Start, IsTombstone: false,
        LastUpdatedDate: Start, OutageId: "OUT-1",
        Comment: "WFR comment", AlarmCode: "A100", AlarmText: "Alarm text",
        Category: "Grid", SubCategory: "Voltage dip");

    private static OutageEventModel Closed(string eventId = "OC-2") => new(
        EventId: eventId, WindfarmId: "farm", ItemId: "wtg-1", EventType: "Outage",
        OriginalStartUtc: Start, OriginalEndUtc: Start.AddHours(2), Status: "Closed",
        SourceRevision: null, SourceWatermarkUtc: Start, IsTombstone: false,
        LastUpdatedDate: Start, OutageId: "OUT-2");

    private static OutageEventModel Tombstone(string eventId = "OC-3") => new(
        EventId: eventId, WindfarmId: "farm", ItemId: "wtg-1", EventType: "Outage",
        OriginalStartUtc: default, OriginalEndUtc: null, Status: "Deleted",
        SourceRevision: null, SourceWatermarkUtc: Start, IsTombstone: true,
        LastUpdatedDate: Start, OutageId: "OUT-3");

    [Fact]
    public async Task OpenEventRoundTripsATrueNullEndNotASentinel()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store);
        var result = await writer.WriteBatchAsync("farm", extractionToken: 1, Start, Start.AddHours(1), [Open()], CancellationToken.None);

        Assert.NotNull(result.BlobPath);
        using var stream = new MemoryStream(store.Objects[result.BlobPath!]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var endField = reader.Schema.GetDataFields().Single(f => f.Name == "OriginalEndUtc");
        var ends = new DateTime?[1];
        await group.ReadAsync<DateTime>(endField, ends);
        Assert.Null(ends[0]);
    }

    [Fact]
    public async Task ClosedEventRoundTripsItsEnd()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store);
        var evt = Closed();
        var result = await writer.WriteBatchAsync("farm", extractionToken: 1, Start, Start.AddHours(1), [evt], CancellationToken.None);

        using var stream = new MemoryStream(store.Objects[result.BlobPath!]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var endField = reader.Schema.GetDataFields().Single(f => f.Name == "OriginalEndUtc");
        var ends = new DateTime?[1];
        await group.ReadAsync<DateTime>(endField, ends);
        Assert.Equal(evt.OriginalEndUtc!.Value.Ticks, ends[0]!.Value.Ticks);
    }

    [Fact]
    public async Task TombstoneRoundTripsIsTombstoneAndDefaultFields()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store);
        var evt = Tombstone();
        var result = await writer.WriteBatchAsync("farm", extractionToken: 1, Start, Start.AddHours(1), [evt], CancellationToken.None);

        Assert.Null(result.BlobPath);
        Assert.NotNull(result.DeletedBlobPath);
        using var stream = new MemoryStream(store.Objects[result.DeletedBlobPath!]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var fields = reader.Schema.GetDataFields();

        var tombstoneFlags = new bool[1];
        await group.ReadAsync<bool>(fields.Single(f => f.Name == "IsTombstone"), tombstoneFlags);
        Assert.True(tombstoneFlags[0]);

        var statuses = new string[1];
        await group.ReadAsync(fields.Single(f => f.Name == "Status"), statuses);
        Assert.Equal("Deleted", statuses[0]);

        var ends = new DateTime?[1];
        await group.ReadAsync<DateTime>(fields.Single(f => f.Name == "OriginalEndUtc"), ends);
        Assert.Null(ends[0]);
    }

    [Fact]
    public async Task MixedBatchWritesLiveAndDeletedEventsAsTwoSeparateArtifacts()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store);
        var live = Open();
        var deleted = Tombstone();

        var result = await writer.WriteBatchAsync("farm", extractionToken: 1, Start, Start.AddHours(1), [live, deleted], CancellationToken.None);

        Assert.NotNull(result.BlobPath);
        Assert.NotNull(result.DeletedBlobPath);
        Assert.NotEqual(result.BlobPath, result.DeletedBlobPath);
        Assert.Contains("/events/", result.BlobPath);
        Assert.Contains("/deleted-events/", result.DeletedBlobPath);
        Assert.Equal(1, result.RowCount);
        Assert.Equal(1, result.DeletedRowCount);
        Assert.Equal(2, result.TotalRowCount);

        using var eventsStream = new MemoryStream(store.Objects[result.BlobPath!]);
        await using var eventsReader = await ParquetReader.CreateAsync(eventsStream);
        using var eventsGroup = eventsReader.OpenRowGroupReader(0);
        var eventIds = new string[1];
        await eventsGroup.ReadAsync(eventsReader.Schema.GetDataFields().Single(f => f.Name == "EventId"), eventIds);
        Assert.Equal(live.EventId, eventIds[0]);

        using var deletedStream = new MemoryStream(store.Objects[result.DeletedBlobPath!]);
        await using var deletedReader = await ParquetReader.CreateAsync(deletedStream);
        using var deletedGroup = deletedReader.OpenRowGroupReader(0);
        var deletedIds = new string[1];
        await deletedGroup.ReadAsync(deletedReader.Schema.GetDataFields().Single(f => f.Name == "EventId"), deletedIds);
        Assert.Equal(deleted.EventId, deletedIds[0]);
    }

    [Fact]
    public async Task DeletionOnlyBatchWritesNoLiveEventsArtifact()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store);

        var result = await writer.WriteBatchAsync("farm", extractionToken: 1, Start, Start.AddHours(1), [Tombstone()], CancellationToken.None);

        Assert.Null(result.BlobPath);
        Assert.Null(result.ShardPaths);
        Assert.Equal(0, result.RowCount);
        Assert.NotNull(result.DeletedBlobPath);
        Assert.Equal(1, result.DeletedRowCount);
    }

    [Fact]
    public async Task ClassificationAttributesRoundTripThroughParquet()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store);
        var evt = Open();
        var result = await writer.WriteBatchAsync("farm", extractionToken: 1, Start, Start.AddHours(1), [evt], CancellationToken.None);

        using var stream = new MemoryStream(store.Objects[result.BlobPath!]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var fields = reader.Schema.GetDataFields();

        async Task<string?> ReadOne(string name)
        {
            var values = new string?[1];
            await group.ReadAsync(fields.Single(f => f.Name == name), values);
            return values[0];
        }

        Assert.Equal(evt.Comment, await ReadOne("Comment"));
        Assert.Equal(evt.AlarmCode, await ReadOne("AlarmCode"));
        Assert.Equal(evt.AlarmText, await ReadOne("AlarmText"));
        Assert.Equal(evt.Category, await ReadOne("Category"));
        Assert.Equal(evt.SubCategory, await ReadOne("SubCategory"));
    }

    [Fact]
    public void ValidateSkipsStartEndChecksForTombstonesButQuarantinesMalformedLiveRows()
    {
        var missingId = Open() with { EventId = "" };
        var malformedStart = Open() with { EventId = "OC-4", OriginalStartUtc = default };
        var endBeforeStart = Closed() with { EventId = "OC-5", OriginalEndUtc = Start.AddHours(-1) };
        var tombstoneWithDefaultStart = Tombstone();

        var (valid, quarantined) = OutageBatchWriter.Validate([missingId, malformedStart, endBeforeStart, tombstoneWithDefaultStart]);

        Assert.Single(valid);
        Assert.Same(tombstoneWithDefaultStart, valid[0]);
        Assert.Equal(3, quarantined.Count);
        Assert.Contains(quarantined, q => q.Reason == "missing EventId");
        Assert.Contains(quarantined, q => q.EventId == "OC-4" && q.Reason.Contains("OriginalStartUtc"));
        Assert.Contains(quarantined, q => q.EventId == "OC-5" && q.Reason.Contains("precedes"));
    }

    [Fact]
    public async Task ShardingIsDeterministicByEventIdAndUsesDistinctPaths()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store, maxEventsPerShard: 1);
        var events = new[] { Open("OC-b"), Open("OC-a") };
        var result = await writer.WriteBatchAsync("farm", extractionToken: 1, Start, Start.AddHours(1), events, CancellationToken.None);

        Assert.Null(result.BlobPath);
        Assert.NotNull(result.ShardPaths);
        Assert.Equal(2, result.ShardPaths!.Count);
        Assert.EndsWith("part-0001.parquet", result.ShardPaths[0]);
        Assert.EndsWith("part-0002.parquet", result.ShardPaths[1]);

        // Shard 1 must carry the ordinally-first EventId ("OC-a"), independent of input order.
        using var stream = new MemoryStream(store.Objects[result.ShardPaths[0]]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        using var group = reader.OpenRowGroupReader(0);
        var ids = new string[1];
        await group.ReadAsync(reader.Schema.GetDataFields().Single(f => f.Name == "EventId"), ids);
        Assert.Equal("OC-a", ids[0]);
    }

    [Fact]
    public async Task WindowScopeKeepsMultipleWindowsUnderOneLeaseFromColliding()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store);
        var firstWindowEnd = Start.AddHours(1);
        var secondWindowEnd = Start.AddHours(2);
        var first = await writer.WriteBatchAsync("farm", extractionToken: 7, Start, firstWindowEnd, [Open("OC-1")], CancellationToken.None);
        var second = await writer.WriteBatchAsync("farm", extractionToken: 7, firstWindowEnd, secondWindowEnd, [Open("OC-2")], CancellationToken.None);

        Assert.NotEqual(first.BlobPath, second.BlobPath);
        Assert.Equal(RawPathBuilder.BuildEventsSingleArtifactPath("farm", 7, Start, firstWindowEnd), first.BlobPath);
        Assert.Equal(RawPathBuilder.BuildEventsSingleArtifactPath("farm", 7, firstWindowEnd, secondWindowEnd), second.BlobPath);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(50)]
    public async Task IncrementalArtifactsAndQuarantineShareTheIncrementalPrefix(int maxEventsPerShard)
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store, maxEventsPerShard);
        var result = await writer.WriteBatchAsync("farm", 7, Start, Start.AddHours(1),
            [Open("live-1"), Open("live-2"), Tombstone("deleted-1"), Tombstone("deleted-2"), Open() with { EventId = "" }], default);

        const string prefix = "/raw/outage/windfarm=farm/mode=incremental/extraction=7/";
        Assert.All(store.Objects.Keys, path => Assert.StartsWith(prefix, path));
        Assert.Single(result.QuarantinedEvents);
        Assert.Contains(prefix + "window-20260901T000000Z-to-20260901T010000Z-outage-quarantine.parquet", store.Objects.Keys);
        Assert.Contains(prefix + "events/window-20260901T000000Z-to-20260901T010000Z-part-0001.parquet", store.Objects.Keys);
        Assert.Contains(prefix + "deleted-events/window-20260901T000000Z-to-20260901T010000Z-part-0001.parquet", store.Objects.Keys);
        Assert.Equal(maxEventsPerShard == 1 ? 5 : 3, store.Objects.Count);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task CompleteSnapshotPartsAndManifestShareTheRefreshPrefix(int validEventCount)
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store, maxEventsPerShard: 1);
        var events = Enumerable.Range(1, validEventCount).Select(i => Open($"live-{i}")).ToList();
        var result = await writer.WriteSnapshotAsync("42", "farm", 7, Start, Start.AddHours(1), "run-1", Stream(events), default);

        const string prefix = "/raw/outage/windfarm=farm/mode=refresh/extraction=7/";
        Assert.All(store.Objects.Keys, path => Assert.StartsWith(prefix, path));
        Assert.Equal(prefix + "snapshot/manifest.json", result.ManifestPath);
        Assert.All(store.Objects.Keys, path => Assert.StartsWith(prefix + "snapshot/", path));
        Assert.Equal(validEventCount + 1, store.Objects.Count);
        Assert.Equal(validEventCount, result.PartPaths.Count);
        for (var i = 0; i < validEventCount; i++)
            Assert.Equal(prefix + $"snapshot/window-20260901T000000Z-to-20260901T010000Z-part-{i + 1:0000}.parquet", result.PartPaths[i]);
        var manifest = System.Text.Json.JsonSerializer.Deserialize<OutageSnapshotManifestModel>(store.Objects[result.ManifestPath]);
        Assert.NotNull(manifest);
        Assert.Equal(result.PartPaths, manifest.Parts);
        Assert.Equal(validEventCount, manifest.RecordCount);
        Assert.Equal("42", manifest.WindfarmId);
        Assert.Equal("farm", manifest.WindfarmName);
        Assert.Equal("run-1", manifest.RunId);
        Assert.Equal(Start, manifest.ScopeStartUtc);
        Assert.Equal(Start.AddHours(1), manifest.ScopeEndUtc);
        Assert.Equal("verify:" + result.ManifestPath, store.Operations.Last());
        foreach (var part in result.PartPaths)
            Assert.True(store.Operations.IndexOf("verify:" + part) < store.Operations.IndexOf("upload:" + result.ManifestPath));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InvalidOrDeletedSnapshotRowsNeverProduceACompleteManifest(bool tombstone)
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store, maxEventsPerShard: 1);
        var invalid = tombstone ? Tombstone() : Open() with { EventId = "" };

        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteSnapshotAsync(
            "42", "farm", 7, Start, Start.AddHours(1), "run-1", Stream([Open(), invalid]), default));

        Assert.Single(store.Objects); // earlier part is retained but is not an authoritative snapshot
        Assert.DoesNotContain(store.Objects.Keys, path => path.EndsWith("manifest.json"));
        Assert.DoesNotContain(store.Objects.Keys, path => path.Contains("deleted-events"));
    }

    [Theory]
    [InlineData("part-0001.parquet", false)]
    [InlineData("part-0002.parquet", false)]
    [InlineData("manifest.json", false)]
    [InlineData("part-0001.parquet", true)]
    [InlineData("manifest.json", true)]
    public async Task SnapshotStorageFailureCannotReturnACompleteResult(string failingSuffix, bool verificationFailure)
    {
        var store = new MemoryObjectStore
        {
            FailUploadSuffix = verificationFailure ? null : failingSuffix,
            FailVerificationSuffix = verificationFailure ? failingSuffix : null
        };
        var writer = new OutageBatchWriter(store, maxEventsPerShard: 1);
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteSnapshotAsync(
            "42", "farm", 7, Start, Start.AddHours(1), "run-1", Stream([Open(), Closed()]), default));

        if (failingSuffix.EndsWith(".parquet"))
            Assert.DoesNotContain(store.Objects.Keys, path => path.EndsWith("manifest.json"));
    }

    [Fact]
    public async Task SnapshotWritesBoundedPartsBeforeReadingTheRestOfTheSource()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store, maxEventsPerShard: 2);
        var events = Stream([Open("1"), Open("2"), Open("3")], index =>
        {
            if (index == 2)
            {
                Assert.Single(store.Objects);
                Assert.EndsWith("part-0001.parquet", store.Objects.Keys.Single());
            }
        });

        var result = await writer.WriteSnapshotAsync("42", "farm", 7, Start, Start.AddHours(1), "run-1", events, default);
        Assert.Equal(2, result.PartPaths.Count);
        Assert.Equal(3, result.RowCount);
    }

    [Fact]
    public async Task SourceFailureAfterAStoredPartNeverWritesManifest()
    {
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store, maxEventsPerShard: 1);
        var events = Stream([Open(), Closed()], index =>
        {
            if (index == 1) throw new InvalidOperationException("Source page failed");
        });
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteSnapshotAsync(
            "42", "farm", 7, Start, Start.AddHours(1), "run-1", events, default));
        Assert.Single(store.Objects);
        Assert.DoesNotContain(store.Objects.Keys, path => path.EndsWith("manifest.json"));
    }

    [Fact]
    public async Task CancellationAfterAStoredPartNeverWritesManifest()
    {
        using var cancellation = new CancellationTokenSource();
        var store = new MemoryObjectStore();
        var writer = new OutageBatchWriter(store, maxEventsPerShard: 1);
        var events = Stream([Open(), Closed()], index => { if (index == 1) cancellation.Cancel(); });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writer.WriteSnapshotAsync(
            "42", "farm", 7, Start, Start.AddHours(1), "run-1", events, cancellation.Token));
        Assert.Single(store.Objects);
        Assert.DoesNotContain(store.Objects.Keys, path => path.EndsWith("manifest.json"));
    }

    [Fact]
    public void InvalidModeCannotSilentlyProduceAnIncrementalPath()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RawPathBuilder.BuildBasePath("farm", 7, (IngestionMode)42));
    }

    private static async IAsyncEnumerable<OutageEventModel> Stream(
        IReadOnlyList<OutageEventModel> events, Action<int>? beforeRead = null)
    {
        await Task.Yield();
        for (var i = 0; i < events.Count; i++)
        {
            beforeRead?.Invoke(i);
            yield return events[i];
        }
    }

    private sealed class MemoryObjectStore : IRawObjectStore
    {
        public Dictionary<string, byte[]> Objects { get; } = new();
        public List<string> Operations { get; } = [];
        public string? FailUploadSuffix { get; init; }
        public string? FailVerificationSuffix { get; init; }
        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Operations.Add("upload:" + path);
            if (FailUploadSuffix is not null && path.EndsWith(FailUploadSuffix))
                throw new InvalidOperationException("Storage write failed");
            Objects.Add(path, content.ToArray());
            return Task.CompletedTask;
        }
        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken)
        {
            Operations.Add("verify:" + path);
            return Task.FromResult(Objects.ContainsKey(path)
                && (FailVerificationSuffix is null || !path.EndsWith(FailVerificationSuffix)));
        }
        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken) => Task.FromResult(Objects[path]);
    }
}
