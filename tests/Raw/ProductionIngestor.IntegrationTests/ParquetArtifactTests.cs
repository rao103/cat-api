using Parquet;
using ProductionIngestor.Contracts;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Raw;
using ProductionIngestor.Serialization;

namespace ProductionIngestor.IntegrationTests;

public sealed class ParquetArtifactTests
{
    private static readonly string[] ProductionFields =
    [
        "WtgRdsppId",
        "StartTimeInterval",
        "EndTimeInterval",
        "ActualProductionKWh",
        "PossibleProductionKWh",
        "LostProductionCapacitykWh",
        "ContractualPoPEColumn",
        "SiteAverageWindSpeedMps",
        "LastUpdatedDate"
    ];

    [Fact]
    public async Task EmptyDayIsAReadableParquetArtifact()
    {
        var store = new MemoryObjectStore();
        var result = await new ProductionParquetWriter(store).WriteDayAsync("farm", new DateOnly(2026, 9, 15), 1, [], CancellationToken.None, 42);
        Assert.Equal(0, result.RowCount);
        Assert.NotNull(result.BlobPath);
        using var stream = new MemoryStream(store.Objects[result.BlobPath]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        Assert.Equal(0, reader.RowGroupCount);
        Assert.Equal(ProductionFields, reader.Schema.GetDataFields().Select(field => field.Name));
        Assert.Equal("2", reader.CustomMetadata!["payload_schema_version"]);
        Assert.Equal("Incremental", reader.CustomMetadata["extraction_mode"]);
        Assert.Equal("42", reader.CustomMetadata["windfarm_id"]);
    }

    [Fact]
    public async Task RowLimitCreatesDeterministicParquetShards()
    {
        var store = new MemoryObjectStore();
        var writer = new ProductionParquetWriter(store, maxRowsPerShard: 1, rowGroupSize: 10);
        var timestamp = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);
        var row = new ProductionRowModel("farm", "first", timestamp, 1m, null, null, null, timestamp, null,
            timestamp.AddMinutes(10), null);
        var result = await writer.WriteDayAsync("farm", DateOnly.FromDateTime(timestamp), 7,
            [row with { WtgName = "second" }, row], CancellationToken.None);
        Assert.Null(result.BlobPath);
        Assert.NotNull(result.ShardPaths);
        Assert.Equal(2, result.ShardPaths.Count);
        Assert.Contains("/mode=incremental/", result.ShardPaths[0]);
        Assert.EndsWith("-part-0001.parquet", result.ShardPaths[0]);
        foreach (var path in result.ShardPaths)
        {
            using var stream = new MemoryStream(store.Objects[path]);
            await using var reader = await ParquetReader.CreateAsync(stream);
            Assert.Equal(1, reader.RowGroupCount);
        }
    }

    [Fact]
    public async Task QuarantineIsParquetAndCorruptionFailsVerification()
    {
        var store = new MemoryObjectStore();
        var timestamp = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);
        var row = new ProductionRowModel("farm", "first", timestamp, -1m, null, null, null, timestamp, null,
            timestamp.AddMinutes(10), null);
        var result = await new ProductionParquetWriter(store).WriteDayAsync("farm", DateOnly.FromDateTime(timestamp), 7, [row], CancellationToken.None);
        Assert.Single(result.QuarantinedRows);
        using var stream = new MemoryStream(store.Objects[RawPathBuilder.BuildQuarantinePath("farm", DateOnly.FromDateTime(timestamp), 7)]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        Assert.Equal(1, reader.RowGroupCount);

        store.CorruptDownloads = true;
        await Assert.ThrowsAsync<InvalidDataException>(() => new ProductionParquetWriter(store)
            .WriteDayAsync("farm", DateOnly.FromDateTime(timestamp), 8, [], CancellationToken.None));
    }

    [Fact]
    public async Task ImpossibleByteLimitFailsWithoutUploadingAnOversizedArtifact()
    {
        var store = new MemoryObjectStore();
        await Assert.ThrowsAsync<InvalidDataException>(() => new ProductionParquetWriter(store, maxShardSizeBytes: 1)
            .WriteDayAsync("farm", new DateOnly(2026, 9, 15), 1, [], CancellationToken.None));
        Assert.Empty(store.Objects);
    }

    [Fact]
    public async Task ArtifactRoundTripsDecimalValuesAcrossRowGroups()
    {
        var store = new MemoryObjectStore();
        var writer = new ProductionParquetWriter(store, maxRowsPerShard: 10, rowGroupSize: 1);
        var timestamp = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc).AddTicks(1);
        const decimal measurement = 123456789.1234567890123456789m;
        var rows = new[]
        {
            new ProductionRowModel("farm", "first", timestamp, measurement, null,
                null, null, timestamp, null, timestamp.AddMinutes(10), "CurrentPowerCurvekWh"),
            new ProductionRowModel("farm", "second", timestamp, 0m, 1m,
                null, null, timestamp, null, timestamp.AddMinutes(10), null)
        };
        var result = await writer.WriteDayAsync("farm", DateOnly.FromDateTime(timestamp), 7, rows, CancellationToken.None);
        Assert.NotNull(result.BlobPath);
        using var stream = new MemoryStream(store.Objects[result.BlobPath]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        Assert.Equal(2, reader.RowGroupCount);
        Assert.Equal(ProductionFields, reader.Schema.GetDataFields().Select(field => field.Name));
        using var group = reader.OpenRowGroupReader(0);
        var fields = reader.Schema.GetDataFields();
        var actual = new decimal?[1];
        await group.ReadAsync<decimal>(fields.Single(field => field.Name == "ActualProductionKWh"), actual);
        Assert.Equal(measurement, actual[0]);
        var times = new DateTime[1];
        await group.ReadAsync<DateTime>(fields.Single(field => field.Name == "StartTimeInterval"), times);
        Assert.Equal(timestamp.Ticks, times[0].Ticks);
        var endTimes = new DateTime[1];
        await group.ReadAsync<DateTime>(fields.Single(field => field.Name == "EndTimeInterval"), endTimes);
        Assert.Equal(timestamp.AddMinutes(10).Ticks, endTimes[0].Ticks);
        var updated = new DateTime[1];
        await group.ReadAsync<DateTime>(fields.Single(field => field.Name == "LastUpdatedDate"), updated);
        Assert.Equal(timestamp.Ticks, updated[0].Ticks);
    }

    [Fact]
    public async Task RefreshPartWritesExactVersionedNineColumnSchema()
    {
        var store = new MemoryObjectStore();
        var writer = new ProductionParquetWriter(store);
        var timestamp = new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);
        var row = new ProductionRowModel("farm", "turbine", timestamp, 1m, null, null, null, timestamp, null,
            timestamp.AddMinutes(10), "CurrentPowerCurvekWh");

        var references = await writer.WriteRefreshPartAsync("farm", 42, 9, "request", "attempt",
            ProductionParquetSchemaV2.Version, 1,
            new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2026, 9, 16, 0, 0, 0, DateTimeKind.Utc), 1,
            [new("2026-09-15", "2026-09-15")], [row], CancellationToken.None);

        var reference = Assert.Single(references);
        Assert.Contains("/mode=refresh/extraction=9/", reference.BlobPath);
        using var stream = new MemoryStream(store.Objects[reference.BlobPath]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        Assert.Equal(ProductionFields, reader.Schema.GetDataFields().Select(field => field.Name));
        Assert.Equal("2", reader.CustomMetadata!["payload_schema_version"]);
        Assert.Equal("Refresh", reader.CustomMetadata["extraction_mode"]);
        using var group = reader.OpenRowGroupReader(0);
        var identities = new string?[1];
        await group.ReadAsync(reader.Schema.GetDataFields().Single(field => field.Name == "WtgRdsppId"),
            identities.AsMemory(), null, CancellationToken.None);
        Assert.Equal("turbine", identities[0]);
    }

    [Fact]
    public async Task ProductionChangesPartWritesExactVersionedNineColumnSchema()
    {
        const decimal measurement = 123456789.1234567890123456789m;
        var store = new MemoryObjectStore();
        var timestamp = new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc).AddTicks(1234);
        var lastUpdated = timestamp.AddDays(2);
        var row = new ProductionChangeRowModel("turbine", timestamp, timestamp.AddMinutes(10), lastUpdated,
            measurement, null, "CurrentPowerCurvekWh", 0.123456789m, 5.25m);
        var writer = new ProductionChangesPartWriter(store, rowGroupSize: 1);
        var result = await writer.WritePartAsync(
            new ProductionChangesPartRequest("42", "farm", "Production", "production", "run", 12,
                lastUpdated.AddDays(-1), lastUpdated, 1),
            [row], CancellationToken.None);

        using var stream = new MemoryStream(store.Objects[result.BlobPath]);
        await using var reader = await ParquetReader.CreateAsync(stream);
        Assert.Equal(ProductionFields, reader.Schema.GetDataFields().Select(field => field.Name));
        Assert.Equal("2", reader.CustomMetadata!["payload_schema_version"]);
        Assert.Equal("Incremental", reader.CustomMetadata["extraction_mode"]);
        using var group = reader.OpenRowGroupReader(0);
        var fields = reader.Schema.GetDataFields();
        var actual = new decimal?[1];
        await group.ReadAsync<decimal>(fields.Single(field => field.Name == "ActualProductionKWh"), actual);
        Assert.Equal(measurement, actual[0]);
        var possible = new decimal?[1];
        await group.ReadAsync<decimal>(fields.Single(field => field.Name == "PossibleProductionKWh"), possible);
        Assert.Null(possible[0]);
        var starts = new DateTime[1];
        await group.ReadAsync<DateTime>(fields.Single(field => field.Name == "StartTimeInterval"), starts);
        Assert.Equal(timestamp.Ticks, starts[0].Ticks);
        var ends = new DateTime[1];
        await group.ReadAsync<DateTime>(fields.Single(field => field.Name == "EndTimeInterval"), ends);
        Assert.Equal(timestamp.AddMinutes(10).Ticks, ends[0].Ticks);
        var revisions = new DateTime[1];
        await group.ReadAsync<DateTime>(fields.Single(field => field.Name == "LastUpdatedDate"), revisions);
        Assert.Equal(lastUpdated.Ticks, revisions[0].Ticks);
    }

    internal sealed class MemoryObjectStore : IRawObjectStore
    {
        public Dictionary<string, byte[]> Objects { get; } = new();
        public bool CorruptDownloads { get; set; }
        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            Objects[path] = content.ToArray();
            return Task.CompletedTask;
        }
        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken) => Task.FromResult(Objects.ContainsKey(path));
        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken)
            => Task.FromResult(CorruptDownloads ? new byte[] { 0 } : Objects[path]);
    }
}