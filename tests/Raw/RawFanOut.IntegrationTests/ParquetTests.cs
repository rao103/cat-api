using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Adapters.Parquet;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.FanOut;
using CAT.RawFanOut.Operations.FanOut.Strategies;
using ProductionIngestor.Contracts;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
namespace CAT.RawFanOut.IntegrationTests;

public sealed class ParquetTests
{
    [Fact]
    public async Task ReadsRealProductionParquetWithUtcTimestamp()
    {
        var time = new DateTime(2026, 6, 3, 23, 50, 0, DateTimeKind.Utc);
        var bytes = await ParquetFixtures.WriteAsync("Production", ["T1", "T2"], new[] { time, time }, farmId: "legacy-name");
        var reader = new ParquetIdentityReader(new MemoryObjects(new() { ["raw/a.parquet"] = bytes }), new());
        var envelope = RawEnvelope.Parse(ParquetFixtures.Body("Production", ["raw/a.parquet"]), new());
        var rows = new List<RawIdentity>();
        await foreach (var row in reader.ReadAsync(envelope, default)) rows.Add(row);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(new DateOnly(2026, 6, 3), row.ProductionDate));
        Assert.All(rows, row => Assert.Equal(7, row.WindfarmId));
    }

    [Fact]
    public async Task ReadsProductionV2IdentityAndFarmFromEnvelope()
    {
        var time = new DateTime(2026, 6, 3, 23, 50, 0, DateTimeKind.Utc);
        var bytes = await ParquetFixtures.WriteProductionV2Async(["T1"], [time]);
        const string path = "raw/production/windfarm=7/mode=incremental/extraction=41/part.parquet";
        var body = JsonSerializer.Serialize(new ProductionChangesV2
        {
            SourceType = "Production",
            WindfarmId = "7",
            WindfarmName = "WF01",
            RunId = "integration-run",
            ExtractionToken = 41,
            AttemptId = "41",
            QueryAxis = "LastUpdatedDate",
            QueryStartInclusiveUtc = time,
            QueryEndExclusiveUtc = time.AddDays(1),
            QueryWindowNumber = 1,
            PartNumber = 1,
            BlobPath = path,
            RowCount = 1,
        });
        var store = new MemoryObjects(new() { [path] = bytes });
        var reader = new ParquetIdentityReader(store, new());
        var envelope = RawEnvelope.Parse(body, new());
        var rows = new List<RawIdentity>();
        await foreach (var row in reader.ReadAsync(envelope, default)) rows.Add(row);

        var identity = Assert.Single(rows);
        Assert.Equal(7, identity.WindfarmId);
        Assert.Equal("T1", identity.WtgName);
        Assert.Equal(new DateOnly(2026, 6, 3), identity.ProductionDate);
        Assert.Equal(ProductionParquetSchemaV2.Version, envelope.PayloadSchemaVersion);
    }

    [Fact]
    public async Task RejectsProductionV2IdentityWithIncompleteSchema()
    {
        var time = new DateTime(2026, 6, 3, 23, 50, 0, DateTimeKind.Utc);
        var bytes = await ParquetFixtures.WriteIncompleteProductionV2Async("T1", time);
        var reader = new ParquetIdentityReader(new MemoryObjects(new() { ["raw/a.parquet"] = bytes }), new());
        var envelope = RawEnvelope.Parse(ParquetFixtures.Body("Production", ["raw/a.parquet"]), new());

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in reader.ReadAsync(envelope, default)) { }
        });
    }

    [Fact]
    public async Task ReadsLegacyProductionChangesV1FieldNames()
    {
        var time = new DateTime(2026, 6, 3, 23, 50, 0, DateTimeKind.Utc);
        var bytes = await ParquetFixtures.WriteAsync("Production", ["T1"], new DateTime[] { time }, farmId: "legacy-name");
        const string path = "raw/production/windfarm=7/mode=incremental/extraction=41/legacy.parquet";
        var body = JsonSerializer.Serialize(new ProductionChangesV2
        {
            PayloadSchemaVersion = 1,
            SourceType = "Production",
            WindfarmId = "7",
            WindfarmName = "WF01",
            RunId = "legacy-run",
            ExtractionToken = 41,
            AttemptId = "41",
            QueryAxis = "LastUpdatedDate",
            QueryStartInclusiveUtc = time.AddDays(-1),
            QueryEndExclusiveUtc = time.AddDays(1),
            QueryWindowNumber = 1,
            PartNumber = 1,
            BlobPath = path,
            RowCount = 1,
        });
        var reader = new ParquetIdentityReader(new MemoryObjects(new() { [path] = bytes }), new());
        var envelope = RawEnvelope.Parse(body, new());
        var rows = new List<RawIdentity>();
        await foreach (var row in reader.ReadAsync(envelope, default)) rows.Add(row);

        var identity = Assert.Single(rows);
        Assert.Equal(7, identity.WindfarmId);
        Assert.Equal("T1", identity.WtgName);
        Assert.Equal(new DateOnly(2026, 6, 3), identity.ProductionDate);
    }
    [Fact]
    public async Task SealedPrefixEnumeratesOnlyDeclaredDirectDataFilesAndExcludesQuarantine()
    {
        const string prefix = "raw/production/windfarm=WF01/mode=refresh/extraction=41/";
        var timestamp = new DateTime(2026, 6, 3, 12, 0, 0, DateTimeKind.Utc);
        var first = await ParquetFixtures.WriteProductionV2Async(["T1"], [timestamp]);
        var second = await ParquetFixtures.WriteProductionV2Async(["T1"], [timestamp]);
        var store = new MemoryObjects(new Dictionary<string, byte[]>
        {
            [prefix + "part-1.parquet"] = first,
            [prefix + "part-2.parquet"] = second,
            [prefix + "part-3-quarantine.parquet"] = new byte[] { 1, 2, 3 },
            [prefix + "nested/ignored.parquet"] = new byte[] { 4, 5, 6 },
        });
        var selectedScope = new[] { new ProductionDateRangeV1("2026-06-03", "2026-06-03") };
        var ready = new ProductionExtractionReadyV1(2, "ExtractionReady", "Production", 7, "WF01",
            "Refresh", "ProductionSnapshot", ProductionParquetSchemaV2.Version, "ProductionTimestamp", "request", "attempt", 41,
            selectedScope,
            [new(1, new DateTime(2026, 6, 3, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2026, 6, 4, 0, 0, 0, DateTimeKind.Utc), selectedScope, 2, 2, true)],
            [], 2, false, ["T1"], null, ArtifactPrefix: prefix, ExpectedArtifactCount: 2,
            ArtifactSelectionRule: ProductionArtifactSelectionRules.DirectParquetFilesV1);
        var envelope = RawEnvelope.Parse(JsonSerializer.Serialize(ready), new());
        var reader = new ParquetIdentityReader(store, new());
        var planner = new FanOutPlanner(reader, new TestFarmCatalog(), new(), TestSupport.DefaultStrategies(new()), new NullTelemetry<FanOutPlanner>());

        var planned = await planner.PlanAsync(envelope, default);
        var work = JsonSerializer.Deserialize<CAT.Messaging.Contracts.ProductionWtgDayWorkV3>(Assert.Single(planned).Body)!;

        Assert.Equal(3, work.SchemaVersion);
        Assert.Equal(7, work.WindfarmId);
        Assert.Equal("WF01", work.WindfarmName);
        Assert.Equal(new[] { prefix + "part-1.parquet", prefix + "part-2.parquet" }, work.RawPaths);
        Assert.Equal(new CAT.Messaging.Contracts.ProductionTurbineIdentityV1[] { new(101, "T1") }, work.ItemTurbines);
        Assert.Empty(work.ScopeTurbines);
    }
    [Fact]
    public async Task ReadsInt64AlarmIdentityWithoutEventDate()
    {
        var bytes = await ParquetFixtures.WriteAsync("Alarm", ["T1", "T1"], new long[] { long.MaxValue, 42 });
        var reader = new ParquetIdentityReader(new MemoryObjects(new() { ["raw/a.parquet"] = bytes }), new());
        var rows = new List<RawIdentity>();
        await foreach (var row in reader.ReadAsync(RawEnvelope.Parse(ParquetFixtures.Body("Alarm", ["raw/a.parquet"]), new()), default)) rows.Add(row);
        Assert.Equal(long.MaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture), rows[0].EventId);
        Assert.All(rows, row => Assert.Null(row.ProductionDate));
    }
    [Theory]
    [InlineData("Curtailment")]
    public async Task IncludesTombstoneIdentityAndAllShards(string source)
    {
        var a = await ParquetFixtures.WriteAsync(source, ["T1"], new[] { "E1" });
        var b = await ParquetFixtures.WriteAsync(source, ["T2"], new[] { "E2" }, tombstones: true);
        var reader = new ParquetIdentityReader(new MemoryObjects(new() { ["raw/a.parquet"] = a, ["raw/b.parquet"] = b }), new());
        var plan = await new FanOutPlanner(reader, new TestFarmCatalog(), new(), TestSupport.DefaultStrategies(new()), new NullTelemetry<FanOutPlanner>()).PlanAsync(RawEnvelope.Parse(
            ParquetFixtures.Body(source, ["raw/b.parquet", "raw/a.parquet"]), new()), default);
        var message = JsonSerializer.Deserialize<CAT.Messaging.Contracts.HarmonizationWorkV2>(Assert.Single(plan).Body)!;
        Assert.Equal(new[] { "E1", "E2" }, message.EventIds);
        Assert.Equal(2, message.RawPaths.Length);
    }
    [Fact]
    public async Task OutageParquetPreservesEventMembersAndContributingPaths()
    {
        var updated = new DateTime(2026, 6, 3, 12, 0, 0, DateTimeKind.Utc);
        var partOne = await ParquetFixtures.WriteOutageAsync("WF01", "T1", "E1", updated, isTombstone: false);
        var partTwo = await ParquetFixtures.WriteOutageAsync("WF01", "T2", "E1", updated, isTombstone: false);
        const string firstPath = "raw/outage/part-1.parquet";
        const string secondPath = "raw/outage/part-2.parquet";
        var store = new MemoryObjects(new() { [firstPath] = partOne, [secondPath] = partTwo });
        var reader = new ParquetIdentityReader(store, new());
        var planner = new FanOutPlanner(reader, new TestFarmCatalog(), new(), TestSupport.DefaultStrategies(new()), new NullTelemetry<FanOutPlanner>());
        var body = ParquetFixtures.OutageBody("OutageEvents", [firstPath, secondPath]);

        var result = await planner.PlanAsync(RawEnvelope.Parse(body, new()), default);
        var work = JsonSerializer.Deserialize<CAT.Messaging.Contracts.EventWorkV3>(Assert.Single(result).Body)!;

        Assert.Equal(new[] { "E1" }, work.EventIds);
        Assert.Equal(new[] { 101, 102 }, work.Members.Select(member => member.WtgId!.Value));
        Assert.Equal(new[] { firstPath }, work.Members[0].RawPaths);
        Assert.Equal(new[] { secondPath }, work.Members[1].RawPaths);
        Assert.Equal(work.Members[0].EventKey, work.Members[1].EventKey);
        Assert.NotEqual(work.Members[0].MemberKey, work.Members[1].MemberKey);
    }
    [Fact]
    public async Task MissingParquetColumnFails()
    {
        var bytes = await ParquetFixtures.WriteAsync("Outage", ["T1"], new[] { "E1" });
        var reader = new ParquetIdentityReader(new MemoryObjects(new() { ["raw/a.parquet"] = bytes }), new());
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in reader.ReadAsync(RawEnvelope.Parse(ParquetFixtures.Body("Production", ["raw/a.parquet"]), new()), default)) { }
        });
    }
    [Fact]
    public async Task OversizedRowGroupIsRejectedBeforeColumnAllocation()
    {
        var bytes = await ParquetFixtures.WriteAsync("Alarm", ["T1", "T2"], new long[] { 1, 2 });
        var reader = new ParquetIdentityReader(new MemoryObjects(new() { ["raw/a.parquet"] = bytes }), new() { MaxRowsPerRowGroup = 1 });
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (var _ in reader.ReadAsync(RawEnvelope.Parse(ParquetFixtures.Body("Alarm", ["raw/a.parquet"]), new()), default)) { }
        });
    }
}
internal sealed class MemoryObjects(Dictionary<string, byte[]> data) : IRawObjectStore
{
    public Task<Stream> OpenSeekableAsync(string path, CancellationToken ct) =>
        Task.FromResult<Stream>(new MemoryStream(data[path], writable: false));

    public async IAsyncEnumerable<string> ListPathsAsync(string prefix,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var path in data.Keys.Where(path => path.StartsWith(prefix, StringComparison.Ordinal))
                     .Order(StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return path;
        }
        await Task.CompletedTask;
    }
}

internal sealed class TestFarmCatalog : IFarmCatalog
{
    public Task<FarmCatalog> LoadAsync(FarmCatalogRequest request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new FarmCatalog(request.WindfarmId, "WF01",
        [
            new CAT.Messaging.Contracts.TurbineIdentity(101, "T1"),
            new CAT.Messaging.Contracts.TurbineIdentity(102, "T2"),
        ]));
    }
}
internal static class TestSupport
{
    public static IRawFanOutStrategy[] DefaultStrategies(FanOutOptions options) =>
    [
        new ProductionFanOutStrategy(options),
        new CanonicalEventFanOutStrategy("Alarm"),
        new CanonicalEventFanOutStrategy("Outage"),
        new CanonicalEventFanOutStrategy("Curtailment"),
        new CanonicalEventFanOutStrategy("Adjustment"),
    ];
}
internal sealed class NullTelemetry<TCategory> : ITelemetry<TCategory>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public void Information(string message, string? runId = null, string? jobName = null, string? callerId = null) { }
    public void Warning(string message, string? runId = null, string? jobName = null, string? callerId = null) { }
    public void Error(string message, Exception error, string? runId = null, string? callerId = null) { }
}

internal static class ParquetFixtures
{
    public static string Body(string source, string[] paths) => JsonSerializer.Serialize(new
    {
        SourceType = source,
        WindfarmId = "7",
        DataDate = source == "Production" ? "2026-06-03" : null,
        ExtractionToken = 41L,
        BlobPath = paths.Length == 1 ? paths[0] : null,
        ShardPaths = paths.Length == 1 ? null : paths,
        RunId = "integration-run",
        TraceId = "integration-trace",
        IsFullSnapshot = false
    });
    public static string OutageBody(string payloadKind, string[] paths) => JsonSerializer.Serialize(new
    {
        SourceType = "Outage",
        WindfarmId = "7",
        ExtractionToken = 41L,
        PayloadKind = payloadKind,
        BlobPath = paths.Length == 1 ? paths[0] : null,
        ShardPaths = paths.Length == 1 ? null : paths,
        RunId = "integration-outage-run",
        IsFullSnapshot = false
    });

    public static async Task<byte[]> WriteOutageAsync(
        string farmName, string turbineName, string eventId, DateTime lastUpdatedUtc, bool isTombstone)
    {
        if (lastUpdatedUtc.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Outage fixture version timestamps must be UTC.", nameof(lastUpdatedUtc));
        var farm = new DataField<string>("WindfarmId");
        var turbine = new DataField<string>("WtgId");
        var eventField = new DataField<string>("EventId");
        var version = new DateTimeDataField("LastUpdatedDate", DateTimeFormat.DateAndTime);
        var tombstone = new DataField<bool>("IsTombstone");
        var schema = new ParquetSchema(farm, turbine, eventField, version, tombstone);
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(schema, stream))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync(farm, (IReadOnlyCollection<string>)[farmName], null);
            await group.WriteAsync(turbine, (IReadOnlyCollection<string>)[turbineName], null);
            await group.WriteAsync(eventField, (IReadOnlyCollection<string>)[eventId], null);
            await group.WriteAsync<DateTime>(version, new ReadOnlyMemory<DateTime>([lastUpdatedUtc]),
                null, null, CancellationToken.None);
            await group.WriteAsync<bool>(tombstone, new ReadOnlyMemory<bool>([isTombstone]),
                null, null, CancellationToken.None);
        }
        return stream.ToArray();
    }
    public static async Task<byte[]> WriteAsync(
        string source,
        string[] items,
        Array identities,
        bool tombstones = false,
        string farmId = "7")
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(identities);

        if (items.Length != identities.Length)
            throw new ArgumentException("Item and identity counts must match.", nameof(identities));

        var validIdentityType = source switch
        {
            "Production" => identities is DateTime[],
            "Alarm" => identities is long[],
            "Outage" or "Curtailment" => identities is string[],
            _ => throw new ArgumentOutOfRangeException(nameof(source), source, "Unsupported fixture source.")
        };

        if (!validIdentityType)
            throw new ArgumentException(
                "Use DateTime[] for Production, long[] for Alarm, and string[] for events.",
                nameof(identities));

        if (identities is DateTime[] timestamps &&
            timestamps.Any(value => value.Kind != DateTimeKind.Utc))
            throw new ArgumentException("Production fixture timestamps must be UTC.", nameof(identities));

        var farm = new DataField<string>("WindfarmId");
        var item = new DataField<string>("ItemId");
        DataField key = source switch
        {
            "Production" => new DateTimeDataField(
                "ProductionTimestampUtc", DateTimeFormat.DateAndTime),
            "Alarm" => new DataField<long>("AlarmId"),
            _ => new DataField<string>("EventId")
        };
        var tombstone = new DataField<bool>("IsTombstone");
        var schema = new ParquetSchema(farm, item, key, tombstone);

        var farms = Enumerable.Repeat(farmId, items.Length).ToArray();
        var flags = Enumerable.Repeat(tombstones, items.Length).ToArray();

        using var stream = new MemoryStream();

        // Dispose the row group first, then asynchronously finalize the file footer.
        await using (var writer = await ParquetWriter.CreateAsync(schema, stream))
        {
            using var group = writer.CreateRowGroup();

            // Columns must be written in exactly the schema order.
            await group.WriteAsync(farm, (IReadOnlyCollection<string>)farms, null);
            await group.WriteAsync(item, (IReadOnlyCollection<string>)items, null);

            switch (source)
            {
                case "Production":
                    await group.WriteAsync<DateTime>(
                        key, new ReadOnlyMemory<DateTime>((DateTime[])identities),
                        null, null, CancellationToken.None);
                    break;
                case "Alarm":
                    await group.WriteAsync<long>(
                        key, new ReadOnlyMemory<long>((long[])identities),
                        null, null, CancellationToken.None);
                    break;
                default:
                    await group.WriteAsync(
                        key, (IReadOnlyCollection<string>)(string[])identities, null);
                    break;
            }

            await group.WriteAsync<bool>(
                tombstone, new ReadOnlyMemory<bool>(flags),
                null, null, CancellationToken.None);
        }

        // The writer must be disposed before obtaining the final Parquet bytes.
        return stream.ToArray();
    }

    public static async Task<byte[]> WriteProductionV2Async(string[] items, DateTime[] timestamps)
    {
        var fields = new DataField[]
        {
            new DataField<string>(ProductionParquetSchemaV2.WtgRdsppId),
            new DateTimeDataField(ProductionParquetSchemaV2.StartTimeInterval, DateTimeFormat.Impala),
            new DateTimeDataField(ProductionParquetSchemaV2.EndTimeInterval, DateTimeFormat.Impala),
            new DecimalDataField(ProductionParquetSchemaV2.ActualProductionKWh, 38, 0, forceByteArrayEncoding: true, isNullable: true),
            new DecimalDataField(ProductionParquetSchemaV2.PossibleProductionKWh, 38, 0, forceByteArrayEncoding: true, isNullable: true),
            new DecimalDataField(ProductionParquetSchemaV2.LostProductionCapacitykWh, 38, 0, forceByteArrayEncoding: true, isNullable: true),
            new DataField<string>(ProductionParquetSchemaV2.ContractualPoPEColumn),
            new DecimalDataField(ProductionParquetSchemaV2.SiteAverageWindSpeedMps, 38, 0, forceByteArrayEncoding: true, isNullable: true),
            new DateTimeDataField(ProductionParquetSchemaV2.LastUpdatedDate, DateTimeFormat.Impala)
        };
        var ends = timestamps.Select(value => value.AddMinutes(10)).ToArray();
        var measurements = Enumerable.Repeat((decimal?)1m, items.Length).ToArray();
        var contractual = Enumerable.Repeat("curve", items.Length).ToArray();

        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync(fields[0], (IReadOnlyCollection<string>)items, null);
            await group.WriteAsync<DateTime>(fields[1], new ReadOnlyMemory<DateTime>(timestamps), null, null, CancellationToken.None);
            await group.WriteAsync<DateTime>(fields[2], new ReadOnlyMemory<DateTime>(ends), null, null, CancellationToken.None);
            await group.WriteAsync<decimal>(fields[3], measurements, cancellationToken: CancellationToken.None);
            await group.WriteAsync<decimal>(fields[4], measurements, cancellationToken: CancellationToken.None);
            await group.WriteAsync<decimal>(fields[5], measurements, cancellationToken: CancellationToken.None);
            await group.WriteAsync(fields[6], (IReadOnlyCollection<string>)contractual, null);
            await group.WriteAsync<decimal>(fields[7], measurements, cancellationToken: CancellationToken.None);
            await group.WriteAsync<DateTime>(fields[8], new ReadOnlyMemory<DateTime>(timestamps), null, null, CancellationToken.None);
        }
        return stream.ToArray();
    }

    public static async Task<byte[]> WriteIncompleteProductionV2Async(string item, DateTime timestamp)
    {
        var itemField = new DataField<string>(ProductionParquetSchemaV2.WtgRdsppId);
        var timeField = new DateTimeDataField(ProductionParquetSchemaV2.StartTimeInterval, DateTimeFormat.Impala);
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(itemField, timeField), stream))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync(itemField, (IReadOnlyCollection<string>)[item], null);
            await group.WriteAsync<DateTime>(timeField, new ReadOnlyMemory<DateTime>([timestamp]), null, null, CancellationToken.None);
        }
        return stream.ToArray();
    }
}