using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Adapters.Parquet;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.FanOut;

namespace CAT.RawFanOut.IntegrationTests;

public sealed class OutageSampleTests
{
    [Theory]
    [InlineData("outage-events.parquet", "OutageEvents", 41)]
    [InlineData("outage-deleted-events.parquet", "OutageDeletedEvents", 1)]
    public async Task SuppliedProducerFilesResolveNamesAndPreserveFeed(string filename, string payloadKind, int expectedRows)
    {
        const string path = "raw/outage-sample.parquet";
        var bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", filename));
        var objects = new MemoryObjects(new() { [path] = bytes });
        var options = new FanOutOptions { EventBatchSize = 10 };
        var reader = new ParquetIdentityReader(objects, options);
        var body = JsonSerializer.Serialize(new
        {
            EnvelopeVersion = 2,
            MessageKind = "RawArtifactAvailable",
            PayloadSchemaVersion = 1,
            SourceType = "Outage",
            WindfarmId = "2",
            WindfarmName = "WOW02",
            ExtractionMode = "Incremental",
            PayloadKind = payloadKind,
            RunId = "sample-run",
            ExtractionToken = 26L,
            BlobPath = path,
            RowCount = expectedRows,
            IsFullSnapshot = false
        });
        var envelope = RawEnvelope.Parse(body, options);
        var identities = new System.Collections.Generic.List<RawIdentity>();
        await foreach (var identity in reader.ReadAsync(envelope, default)) identities.Add(identity);
        Assert.Equal(expectedRows, identities.Count);

        var turbineNames = identities.Select(row => row.WtgName!).Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal).ToArray();
        var catalog = new SampleCatalog(turbineNames);
        var planner = new FanOutPlanner(reader, catalog, options,
            TestSupport.DefaultStrategies(options), new NullTelemetry<FanOutPlanner>());
        var plan = await planner.PlanAsync(envelope, default);
        var work = plan.Select(message => JsonSerializer.Deserialize<EventWorkV3>(message.Body)!).ToArray();

        Assert.Equal(expectedRows, work.Sum(message => message.Members.Length));
        Assert.All(work, message =>
        {
            Assert.Equal(payloadKind, message.PayloadKind);
            Assert.False(message.IsFullSnapshot);
            Assert.Equal(new[] { path }, message.RawPaths);
            Assert.All(message.Members, member => Assert.NotNull(member.WtgId));
        });
        Assert.Equal(plan, await planner.PlanAsync(envelope, default));
        if (payloadKind == "OutageDeletedEvents")
            Assert.Equal("WTG-ACC-WOW02F10-260914205417", Assert.Single(work).Members[0].EventId);
    }

    [Fact]
    public async Task ProducerManifestPartsSchemaSupportsEmptyReceiptOnlySnapshot()
    {
        const string manifestPath = "raw/outage/windfarm=WF01/mode=refresh/extraction=44/snapshot/manifest.json";
        var observedAt = new DateTime(2026, 7, 1, 0, 1, 0, DateTimeKind.Utc);
        var manifest = JsonSerializer.SerializeToUtf8Bytes(new
        {
            SchemaVersion = 1,
            WindfarmId = "7",
            WindfarmName = "WF01",
            RunId = "snapshot-run",
            ExtractionToken = 44L,
            ScopeStartUtc = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
            ScopeEndUtc = new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc),
            SnapshotAsOfUtc = observedAt,
            RecordCount = 0,
            Parts = Array.Empty<string>()
        });
        var objects = new MemoryObjects(new() { [manifestPath] = manifest });
        var body = JsonSerializer.Serialize(new
        {
            SourceType = "Outage",
            WindfarmId = "7",
            WindfarmName = "WF01",
            ExtractionToken = 44L,
            PayloadKind = "OutageSnapshot",
            ManifestPath = manifestPath,
            RunId = "snapshot-run",
            IsFullSnapshot = true,
            WindowStartUtc = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
            WindowEndUtc = new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc)
        });
        var options = new FanOutOptions();
        var planner = new FanOutPlanner(new ParquetIdentityReader(objects, options), new TestFarmCatalog(), options,
            TestSupport.DefaultStrategies(options), new NullTelemetry<FanOutPlanner>(), objects);

        var work = JsonSerializer.Deserialize<EventWorkV3>(Assert.Single(await planner.PlanAsync(
            RawEnvelope.Parse(body, options), default)).Body)!;

        Assert.Equal(HarmonizationWorkKinds.EventSnapshotReconciliation, work.WorkKind);
        Assert.Equal(manifestPath, work.ManifestPath);
        Assert.Equal(new DateTimeOffset(observedAt), work.SnapshotObservedAtUtc);
        Assert.Equal(EventReconciliationModes.ReceiptOnly, work.ReconciliationMode);
        Assert.Empty(work.ExpectedEventBatchIds);
    }

    private sealed class SampleCatalog(string[] turbineNames) : IFarmCatalog
    {
        public Task<FarmCatalog> LoadAsync(FarmCatalogRequest request, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(new FarmCatalog(2, "WOW02", turbineNames
                .Select((name, index) => new TurbineIdentity(index + 1, name)).ToArray()));
        }
    }
}