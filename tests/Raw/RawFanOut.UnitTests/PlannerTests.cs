using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.FanOut;
using CAT.RawFanOut.Operations.FanOut.Strategies;
using CAT.RawFanOut.Operations.Delivery;
namespace CAT.RawFanOut.UnitTests;
public sealed class PlannerTests
{
    [Fact] public async Task ProductionIsDistinctSortedAndBatched()
    {
        var options = new FanOutOptions { ItemBatchSize=2 };
        var rows = new Rows(Fixtures.Production("T3"), Fixtures.Production("T1"), Fixtures.Production("T2"), Fixtures.Production("T1"));
        var planner = TestPlanner.Create(rows,options);
        var result = await planner.PlanAsync(RawEnvelope.Parse(Fixtures.Body(),options), default);
        var messages = result.Select(x=>JsonSerializer.Deserialize<ProductionWtgDayWorkV2>(x.Body)!).ToArray();
        Assert.Equal(new[]{"T1","T2"},messages[0].WtgNames);
        Assert.Equal(new[]{"T3"},messages[1].WtgNames);
        Assert.Equal(HarmonizationWorkKinds.ProductionSnapshotReconciliation,messages[2].WorkKind);
        var replay = await planner.PlanAsync(RawEnvelope.Parse(Fixtures.Body(),options),default);
        Assert.Equal(result.Select(x=>x.Id),replay.Select(x=>x.Id));
    }
    [Fact] public async Task EmptyProductionStillRequestsAbsenceReconciliation()
    {
        var result = await TestPlanner.Create(new Rows()).PlanAsync(RawEnvelope.Parse(Fixtures.Body(),new()),default);
        Assert.Single(result);
        Assert.Equal(HarmonizationWorkKinds.ProductionSnapshotReconciliation,
            JsonSerializer.Deserialize<ProductionWtgDayWorkV2>(result[0].Body)!.WorkKind);
    }
    [Fact]
    public async Task ExtractionReadyGroupsAllPartPathsAndEmitsOnlySelectedDates()
    {
        var rows = new Rows(
            Fixtures.Production("T1", new DateOnly(2026, 6, 3), "raw/part-1.parquet"),
            Fixtures.Production("T1", new DateOnly(2026, 6, 3), "raw/part-2.parquet"),
            Fixtures.Production("T2", new DateOnly(2026, 6, 6), "raw/part-1.parquet"));
        var catalog = new FakeFarmCatalog();
        var planner = TestPlanner.Create(rows, new(), catalog);
        var result = await planner.PlanAsync(
            RawEnvelope.Parse(Fixtures.ExtractionReadyBody(), new()), default);
        var work = result.Select(message => JsonSerializer.Deserialize<ProductionWtgDayWorkV3>(message.Body)!).ToArray();

        Assert.Equal(3, work.Length);
        Assert.Equal(7, catalog.LastRequest!.WindfarmId);
        Assert.Equal(7, work[0].WindfarmId);
        Assert.Equal("WF01", work[0].WindfarmName);
        Assert.Equal(new[] { "2026-06-03", "2026-06-04", "2026-06-06" }, work.Select(item => item.DataDate));
        Assert.Equal(new[] { "raw/part-1.parquet", "raw/part-2.parquet" }, work[0].RawPaths);
        Assert.Equal(new[] { new ProductionTurbineIdentityV1(101, "T1") }, work[0].ItemTurbines);
        Assert.False(work[0].HasNoSourceRows);
        Assert.True(work[1].HasNoSourceRows);
        Assert.Empty(work[1].ItemTurbines);
        Assert.Equal(new[] { new ProductionTurbineIdentityV1(101, "T1"), new ProductionTurbineIdentityV1(102, "T2") },
            work[1].ScopeTurbines);
        Assert.False(work[2].HasNoSourceRows);
        Assert.Equal(new[] { new ProductionTurbineIdentityV1(102, "T2") }, work[2].ItemTurbines);
        Assert.All(work, item => Assert.False(item.AllowRemovalReconciliation));
        Assert.Equal(result.Select(message => message.Id), (await planner.PlanAsync(
            RawEnvelope.Parse(Fixtures.ExtractionReadyBody(), new()), default)).Select(message => message.Id));
    }

    // OriginalMessageBody is a required member of the published V2/V3 contracts; removal needs a coordinated version change.
    [Fact]
    public async Task OutgoingMessagesStillCarryTheParentEnvelopeUntilTheContractIsVersioned()
    {
        var body = Fixtures.ExtractionReadyBody();
        var rows = new Rows(
            Fixtures.Production("T1", new DateOnly(2026, 6, 3), "raw/part-1.parquet"),
            Fixtures.Production("T1", new DateOnly(2026, 6, 3), "raw/part-2.parquet"),
            Fixtures.Production("T2", new DateOnly(2026, 6, 6), "raw/part-1.parquet"));
        var result = await TestPlanner.Create(rows, new(), new FakeFarmCatalog())
            .PlanAsync(RawEnvelope.Parse(body, new()), default);

        Assert.All(result.Select(message => JsonSerializer.Deserialize<ProductionWtgDayWorkV3>(message.Body)!),
            work => Assert.Equal(body, work.OriginalMessageBody));
    }

    [Fact]
    public async Task ExtractionReadyFailsWhenCatalogDoesNotContainEveryRequestedWtg()
    {
        var catalog = new FakeFarmCatalog();
        catalog.MissingWtgNames.Add("T2");
        var planner = TestPlanner.Create(new Rows(), new(), catalog);

        await Assert.ThrowsAsync<InvalidDataException>(() => planner.PlanAsync(
            RawEnvelope.Parse(Fixtures.ExtractionReadyBody(), new()), default));
    }
    [Fact]
    public async Task ExtractionReadyEnforcesPerArtifactRowLimit()
    {
        var options = new FanOutOptions { MaxRowsPerArtifact = 2 };
        var rows = new Rows(
            Fixtures.Production("T1", new DateOnly(2026, 6, 3), "raw/part-1.parquet"),
            Fixtures.Production("T1", new DateOnly(2026, 6, 3), "raw/part-1.parquet"),
            Fixtures.Production("T2", new DateOnly(2026, 6, 6), "raw/part-2.parquet"));

        await Assert.ThrowsAsync<InvalidDataException>(() => TestPlanner.Create(rows, options).PlanAsync(
            RawEnvelope.Parse(Fixtures.ExtractionReadyBody(), options), default));
    }
    [Fact]
    public async Task ExtractionReadyRejectsRowsThatDifferFromDeclaredTotal()
    {
        var rows = new Rows(
            Fixtures.Production("T1", new DateOnly(2026, 6, 3), "raw/part-1.parquet"),
            Fixtures.Production("T2", new DateOnly(2026, 6, 6), "raw/part-2.parquet"));

        await Assert.ThrowsAsync<InvalidDataException>(() => TestPlanner.Create(rows, new()).PlanAsync(
            RawEnvelope.Parse(Fixtures.ExtractionReadyBody(), new()), default));
    }
    [Fact] public async Task OutageEventsPreserveCanonicalMembershipAndExactMemberPaths()
    {
        var rows = new Rows(
            new RawIdentity(7, 101, "E1", null) { WtgName = "T1", RawPath = "raw/outage/a.parquet" },
            new RawIdentity(7, 101, "E1", null) { WtgName = "T1", RawPath = "raw/outage/b.parquet" },
            new RawIdentity(7, 102, "E1", null) { WtgName = "T2", RawPath = "raw/outage/b.parquet" },
            new RawIdentity(7, 103, "E2", null) { WtgName = "T3", RawPath = "raw/outage/c.parquet" });
        var body = Fixtures.OutageEventsBody(paths: ["raw/outage/a.parquet", "raw/outage/b.parquet", "raw/outage/c.parquet"]);
        var result = await TestPlanner.Create(rows, new()).PlanAsync(RawEnvelope.Parse(body, new()), default);
        var work = JsonSerializer.Deserialize<EventWorkV3>(Assert.Single(result).Body)!;

        Assert.Equal(HarmonizationWorkKinds.CanonicalEvents, work.WorkKind);
        Assert.Equal(new[] { "E1", "E2" }, work.EventIds);
        var e1Members = work.Members.Where(member => member.EventId == "E1").OrderBy(member => member.WtgId).ToArray();
        Assert.Equal(new[] { 101, 102 }, e1Members.Select(member => member.WtgId!.Value));
        Assert.Equal(new[] { "raw/outage/a.parquet", "raw/outage/b.parquet" }, e1Members[0].RawPaths);
        Assert.Equal(new[] { "raw/outage/b.parquet" }, e1Members[1].RawPaths);
        Assert.Equal("T1", e1Members[0].WtgName);
        Assert.Equal(e1Members[0].EventKey, e1Members[1].EventKey);
        Assert.NotEqual(e1Members[0].MemberKey, e1Members[1].MemberKey);
        Assert.Equal(body, work.OriginalMessageBody);
    }
    [Fact] public async Task EmptyOutageSnapshotEmitsReceiptOnlyBarrierWithFrozenWtgScope()
    {
        var result = await TestPlanner.Create(new Rows()).PlanAsync(
            RawEnvelope.Parse(Fixtures.EmptyOutageSnapshotBody(),new()),default);
        var work = JsonSerializer.Deserialize<EventWorkV3>(Assert.Single(result).Body)!;

        Assert.Equal(HarmonizationWorkKinds.EventSnapshotReconciliation, work.WorkKind);
        Assert.True(work.IsFullSnapshot);
        Assert.Equal(EventReconciliationModes.ReceiptOnly, work.ReconciliationMode);
        Assert.Equal(new[] { 101, 102, 103 }, work.SnapshotScope!.WtgIds);
        Assert.Equal(DateTimeOffset.Parse("2026-07-01T00:01:00+00:00"), work.SnapshotObservedAtUtc);
        Assert.Empty(work.ExpectedEventBatchIds);
        Assert.Empty(work.Members);
    }
    [Fact]
    public async Task OutageSnapshotBarrierDependsOnEveryCanonicalBatch()
    {
        const string path = "raw/outage/snapshot/part-1.parquet";
        var row = new RawIdentity(7, 101, "E1", null) { WtgName = "T1", RawPath = path };
        var result = await TestPlanner.Create(new Rows(row)).PlanAsync(
            RawEnvelope.Parse(Fixtures.OutageSnapshotBody(path, 1), new()), default);
        var work = result.Select(message => JsonSerializer.Deserialize<EventWorkV3>(message.Body)!).ToArray();
        var eventWork = Assert.Single(work.Where(item => item.WorkKind == HarmonizationWorkKinds.CanonicalEvents));
        var barrier = Assert.Single(work.Where(item => item.WorkKind == HarmonizationWorkKinds.EventSnapshotReconciliation));

        Assert.Null(eventWork.ReconciliationMode);
        Assert.Equal(new[] { eventWork.BatchId }, barrier.ExpectedEventBatchIds);
        Assert.Equal(EventReconciliationModes.ReceiptOnly, barrier.ReconciliationMode);
        Assert.Equal(new[] { 101, 102, 103 }, barrier.SnapshotScope!.WtgIds);
    }
    [Fact] public async Task EmptyIncrementalEventFeedDoesNotRequestSnapshotDeletion()
    {
        var result = await TestPlanner.Create(new Rows()).PlanAsync(
            RawEnvelope.Parse(Fixtures.OutageEventsBody(), new()), default);
        Assert.DoesNotContain(result, message =>
            message.Body.Contains(HarmonizationWorkKinds.EventSnapshotReconciliation, StringComparison.Ordinal));
    }
    [Fact] public async Task MissingShardPublishesNothing()
    {
        var transport = new Transport();
        var processor = new RawFanOutProcessor(TestPlanner.Create(new Rows(Fixtures.Production("T1")){FailAfterRows=true}),new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()),new(),new ArtifactStore(),new NullTelemetry<RawFanOutProcessor>());
        await Assert.ThrowsAsync<IOException>(() => processor.ProcessAsync(Fixtures.Body(shards:["raw/a","raw/b"]),default));
        Assert.Empty(transport.Calls);
    }
    [Fact] public async Task WrongFarmFailsWholeArtifact()
    {
        var row = new RawIdentity(999,null,null,new DateOnly(2026,6,3)) { WtgName = "T1" };
        await Assert.ThrowsAsync<InvalidDataException>(()=>TestPlanner.Create(new Rows(row)).PlanAsync(
            RawEnvelope.Parse(Fixtures.Body(),new()),default));
    }
    [Fact] public async Task CrossMidnightProductionRowIsRejected()
    {
        var row = new RawIdentity(7,null,null,new DateOnly(2026,6,4)) { WtgName = "T1" };
        await Assert.ThrowsAsync<InvalidDataException>(()=>TestPlanner.Create(new Rows(row)).PlanAsync(
            RawEnvelope.Parse(Fixtures.Body(),new()),default));
    }
    [Fact] public async Task TooManyIdentitiesFailsBeforePublish()
    {
        var options = new FanOutOptions{MaxIdentities=1};
        await Assert.ThrowsAsync<InvalidDataException>(()=>TestPlanner.Create(new Rows(Fixtures.Production("T1"),Fixtures.Production("T2")),options)
            .PlanAsync(RawEnvelope.Parse(Fixtures.Body(),options),default));
    }
    [Fact] public async Task CancellationStopsEnumeration()
    {
        using var ct = new CancellationTokenSource(); ct.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>TestPlanner.Create(new Rows(Fixtures.Production("T1")))
            .PlanAsync(RawEnvelope.Parse(Fixtures.Body(),new()),ct.Token));
    }
    [Fact] public async Task OversizedPlanFailsBeforePublishingAnyBatch()
    {
        var options=new FanOutOptions{MaxPlanBytes=1};
        var transport=new Transport();
        var processor=new RawFanOutProcessor(TestPlanner.Create(new Rows(Fixtures.Production("T1")),options),new ReliableBatchPublisher(transport,options,new NullTelemetry<ReliableBatchPublisher>()),options,new ArtifactStore(),new NullTelemetry<RawFanOutProcessor>());
        await Assert.ThrowsAsync<InvalidDataException>(()=>processor.ProcessAsync(Fixtures.Body(),default));
        Assert.Empty(transport.Calls);
    }
    [Fact]
    public async Task AdjustmentWithNoCurrentMembersStillEmitsParentIdentity()
    {
        var row = new RawIdentity(7, null, "ADJ1", null);

        var options = new FanOutOptions();
        var planner = TestPlanner.Create(new Rows(row), options);
        var artifact = RawEnvelope.Parse(
            Fixtures.Body("Adjustment", null),
            options);

        var result = await planner.PlanAsync(
            artifact,
            CancellationToken.None);

        var work = JsonSerializer.Deserialize<HarmonizationWorkV2>(
            Assert.Single(result).Body)!;

        Assert.Equal(new[] { "ADJ1" }, work.EventIds);
        Assert.Empty(work.ItemIds);
    }
}
