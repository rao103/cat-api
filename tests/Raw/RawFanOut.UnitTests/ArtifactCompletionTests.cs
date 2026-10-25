using CAT.Messaging.Contracts;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.Delivery;
using CAT.RawFanOut.Operations.FanOut;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
namespace CAT.RawFanOut.UnitTests;
public sealed class ArtifactCompletionTests
{
    [Fact] public async Task DatabaseFailurePublishesNothing()
    {
        var transport=new Transport();var store=new ArtifactStore{Fail=true};
        var processor=new RawFanOutProcessor(TestPlanner.Create(new Rows(Fixtures.Production("T1"))),new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()),new(),store,new NullTelemetry<RawFanOutProcessor>());
        await Assert.ThrowsAsync<IOException>(()=>processor.ProcessAsync(Fixtures.Body(),default));
        Assert.Empty(transport.Calls);Assert.Equal(0,store.Dispatched);
    }
    [Fact] public async Task PublishFailureDoesNotMarkFanOutComplete()
    {
        var transport=new Transport{Respond=messages=>new([],messages.Select(m=>new CAT.RawFanOut.Models.PublishFailure(m.Id,true,"rejected")).ToArray())};
        var store=new ArtifactStore();
        var processor=new RawFanOutProcessor(TestPlanner.Create(new Rows(Fixtures.Production("T1"))),new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()),new(),store,new NullTelemetry<RawFanOutProcessor>());
        await Assert.ThrowsAsync<InvalidDataException>(()=>processor.ProcessAsync(Fixtures.Body(),default));
        Assert.Equal(1,store.Saves);Assert.Equal(0,store.Dispatched);
    }
    [Fact] public async Task EmptyArtifactEmitsReconciliationAndMarksDispatched()
    {
        var transport=new Transport();var store=new ArtifactStore();
        var processor=new RawFanOutProcessor(TestPlanner.Create(new Rows()),new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()),new(),store,new NullTelemetry<RawFanOutProcessor>());
        await processor.ProcessAsync(Fixtures.EmptyOutageSnapshotBody(),default);
        Assert.Single(Assert.Single(transport.Calls));Assert.Equal(1,store.Dispatched);
    }
    [Fact] public async Task EmptyIncrementalArtifactDoesNotTriggerRemovalReconciliation()
    {
        var transport=new Transport();var store=new ArtifactStore();
        var processor=new RawFanOutProcessor(TestPlanner.Create(new Rows()),new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()),new(),store,new NullTelemetry<RawFanOutProcessor>());
        await processor.ProcessAsync(Fixtures.OutageEventsBody(),default);
        Assert.DoesNotContain(transport.Calls.SelectMany(batch => batch),
            message => message.Body.Contains(HarmonizationWorkKinds.EventSnapshotReconciliation, System.StringComparison.Ordinal));
        Assert.Equal(1,store.Dispatched);
    }
    [Fact] public async Task DispatchMarkerFailureLeavesParentUndeleted()
    {
        var transport=new Transport();var store=new ArtifactStore{FailMark=true};
        var processor=new RawFanOutProcessor(TestPlanner.Create(new Rows()),new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()),new(),store,new NullTelemetry<RawFanOutProcessor>());
        var queue=new Queue();var delivery=new CAT.RawFanOut.Models.QueueDelivery("id","receipt",Fixtures.Body(),new Dictionary<string,string>());
        await Assert.ThrowsAsync<IOException>(()=>new DeliveryHandler(new()).HandleAsync(queue,delivery,ct=>processor.ProcessAsync(delivery.Body,ct),default));
        Assert.NotEmpty(transport.Calls);Assert.Equal(0,queue.Deletes);
    }
}
