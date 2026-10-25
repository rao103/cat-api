using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.DeadLetters;
using CAT.RawFanOut.Operations.Delivery;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.UnitTests;
public sealed class DeliveryAndPublishingTests
{
    private static readonly QueueDelivery Delivery = new("message-1","receipt-1","{}",new Dictionary<string,string>());
    [Fact] public async Task PartialBatchRetrySendsOnlyFailedEntries()
    {
        var transport = new Transport();
        transport.Respond = messages => transport.Calls.Count == 1 ?
            new([messages[0].Id],[new(messages[1].Id,false,"Throttled")]) :
            new(messages.Select(x=>x.Id).ToArray(),[]);
        await new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()).PublishAsync([new("a","{}"),new("b","{}")],default);
        Assert.Equal("b",Assert.Single(transport.Calls[1]).Id);
    }
    [Fact] public async Task BatchIsLimitedToTenEntries()
    {
        var transport = new Transport();
        await new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()).PublishAsync(
            Enumerable.Range(0,21).Select(x=>new OutgoingMessage(x.ToString(),"{}")).ToArray(),default);
        Assert.Equal(new[]{10,10,1},transport.Calls.Select(x=>x.Length));
    }
    [Fact] public async Task AggregateUtf8ByteLimitIsRespected()
    {
        var transport = new Transport();
        await new ReliableBatchPublisher(transport,new(){MaxMessageBytes=1024,MaxBatchBytes=1024},new NullTelemetry<ReliableBatchPublisher>()).PublishAsync(
            [new("a",new string('界',200)),new("b",new string('界',200))],default);
        Assert.Equal(2,transport.Calls.Count);
    }
    [Fact] public async Task HttpSuccessWithMissingResultsIsFailure()
    {
        var transport = new Transport{Respond = _ => new([],[])};
        await Assert.ThrowsAsync<IOException>(()=>new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()).PublishAsync([new("a","{}")],default));
    }
    [Fact] public async Task PermanentBatchErrorFailsParent()
    {
        var transport = new Transport{Respond = _ => new([],[new("a",true,"InvalidMessageContents")])};
        await Assert.ThrowsAsync<InvalidDataException>(()=>new ReliableBatchPublisher(transport,new(),new NullTelemetry<ReliableBatchPublisher>()).PublishAsync([new("a","{}")],default));
        Assert.Single(transport.Calls);
    }
    [Fact] public async Task ExhaustedRetriesFailParent()
    {
        var transport = new Transport{Respond = _ => new([],[new("a",false,"Throttled")])};
        await Assert.ThrowsAsync<IOException>(()=>new ReliableBatchPublisher(transport,new(){PublishAttempts=2},new NullTelemetry<ReliableBatchPublisher>()).PublishAsync([new("a","{}")],default));
        Assert.Equal(2,transport.Calls.Count);
    }
    [Fact] public async Task DeleteOccursOnlyAfterProcessingSuccess()
    {
        var queue=new Queue();
        await new DeliveryHandler(new()).HandleAsync(queue,Delivery,_=>
        { Assert.Equal(0,queue.Deletes); return Task.CompletedTask; },default);
        Assert.Equal(1,queue.Deletes);
    }
    [Fact] public async Task ProcessingFailureDoesNotDelete()
    {
        var queue=new Queue();
        await Assert.ThrowsAsync<IOException>(()=>new DeliveryHandler(new()).HandleAsync(queue,Delivery,
            _=>Task.FromException(new IOException()),default));
        Assert.Equal(0,queue.Deletes);
    }
    [Fact] public async Task RenewalFailureCancelsWorkAndPreventsDelete()
    {
        var queue=new Queue{RenewalFails=true};
        await Assert.ThrowsAnyAsync<Exception>(()=>new DeliveryHandler(new(){HeartbeatSeconds=1}).HandleAsync(queue,Delivery,
            ct=>Task.Delay(TimeSpan.FromSeconds(5),ct),default));
        Assert.True(queue.Renewals>0); Assert.Equal(0,queue.Deletes);
    }
    [Fact] public async Task ShutdownDoesNotAcknowledgeWork()
    {
        var queue=new Queue(); using var stop=new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>new DeliveryHandler(new()).HandleAsync(queue,Delivery,
            ct=>{stop.Cancel();return Task.CompletedTask;},stop.Token));
        Assert.Equal(0,queue.Deletes);
    }
    [Fact] public async Task DeleteFailureSurfacesForReplay()
    {
        var queue=new Queue{DeleteFails=true};
        await Assert.ThrowsAsync<IOException>(()=>new DeliveryHandler(new()).HandleAsync(queue,Delivery,_=>Task.CompletedTask,default));
    }
    [Fact] public async Task MalformedDlqBodyIsPreservedWithoutInventedArtifact()
    {
        var store=new DeadLetters();
        await new DeadLetterProcessor(store,new()).ProcessAsync("dlq",Delivery with{Body="not-json"},default);
        Assert.Equal("not-json",store.RecordedBody); Assert.Null(store.Artifact);
        // Only the evidenced parse failure is recorded as the error.
        Assert.NotNull(store.ErrorType); Assert.NotNull(store.LastError);
    }
    [Fact] public async Task ValidDlqEnvelopeRecordsNoInventedError()
    {
        var store=new DeadLetters();
        await new DeadLetterProcessor(store,new()).ProcessAsync("dlq",Delivery with{Body=Fixtures.Body()},default);
        Assert.NotNull(store.Artifact); Assert.Null(store.ErrorType); Assert.Null(store.LastError);
    }
    [Fact] public async Task FailedArchiveSurfacesAndPreventsReceiptDeletion()
    {
        var store=new DeadLetters{FailRecord=true}; var queue=new Queue();
        await Assert.ThrowsAsync<IOException>(()=>new DeliveryHandler(new()).HandleAsync(queue,Delivery,
            ct=>new DeadLetterProcessor(store,new()).ProcessAsync("dlq",Delivery,ct),default));
        Assert.Equal(0,queue.Deletes);
    }
    [Fact] public async Task ReplayFailureCannotMarkReplayOrResolveIncident()
    {
        var store=new DeadLetters();
        await Assert.ThrowsAsync<IOException>(()=>new ReplayIncident(store,new Processor(true)).ExecuteAsync(Guid.NewGuid(),default));
        Assert.False(store.Resolved); Assert.False(store.ReplayRequested);
    }
    [Fact] public async Task ReplaySuccessRecordsRequestButLeavesIncidentUnresolved()
    {
        var store=new DeadLetters();
        await new ReplayIncident(store,new Processor()).ExecuteAsync(Guid.NewGuid(),default);
        Assert.True(store.ReplayRequested); Assert.False(store.Resolved);
    }
    [Fact] public async Task ExplicitClosureIsPassedToTheStoreWithReasonAndOperator()
    {
        var store=new DeadLetters(); var id=Guid.NewGuid();
        await new ResolveIncident(store).ExecuteAsync(new ResolveIncidentRequest(id,"verified","alice"),default);
        Assert.True(store.Resolved); Assert.Equal("verified",store.Resolution!.Reason); Assert.Equal("alice",store.Resolution.Operator);
    }
}
