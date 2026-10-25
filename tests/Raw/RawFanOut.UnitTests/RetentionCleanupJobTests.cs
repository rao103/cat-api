using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.Retention;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.UnitTests;

public sealed class RetentionCleanupJobTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 1, 0, 0, 0, TimeSpan.Zero);

    private static RetentionCleanupJob CreateJob(IArtifactRetentionCleanupStore store, RetentionCleanupOptions? options = null) =>
        new(store, options ?? new(), new NullTelemetry<RetentionCleanupJob>(), new FakeTimeProvider(Now));

    [Fact]
    public async Task UsesSixCalendarMonthCutoffByDefault()
    {
        var store = new FakeRetentionCleanupStore();
        var job = CreateJob(store);
        await job.ExecuteAsync(default);
        Assert.Single(store.Calls);
        Assert.Equal(Now.AddMonths(-6), store.Calls[0].CutoffUtc);
    }

    [Fact]
    public async Task PassesConfiguredMaxParentsPerBatch()
    {
        var store = new FakeRetentionCleanupStore();
        var job = CreateJob(store, new RetentionCleanupOptions { MaxParentsPerBatch = 17 });
        await job.ExecuteAsync(default);
        Assert.Equal(17, store.Calls[0].MaxParentsPerBatch);
    }

    [Fact]
    public async Task RepeatsBoundedBatchesUntilNothingEligibleRemains()
    {
        var store = new FakeRetentionCleanupStore();
        store.Results.Enqueue(new(5, 12));
        store.Results.Enqueue(new(5, 11));
        store.Results.Enqueue(new(0, 0));
        var job = CreateJob(store);

        var summary = await job.ExecuteAsync(default);

        Assert.Equal(3, store.Calls.Count);
        Assert.Equal(10, summary.DeletedArtifacts);
        Assert.Equal(23, summary.DeletedBatches);
    }

    [Fact]
    public async Task NothingEligibleCompletesAfterOneCall()
    {
        var store = new FakeRetentionCleanupStore();
        var job = CreateJob(store);
        var summary = await job.ExecuteAsync(default);
        Assert.Single(store.Calls);
        Assert.Equal(0, summary.DeletedArtifacts);
        Assert.Equal(0, summary.DeletedBatches);
    }

    [Fact]
    public async Task CancellationStopsBeforeTheNextBoundedBatch()
    {
        var store = new FakeRetentionCleanupStore();
        store.Results.Enqueue(new(5, 5));
        store.Results.Enqueue(new(5, 5));
        using var cts = new CancellationTokenSource();
        var job = CreateJob(store);
        // Cancel after the first call is observed, simulated by failing the second call's precheck.
        var callCount = 0;
        var wrapped = new CancelingStore(store, () => { callCount++; if (callCount == 2) cts.Cancel(); });
        var cancelJob = new RetentionCleanupJob(wrapped, new(), new NullTelemetry<RetentionCleanupJob>(), new FakeTimeProvider(Now));
        await Assert.ThrowsAsync<OperationCanceledException>(() => cancelJob.ExecuteAsync(cts.Token));
    }

    [Fact]
    public async Task FailureIsLoggedAndRethrownAfterPartialProgress()
    {
        var store = new FakeRetentionCleanupStore();
        store.Results.Enqueue(new(3, 4));
        var job = CreateJob(store);
        var firstSummary = await job.ExecuteAsync(default);
        Assert.Equal(3, firstSummary.DeletedArtifacts);

        var failingStore = new FailAfterFirstCallStore();
        var failingJob = CreateJob(failingStore);
        await Assert.ThrowsAsync<IOException>(() => failingJob.ExecuteAsync(default));
        Assert.Equal(2, failingStore.CallCount);
    }

    [Fact]
    public async Task ResolvedIncidentsUseTheirOwnCutoffAndAreDrainedInBoundedBatches()
    {
        var incidents = new FakeIncidentRetentionStore();
        incidents.Results.Enqueue(7);
        incidents.Results.Enqueue(3);
        var options = new RetentionCleanupOptions { RetentionMonths = 6, ResolvedIncidentRetentionMonths = 12, MaxParentsPerBatch = 9 };
        var job = new RetentionCleanupJob(new FakeRetentionCleanupStore(), options, new NullTelemetry<RetentionCleanupJob>(),
            new FakeTimeProvider(Now), incidents);

        var summary = await job.ExecuteAsync(default);

        Assert.Equal(10, summary.DeletedIncidents);
        Assert.Equal(3, incidents.Calls.Count);
        Assert.All(incidents.Calls, call => { Assert.Equal(Now.AddMonths(-12), call.CutoffUtc); Assert.Equal(9, call.Max); });
    }

    private sealed class FakeIncidentRetentionStore : IDeadLetterRetentionStore
    {
        public Queue<int> Results { get; } = new();
        public List<(DateTimeOffset CutoffUtc, int Max)> Calls { get; } = [];
        public Task<int> DeleteResolvedBatchAsync(DateTimeOffset cutoffUtc, int maxIncidents, CancellationToken cancellationToken)
        {
            Calls.Add((cutoffUtc, maxIncidents));
            return Task.FromResult(Results.Count > 0 ? Results.Dequeue() : 0);
        }
    }

    private sealed class CancelingStore(FakeRetentionCleanupStore inner, Action onCall) : IArtifactRetentionCleanupStore
    {
        public Task<RetentionCleanupBatchResult> DeleteEligibleBatchAsync(
            DateTimeOffset cutoffUtc, int maxParentsPerBatch, CancellationToken cancellationToken)
        {
            onCall();
            cancellationToken.ThrowIfCancellationRequested();
            return inner.DeleteEligibleBatchAsync(cutoffUtc, maxParentsPerBatch, cancellationToken);
        }
    }

    private sealed class FailAfterFirstCallStore : IArtifactRetentionCleanupStore
    {
        public int CallCount { get; private set; }
        public Task<RetentionCleanupBatchResult> DeleteEligibleBatchAsync(
            DateTimeOffset cutoffUtc, int maxParentsPerBatch, CancellationToken cancellationToken)
        {
            CallCount++;
            if (CallCount == 1) return Task.FromResult(new RetentionCleanupBatchResult(3, 4));
            throw new IOException("Cleanup batch failed.");
        }
    }

    private sealed class FakeTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
