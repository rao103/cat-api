using System.Globalization;
using CurtailmentIngestor.Configuration;
using CurtailmentIngestor.Contracts;
using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Interfaces;
using CurtailmentIngestor.Models;
using CurtailmentIngestor.Monitoring;
using CurtailmentIngestor.Operations.Discovery;
using CurtailmentIngestor.Operations.Execution;
using CurtailmentIngestor.Serialization;

namespace CurtailmentIngestor.UnitTests;

/// <summary>
/// Lifecycle/numeric-boundary/fencing coverage for the shared extraction_cursor/extraction_run
/// model as wired through <see cref="CurtailmentIngestionRunner"/>: run-audit creation on lease
/// acquisition, atomic completion, numeric (not lexical) committed-id comparison, and zombie
/// handling on rejected checkpoints/final completion/cancellation-during-publication.
/// </summary>
public sealed class IngestionRunnerTests
{
    private static readonly DateTime Start = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(IngestionMode.Incremental)]
    [InlineData(IngestionMode.Refresh)]
    public async Task TriggeredRunPublishesSuppliedOperationId(IngestionMode mode)
    {
        const string operationId = "bb2e7ced-3d1c-43f9-9535-de6217640b76";
        var fixture = new Fixture { DiscoveredId = 1 };
        var request = new IngestionRequestModel("farm", "Curtailment", mode, "refresh-run")
        {
            OperationId = operationId,
        };

        var result = await fixture.Runner.RunAsync(request, CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(operationId, fixture.ReceivedOperationId);
        Assert.NotEmpty(fixture.Published);
        Assert.All(fixture.Published, message => Assert.Equal(operationId, message.OperationId));
    }

    [Fact]
    public async Task ScheduledRunPublishesPersistedOperationId()
    {
        var fixture = new Fixture { DiscoveredId = 1 };

        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Null(fixture.ReceivedOperationId);
        Assert.Equal(fixture.GeneratedOperationId, Assert.Single(fixture.Published).OperationId);
    }

    [Fact]
    public async Task InvalidOperationIdIsRejectedBeforeRunStarts()
    {
        var fixture = new Fixture();
        var request = new IngestionRequestModel("farm", "Curtailment", IngestionMode.Incremental) { OperationId = " " };

        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Runner.RunAsync(request, CancellationToken.None));

        Assert.False(fixture.RunStarted);
        Assert.Empty(fixture.Published);
    }

    [Fact]
    public async Task SuccessfulRunStartsAndAtomicallyCompletesTheRunAudit()
    {
        var fixture = new Fixture();
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.True(fixture.RunStarted);
        Assert.Equal(1, fixture.RunsCompleted);
        Assert.Equal(0, fixture.RunsFailed);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task NumericCommittedIdComparisonIsNotLexical()
    {
        // Committed id "9" as text would lexically compare greater than "10"; the runner must
        // parse both as numbers so 10 is correctly treated as forward progress.
        var fixture = new Fixture { InitialCommittedId = "9", DiscoveredId = 10 };
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal("10", Assert.Single(fixture.CommittedIds));
    }

    [Fact]
    public async Task NumericCommittedIdAtBoundaryDoesNotRegress()
    {
        // Same LastUpdatedDate, discovered id (9) not greater than committed (10): must not commit backwards.
        var fixture = new Fixture { InitialCommittedId = "10", DiscoveredId = 9, DiscoveredSameDate = true };
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Empty(fixture.CommittedIds);
        Assert.Equal(1, fixture.RunsCompleted);
    }

    [Fact]
    public async Task RejectedCheckpointFailsTheRunAuditAndReportsZombie()
    {
        var fixture = new Fixture { RejectCommit = true };
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Equal(1, fixture.RunsFailed);
        Assert.Equal(0, fixture.RunsCompleted);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task RejectedFinalCompletionFailsTheRunAuditAndReportsZombie()
    {
        var fixture = new Fixture { RejectFinalCompletion = true };
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Equal(1, fixture.RunsFailed);
        Assert.Equal(0, fixture.RunsCompleted);
    }

    [Fact]
    public async Task LeaseLossDuringPublicationFailsTheRunAuditAsUnfinalized()
    {
        var fixture = new Fixture { LoseLeaseDuringPublication = true, DiscoveredId = 1 };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), timeout.Token);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        // The runner never reached an explicit Complete/Fail path; the outer cleanup writes the audit.
        Assert.Equal(1, fixture.RunsFailed);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task RefreshCompletesTheRunAuditWithoutAdvancingTheCursor()
    {
        var fixture = new Fixture();
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Refresh), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Empty(fixture.CommittedIds);
        Assert.Equal(1, fixture.RunsCompleted);
    }

    [Fact]
    public async Task RefreshWithRejectedFinalCompletionFailsInsteadOfReportingSuccess()
    {
        var fixture = new Fixture { RejectFinalCompletion = true };
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Refresh), CancellationToken.None);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Equal(1, fixture.RunsFailed);
        Assert.Equal(0, fixture.RunsCompleted);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task NoDataIncrementalRunCompletesWithFromEqualToNoCursorAdvance()
    {
        // No changed events and the single planned window does not extend past the committed
        // cursor (high watermark == committed date): the run must still complete the audit row
        // (from==to) without ever calling TryCommitAsync.
        var fixture = new Fixture { HighWatermark = Start };
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(0, result.EventsWritten);
        Assert.Empty(fixture.CommittedIds);
        Assert.Equal(1, fixture.RunsCompleted);
        Assert.Equal(0, fixture.RunsFailed);
    }

    [Fact]
    public async Task PartialFailureAfterACheckpointAuditsTheLatestCommittedProgressNotTheOriginalLease()
    {
        // Two windows: the first commits a real checkpoint, the second's publish hangs until the
        // renewal loop reports the lease lost. The outer cleanup's Fail audit must report the
        // checkpoint actually committed in window 1 — not null and not the pre-run lease snapshot.
        var fixture = new Fixture
        {
            InitialCommittedId = "0",
            MaxCatchUpWindowSize = TimeSpan.FromHours(12),
            HighWatermark = Start.AddHours(24),
            TwoPhaseLeaseLoss = true,
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = await fixture.Runner.RunAsync(new("farm", "Curtailment", IngestionMode.Incremental), timeout.Token);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Equal(1, fixture.RunsFailed);
        Assert.Equal(0, fixture.RunsCompleted);
        Assert.Equal("1", fixture.CommittedIds.Single());
        Assert.Equal(Start.AddHours(1), fixture.LastFailPartialUpdatedAt);
        Assert.Equal("1", fixture.LastFailPartialCommittedId);
    }

    private sealed class Fixture : IExtractionCursorRepository, ICurtailmentSourceAdapter, IRawObjectStore, IRawArtifactPublisher
    {
        private readonly Dictionary<string, byte[]> _objects = new();
        private readonly TaskCompletionSource<bool> _renewalResult = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _discoverCalls;
        private int _publishCalls;

        public string InitialCommittedId { get; init; } = "0";
        public long? DiscoveredId { get; init; }
        public bool DiscoveredSameDate { get; init; }
        public bool RejectCommit { get; init; }
        public bool RejectFinalCompletion { get; init; }
        public bool LoseLeaseDuringPublication { get; init; }
        public bool TwoPhaseLeaseLoss { get; init; }
        public DateTime? HighWatermark { get; init; }
        public TimeSpan? MaxCatchUpWindowSize { get; init; }

        public string GeneratedOperationId { get; } = Guid.NewGuid().ToString();
        public string? ReceivedOperationId { get; private set; }
        public List<RawCurtailmentBatchAvailableV1> Published { get; } = new();
        public bool RunStarted { get; private set; }
        public int RunsCompleted { get; private set; }
        public int RunsFailed { get; private set; }
        public List<string> CommittedIds { get; } = new();
        public bool Released { get; private set; }
        public DateTime? LastFailPartialUpdatedAt { get; private set; }
        public string? LastFailPartialCommittedId { get; private set; }

        public CurtailmentIngestionRunner Runner => new(this, this,
            new CurtailmentBatchWriter(this), this, new CatchUpWindowPlanner(), new ChangeDiscoveryReader(this),
            new NullIngestionTelemetry(),
            new IngestionOptions { MaxCatchUpWindowSize = MaxCatchUpWindowSize ?? TimeSpan.FromDays(30) },
            "test-owner");

        public Task<ExtractionLeaseModel?> GetCurrentAsync(string windfarmId, string sourceType, CancellationToken cancellationToken)
            => Task.FromResult<ExtractionLeaseModel?>(new ExtractionLeaseModel
            {
                WindfarmId = windfarmId, SourceType = sourceType, LastUpdatedDate = Start,
                LastCommittedId = InitialCommittedId, LeaseToken = 1, LeaseExpiresAt = Start.AddYears(1)
            });

        public Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(string windfarmId, string sourceType, string owner,
            TimeSpan leaseDuration, CancellationToken cancellationToken) => GetCurrentAsync(windfarmId, sourceType, cancellationToken);

        public Task<bool> TryRenewLeaseAsync(string windfarmId, string sourceType, string owner, long leaseToken,
            TimeSpan leaseDuration, CancellationToken cancellationToken)
            => LoseLeaseDuringPublication || TwoPhaseLeaseLoss ? _renewalResult.Task.WaitAsync(cancellationToken) : Task.FromResult(true);

        public Task<bool> TryCommitAsync(string windfarmId, string sourceType, long leaseToken, DateTime newLastUpdatedDate,
            string newLastCommittedId, CancellationToken cancellationToken, DateTime? expectedLastUpdatedDate = null)
        {
            if (RejectCommit) return Task.FromResult(false);
            CommittedIds.Add(newLastCommittedId);
            return Task.FromResult(true);
        }

        public Task<ExtractionRunModel> StartRunAsync(string windfarmId, string sourceType, long leaseToken,
            DateTime fromUpdatedAt, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null)
        {
            RunStarted = true;
            ReceivedOperationId = operationId;
            return Task.FromResult(new ExtractionRunModel(1, windfarmId, sourceType, ExtractionRunStatus.Running,
                Start, null, fromUpdatedAt, fromCommittedId, null, null, outputPath, null, operationId ?? GeneratedOperationId));
        }

        public Task<bool> TryCompleteRunAsync(long runId, string windfarmId, string sourceType, long leaseToken,
            DateTime newLastUpdatedDate, string newLastCommittedId, CancellationToken cancellationToken, DateTime? expectedLastUpdatedDate = null)
        {
            if (RejectFinalCompletion) return Task.FromResult(false);
            RunsCompleted++;
            return Task.FromResult(true);
        }

        public Task FailRunAsync(long runId, string windfarmId, string sourceType, long leaseToken,
            DateTime? partialUpdatedAt, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken)
        {
            RunsFailed++;
            LastFailPartialUpdatedAt = partialUpdatedAt;
            LastFailPartialCommittedId = partialCommittedId;
            return Task.CompletedTask;
        }

        public Task ReleaseLeaseAsync(string windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken)
        {
            Released = true;
            return Task.CompletedTask;
        }

        public Task<DateTime> GetHighWatermarkAsync(string windfarmId, CancellationToken cancellationToken)
            => Task.FromResult(HighWatermark ?? Start.AddDays(1));

        public async IAsyncEnumerable<CurtailmentEventModel> GetChangedEventsAsync(
            string windfarmId, DateTime low, DateTime end, (DateTime LastUpdatedDate, long Id)? continuationKey,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;

            if (TwoPhaseLeaseLoss)
            {
                _discoverCalls++;
                var (id, lastUpdated) = _discoverCalls == 1 ? (1L, Start.AddHours(1)) : (2L, Start.AddHours(13));
                yield return new CurtailmentEventModel(
                    $"evt-{id}", windfarmId, "item-1", "Curtailment", Start, null, "Active",
                    "type-1", "rev-1", Start, false, lastUpdated, id);
                yield break;
            }

            if (DiscoveredId is { } discoveredId)
            {
                var lastUpdated = DiscoveredSameDate ? Start : Start.AddHours(1);
                yield return new CurtailmentEventModel(
                    $"evt-{discoveredId}", windfarmId, "item-1", "Curtailment", Start, null, "Active",
                    "type-1", "rev-1", Start, false, lastUpdated, discoveredId);
            }
        }

        public async IAsyncEnumerable<CurtailmentEventModel> GetAllCurrentEventsAsync(
            string windfarmId, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return new CurtailmentEventModel(
                "evt-refresh", windfarmId, "item-1", "Curtailment", Start, null, "Active",
                "type-1", "rev-1", Start, false, Start, 1);
            await Task.CompletedTask;
        }

        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            _objects[path] = content.ToArray();
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken)
            => Task.FromResult(_objects.ContainsKey(path));

        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken)
            => Task.FromResult(_objects[path]);

        public async Task PublishAsync(RawCurtailmentBatchAvailableV1 message, CancellationToken cancellationToken)
        {
            Published.Add(message);
            if (TwoPhaseLeaseLoss)
            {
                _publishCalls++;
                if (_publishCalls >= 2)
                {
                    _renewalResult.TrySetResult(false);
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                return;
            }

            if (LoseLeaseDuringPublication)
            {
                _renewalResult.TrySetResult(false);
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
        }
    }
}
