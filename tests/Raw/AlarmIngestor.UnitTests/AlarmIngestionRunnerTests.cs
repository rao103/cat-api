using AlarmIngestor.Configuration;
using AlarmIngestor.Contracts;
using AlarmIngestor.Enums;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;
using AlarmIngestor.Operations.Discovery;
using AlarmIngestor.Operations.Execution;
using AlarmIngestor.Operations.Polling;
using AlarmIngestor.Raw;
using AlarmIngestor.Serialization;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="AlarmIngestionRunner"/> ordering/durability behavior: publish precedes cursor/open
/// enrollment, known-open IDs are polled every run even with nothing newly discovered, missing
/// polled IDs stay tracked, resolved IDs are removed only after their closure batch publishes,
/// and Historical never advances the discovery cursor.
/// </summary>
public sealed class AlarmIngestionRunnerTests
{
    private static readonly DateTime MinRetrievalDate = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly TurbineIdentity Turbine = new("RDSPP-1", "ITEM-1");

    [Theory]
    [InlineData(IngestionMode.Incremental)]
    [InlineData(IngestionMode.Historical)]
    public async Task TriggeredRunPublishesSuppliedOperationId(IngestionMode mode)
    {
        const string operationId = "bb2e7ced-3d1c-43f9-9535-de6217640b76";
        var fixture = new Fixture
        {
            DiscoveryPages = [[MakeEvent(1)]],
            HistoricalEvents = [MakeEvent(1)],
        };
        var request = new IngestionRequestModel("farm", "Alarm", mode,
            MinRetrievalDate, MinRetrievalDate.AddDays(1), "refresh-run")
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
        var fixture = new Fixture { DiscoveryPages = [[MakeEvent(1)]] };

        var result = await fixture.Runner.RunAsync(
            new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Null(fixture.ReceivedOperationId);
        Assert.Equal(fixture.GeneratedOperationId, Assert.Single(fixture.Published).OperationId);
    }

    [Fact]
    public async Task InvalidOperationIdIsRejectedBeforeRunStarts()
    {
        var fixture = new Fixture();
        var request = new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental) { OperationId = " " };

        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Runner.RunAsync(request, CancellationToken.None));

        Assert.Null(fixture.ReceivedOperationId);
        Assert.Empty(fixture.Published);
    }

    private static WindfarmAlarmOptions Windfarm() => new()
    {
        WindfarmId = "farm",
        MinRetrievalDateUtc = MinRetrievalDate,
        AlarmCodes = ["100"],
        Turbines = [Turbine],
    };

    private static AlarmEventModel MakeEvent(long alarmId, DateTime? end = null, bool tombstone = false) => new(
        WindfarmId: "farm", TurbineRdspp: "RDSPP-1", ItemId: "ITEM-1", AlarmId: alarmId,
        AlarmCode: "100", AlarmText: "text", StartTimeUtc: MinRetrievalDate.AddHours(1), EndTimeUtc: end,
        AlarmType: "Alarm", AlarmState: tombstone ? "Deleted" : "Good", IsTombstone: tombstone,
        ObservedAtUtc: MinRetrievalDate.AddHours(2), SequenceInRun: 0);

    [Fact]
    public async Task NewlyDiscoveredOpenAlarmIsPublishedThenEnrolledForPolling()
    {
        var fixture = new Fixture { DiscoveryPages = [[MakeEvent(1)]] };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Single(fixture.Published);
        Assert.Equal("Discovery", fixture.Published[0].BatchKind);
        Assert.Single(fixture.NewOpenEnrollments);
        Assert.Equal(1L, fixture.NewOpenEnrollments[0].AlarmId);
        Assert.Equal(1L, fixture.CursorAdvances["farm"]);
    }

    [Fact]
    public async Task KnownOpenIdsArePolledEveryRunEvenWithNoNewDiscovery()
    {
        var fixture = new Fixture
        {
            DiscoveryPages = [[]],
            KnownOpen = { ["RDSPP-1"] = [7L] },
            PollResults = [MakeEvent(7)],
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Single(fixture.Published);
        Assert.Equal("Polling", fixture.Published[0].BatchKind);
    }

    [Fact]
    public async Task StillOpenPolledAlarmRemainsTrackedAndIsNotRemoved()
    {
        var fixture = new Fixture
        {
            DiscoveryPages = [[]],
            KnownOpen = { ["RDSPP-1"] = [7L] },
            PollResults = [MakeEvent(7)],
        };

        await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Empty(fixture.ResolvedRemovals);
    }

    [Fact]
    public async Task ClosedPolledAlarmIsPublishedThenRemovedFromTracking()
    {
        var fixture = new Fixture
        {
            DiscoveryPages = [[]],
            KnownOpen = { ["RDSPP-1"] = [7L] },
            PollResults = [MakeEvent(7, end: MinRetrievalDate.AddHours(3))],
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Single(fixture.ResolvedRemovals);
        Assert.Equal(7L, fixture.ResolvedRemovals[0].AlarmId);
    }

    [Fact]
    public async Task MissingPolledIdRemainsTrackedWithoutInventingClosure()
    {
        // The source returns nothing for the requested id; the poller/runner must not
        // synthesize a resolution for an id absent from the response.
        var fixture = new Fixture
        {
            DiscoveryPages = [[]],
            KnownOpen = { ["RDSPP-1"] = [7L] },
            PollResults = [],
        };

        await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Empty(fixture.ResolvedRemovals);
        Assert.Empty(fixture.Published);
    }

    [Fact]
    public async Task PublishFailureOnDiscoveryBatchLeavesCursorUncommitted()
    {
        var fixture = new Fixture { DiscoveryPages = [[MakeEvent(1)]], FailPublish = true };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.CursorAdvances);
        Assert.Empty(fixture.NewOpenEnrollments);
    }

    [Fact]
    public async Task HistoricalModeNeverAdvancesDiscoveryCursor()
    {
        // Seed a pre-existing NON-ZERO checkpoint: a fixture that always starts at 0 can't
        // distinguish "never advances" from "coincidentally completes at the value it started at".
        var fixture = new Fixture { HistoricalEvents = [MakeEvent(5)], InitialCommittedId = 42 };

        var request = new IngestionRequestModel(
            "farm", "Alarm", IngestionMode.Historical, MinRetrievalDate, MinRetrievalDate.AddDays(1));
        var result = await fixture.Runner.RunAsync(request, CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Single(fixture.Published);
        Assert.Equal("Historical", fixture.Published[0].BatchKind);
        Assert.Empty(fixture.CursorAdvances);
        Assert.Equal(42L, fixture.CurrentCommittedId);
        Assert.Contains(fixture.CompletedAudits, a => a.WindfarmId == "farm" && a.From == 42 && a.To == 42);
    }

    [Fact]
    public async Task HistoricalRepeatedFullPageFailsClosedAndNeverFetchesAThirdPage()
    {
        // The server ignores offset and returns the same full page twice: the offset loop alone
        // does not prove progress, so the runner must fail after detecting the repeat and must
        // never request a third page (Section "historical offset is not safe solely because
        // offset increases").
        var fixture = new Fixture
        {
            HistoricalPages = [[MakeEvent(1), MakeEvent(2)], [MakeEvent(1), MakeEvent(2)], [MakeEvent(3)]],
            SourceQueryPageSize = 2,
        };

        var request = new IngestionRequestModel(
            "farm", "Alarm", IngestionMode.Historical, MinRetrievalDate, MinRetrievalDate.AddDays(1));
        var result = await fixture.Runner.RunAsync(request, CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal(2, fixture.HistoricalPageCallCount);
    }

    [Fact]
    public async Task HistoricalUnorderedPageAcrossPagesFailsClosed()
    {
        var fixture = new Fixture
        {
            HistoricalPages = [[MakeEvent(3), MakeEvent(5)], [MakeEvent(4)]],
            SourceQueryPageSize = 2,
        };

        var request = new IngestionRequestModel(
            "farm", "Alarm", IngestionMode.Historical, MinRetrievalDate, MinRetrievalDate.AddDays(1));
        var result = await fixture.Runner.RunAsync(request, CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
    }

    [Fact]
    public async Task MixedInvalidLowerIdValidHigherIdPageFailsClosedWithNoCheckpointOrPublication()
    {
        // AlarmId 1 (invalid) is lower than AlarmId 2 (valid) in the same page: the batch must
        // fail closed entirely rather than publish/checkpoint the valid id 2 while silently
        // skipping the still-unresolved invalid id 1.
        var quarantined = new AlarmEventModel(
            "farm", "RDSPP-1", "ITEM-1", 1, "100", "text",
            StartTimeUtc: MinRetrievalDate.AddHours(2), EndTimeUtc: MinRetrievalDate.AddHours(1),
            AlarmType: "Alarm", AlarmState: "Good", IsTombstone: false, ObservedAtUtc: MinRetrievalDate.AddHours(3), SequenceInRun: 0);
        var fixture = new Fixture { DiscoveryPages = [[quarantined, MakeEvent(2)]] };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.Published);
        Assert.Empty(fixture.CursorAdvances);
        Assert.Empty(fixture.NewOpenEnrollments);
    }

    [Fact]
    public async Task AllInvalidPageFailsClosedAndLaterValidPageIsNeverFetched()
    {
        // First discovery page is entirely invalid; the runner must fail the run before ever
        // requesting the second (valid) page, so nothing later silently continues past it.
        var quarantined = new AlarmEventModel(
            "farm", "RDSPP-1", "ITEM-1", 1, "100", "text",
            StartTimeUtc: MinRetrievalDate.AddHours(2), EndTimeUtc: MinRetrievalDate.AddHours(1),
            AlarmType: "Alarm", AlarmState: "Good", IsTombstone: false, ObservedAtUtc: MinRetrievalDate.AddHours(3), SequenceInRun: 0);
        var fixture = new Fixture
        {
            DiscoveryPages = [[quarantined], [MakeEvent(2)]],
            SourceQueryPageSize = 1,
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.Published);
        Assert.Empty(fixture.CursorAdvances);
        Assert.Empty(fixture.NewOpenEnrollments);
        Assert.Equal(1, fixture.DiscoveryPageCallCount);
    }

    [Fact]
    public async Task QuarantinedDiscoveryEventFailsTheWholeRunClosedWithNoCheckpointOrEnrollment()
    {
        // AlarmId 2 has EndTimeUtc before StartTimeUtc: the whole batch — including the
        // otherwise-valid AlarmId 1 in the same page — must fail closed with no cursor
        // advance and no open-alarm enrollment.
        var quarantined = new AlarmEventModel(
            "farm", "RDSPP-1", "ITEM-1", 2, "100", "text",
            StartTimeUtc: MinRetrievalDate.AddHours(2), EndTimeUtc: MinRetrievalDate.AddHours(1),
            AlarmType: "Alarm", AlarmState: "Good", IsTombstone: false, ObservedAtUtc: MinRetrievalDate.AddHours(3), SequenceInRun: 0);
        var fixture = new Fixture { DiscoveryPages = [[MakeEvent(1), quarantined]] };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.Published);
        Assert.Empty(fixture.CursorAdvances);
        Assert.Empty(fixture.NewOpenEnrollments);
    }

    [Fact]
    public async Task QuarantinedClosureFailsClosedAndKnownOpenIdIsNeverRemoved()
    {
        // The poll response for the known-open id 7 is malformed (end precedes start): the batch
        // must fail closed and the known-open entry must remain tracked, not removed, because
        // the closure was never durably published.
        var malformedClosure = new AlarmEventModel(
            "farm", "RDSPP-1", "ITEM-1", 7, "100", "text",
            StartTimeUtc: MinRetrievalDate.AddHours(2), EndTimeUtc: MinRetrievalDate.AddHours(1),
            AlarmType: "Alarm", AlarmState: "Good", IsTombstone: false, ObservedAtUtc: MinRetrievalDate.AddHours(3), SequenceInRun: 0);
        var fixture = new Fixture
        {
            DiscoveryPages = [[]],
            KnownOpen = { ["RDSPP-1"] = [7L] },
            PollResults = [malformedClosure],
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.ResolvedRemovals);
        Assert.Empty(fixture.Published);
    }

    [Fact]
    public async Task EachDiscoveryPageCommitsBeforeTheNextPageIsFetched()
    {
        // Two bounded pages for the same turbine; the fake commit records the call order
        // relative to the next page fetch so we can assert page-scoped publish-then-commit
        // happens before the following page is requested (bounded streaming, not full
        // accumulation).
        var fixture = new Fixture
        {
            DiscoveryPages = [[MakeEvent(1)], [MakeEvent(2)]],
            SourceQueryPageSize = 1,
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(2, fixture.Published.Count);
        Assert.Equal([0, 1], fixture.Published.Select(p => p.WindowSequence));
        Assert.Equal(["CommitBeforeFetch:1", "CommitBeforeFetch:2"], fixture.CommitOrderMarkers);
    }

    [Fact]
    public async Task MultiTurbineTransientFailureOnRetryDoesNotAdvanceGlobalCheckpointUntilAllTurbinesSucceed()
    {
        // Turbine A (higher max) succeeds; turbine B (lower max) fails once. The global
        // checkpoint must stay unchanged after the failed attempt, and only advance to the
        // single frozen MAX cutoff once a retry lets B succeed too.
        var turbineA = new TurbineIdentity("RDSPP-A", "ITEM-A");
        var turbineB = new TurbineIdentity("RDSPP-B", "ITEM-B");
        var fixture = new Fixture
        {
            Turbines = [turbineA, turbineB],
            MaxAlarmIdByTurbine = { ["RDSPP-A"] = 10, ["RDSPP-B"] = 3 },
            DiscoveryPagesByTurbine =
            {
                ["RDSPP-A"] = [[MakeEvent(10)]],
                ["RDSPP-B"] = [[MakeEvent(3)]],
            },
            FailDiscoveryOnCallNumber = { ["RDSPP-B"] = 0 },
        };
        var request = new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental);

        var firstAttempt = await fixture.Runner.RunAsync(request, CancellationToken.None);
        Assert.Equal(RunOutcome.Failed, firstAttempt.Outcome);
        Assert.Empty(fixture.CursorAdvances);

        fixture.ResetDiscoveryCallIndices();
        var secondAttempt = await fixture.Runner.RunAsync(request, CancellationToken.None);

        Assert.Equal(RunOutcome.Success, secondAttempt.Outcome);
        Assert.Equal(10L, fixture.CursorAdvances["farm"]);
    }

    [Fact]
    public async Task LeaseRenewalHasABoundedDeadlineAndDoesNotWaitForever()
    {
        // A renewal call that never completes must not be awaited indefinitely: it is bounded
        // to a fraction of the lease duration and the run must still terminate with Zombie
        // rather than hang.
        var fixture = new Fixture
        {
            DiscoveryPages = [[MakeEvent(1)]],
            RenewalNeverCompletes = true,
            LeaseDuration = TimeSpan.FromMilliseconds(60),
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel("farm", "Alarm", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
    }

    private sealed class Fixture : IExtractionCursorRepository, IAlarmSourceAdapter, IRawObjectStore, IRawArtifactPublisher
    {
        private readonly Dictionary<string, byte[]> _objects = new();
        private readonly Dictionary<string, int> _perTurbineCallIndex = new();
        private readonly HashSet<string> _failedTurbinesOnce = new();
        private int _historicalPageCallIndex;
        private long _nextRunId = 1;

        public List<TurbineIdentity> Turbines { get; init; } = [Turbine];
        public List<IReadOnlyList<AlarmEventModel>> DiscoveryPages { get; init; } = [];
        public Dictionary<string, List<IReadOnlyList<AlarmEventModel>>> DiscoveryPagesByTurbine { get; init; } = new();
        public Dictionary<string, long?> MaxAlarmIdByTurbine { get; init; } = new();
        public Dictionary<string, int> FailDiscoveryOnCallNumber { get; init; } = new();
        public IReadOnlyList<AlarmEventModel> PollResults { get; init; } = [];
        public IReadOnlyList<AlarmEventModel> HistoricalEvents { get; init; } = [];
        public List<IReadOnlyList<AlarmEventModel>>? HistoricalPages { get; init; }
        public int HistoricalPageCallCount => _historicalPageCallIndex;
        public Dictionary<string, IReadOnlyList<long>> KnownOpen { get; } = new();
        public bool FailPublish { get; init; }
        public bool FailComplete { get; init; }

        public string GeneratedOperationId { get; } = Guid.NewGuid().ToString();
        public string? ReceivedOperationId { get; private set; }
        public List<RawAlarmBatchAvailableV1> Published { get; } = [];
        public Dictionary<string, long> CursorAdvances { get; } = new();
        public long InitialCommittedId { get; init; } = 0;
        public long CurrentCommittedId => _currentCommittedId ?? InitialCommittedId;
        public List<(string WindfarmId, long From, long To)> CompletedAudits { get; } = new();
        private long? _currentCommittedId;
        public List<(string TurbineRdspp, long AlarmId)> NewOpenEnrollments { get; private set; } = [];
        public List<(string TurbineRdspp, long AlarmId)> ResolvedRemovals { get; private set; } = [];
        public List<string> CommitOrderMarkers { get; } = [];
        public int DiscoveryPageCallCount => _perTurbineCallIndex.TryGetValue(Turbine.TurbineRdspp, out var n) ? n : 0;
        public bool RenewalNeverCompletes { get; init; }
        public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(5);
        public int SourceQueryPageSize { get; init; } = 3000;

        public void ResetDiscoveryCallIndices() => _perTurbineCallIndex.Clear();

        public AlarmIngestionRunner Runner => new(
            this, this,
            new AlarmDiscoveryReader(this),
            new KnownOpenAlarmPoller(this, 500),
            new AlarmBatchWriter(this),
            this,
            new Monitoring.NullIngestionTelemetry(),
            new IngestionOptions { Windfarms = [WindfarmFor()], LeaseDuration = LeaseDuration, SourceQueryPageSize = SourceQueryPageSize },
            "test-owner");

        private WindfarmAlarmOptions WindfarmFor() => new()
        {
            WindfarmId = "farm",
            MinRetrievalDateUtc = MinRetrievalDate,
            AlarmCodes = ["100"],
            Turbines = Turbines,
        };

        public Task<ExtractionLeaseModel?> GetCurrentAsync(string windfarmId, string sourceType, CancellationToken cancellationToken)
            => Task.FromResult<ExtractionLeaseModel?>(null);

        public Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(string windfarmId, string sourceType, string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
            => Task.FromResult<ExtractionLeaseModel?>(new ExtractionLeaseModel(windfarmId, sourceType, 1, CurrentCommittedId.ToString(), DateTime.UtcNow.AddMinutes(5)));

        public async Task<bool> TryRenewLeaseAsync(string windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            if (RenewalNeverCompletes)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }
            return true;
        }

        public Task ReleaseLeaseAsync(string windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<ExtractionRunModel> StartRunAsync(string windfarmId, string sourceType, long leaseToken, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null)
        {
            ReceivedOperationId = operationId;
            return Task.FromResult(new ExtractionRunModel(_nextRunId++, windfarmId, sourceType, ExtractionRunStatus.Running,
                DateTime.UtcNow, null, fromCommittedId, null, outputPath, null, operationId ?? GeneratedOperationId));
        }

        public Task<bool> TryCompleteRunAsync(long runId, string windfarmId, string sourceType, long leaseToken, string newCommittedId, CancellationToken cancellationToken)
        {
            if (FailComplete)
            {
                return Task.FromResult(false);
            }
            var parsed = long.Parse(newCommittedId);
            CompletedAudits.Add((windfarmId, CurrentCommittedId, parsed));
            // Only a genuine change in the committed id counts as an advance — completing at the
            // same value (Historical's "no-data completes"/never-advances semantics) must not.
            if (parsed != CurrentCommittedId)
            {
                CursorAdvances[windfarmId] = parsed;
            }
            _currentCommittedId = parsed;
            return Task.FromResult(true);
        }

        public Task FailRunAsync(long runId, string windfarmId, string sourceType, long leaseToken, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<IReadOnlyDictionary<string, IReadOnlyList<long>>> GetKnownOpenAlarmsAsync(string windfarmId, IReadOnlyList<string> turbineRdspps, int maxIdsPerTurbine, CancellationToken cancellationToken)
            => Task.FromResult<IReadOnlyDictionary<string, IReadOnlyList<long>>>(
                turbineRdspps.ToDictionary(t => t, t => KnownOpen.TryGetValue(t, out var ids) ? ids : (IReadOnlyList<long>)[]));

        public Task<bool> TryEnrollOpenAlarmsAsync(
            string windfarmId, long leaseToken, IReadOnlyList<(string TurbineRdspp, long AlarmId)> newOpenAlarms, CancellationToken cancellationToken)
        {
            NewOpenEnrollments.AddRange(newOpenAlarms);
            if (newOpenAlarms.Count > 0)
            {
                CommitOrderMarkers.Add($"CommitBeforeFetch:{newOpenAlarms[0].AlarmId}");
            }
            return Task.FromResult(true);
        }

        public Task<bool> TryRemoveResolvedOpenAlarmsAsync(
            string windfarmId, long leaseToken,
            IReadOnlyList<(string TurbineRdspp, long AlarmId)> resolvedAlarmIds,
            CancellationToken cancellationToken)
        {
            ResolvedRemovals = resolvedAlarmIds.ToList();
            return Task.FromResult(true);
        }

        private List<IReadOnlyList<AlarmEventModel>> PagesFor(string turbineRdspp)
            => DiscoveryPagesByTurbine.TryGetValue(turbineRdspp, out var pages)
                ? pages
                : turbineRdspp == Turbine.TurbineRdspp ? DiscoveryPages : [];

        public Task<long?> GetMaxAlarmIdAsync(string turbineRdspp, DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, CancellationToken cancellationToken)
        {
            if (MaxAlarmIdByTurbine.TryGetValue(turbineRdspp, out var overrideMax))
            {
                return Task.FromResult(overrideMax);
            }
            var all = PagesFor(turbineRdspp).SelectMany(p => p).ToList();
            return Task.FromResult<long?>(all.Count == 0 ? null : all.Max(e => e.AlarmId));
        }

        public async Task<IReadOnlyList<AlarmEventModel>> DiscoverPageAsync(
            string windfarmId, string turbineRdspp, string itemId, long afterAlarmId, long upperBoundAlarmId,
            DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, int pageSize, CancellationToken cancellationToken)
        {
            if (RenewalNeverCompletes)
            {
                // Gives the bounded renewal deadline time to fire and cancel processing before
                // this instantly-resolving fake would otherwise let the run finish first.
                await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
            }

            var callNumber = _perTurbineCallIndex.TryGetValue(turbineRdspp, out var n) ? n : 0;
            _perTurbineCallIndex[turbineRdspp] = callNumber + 1;

            if (FailDiscoveryOnCallNumber.TryGetValue(turbineRdspp, out var failAtCall)
                && callNumber == failAtCall && _failedTurbinesOnce.Add(turbineRdspp))
            {
                throw new InvalidOperationException($"Simulated transient discovery failure for turbine {turbineRdspp}.");
            }

            var pages = PagesFor(turbineRdspp);
            return callNumber < pages.Count ? pages[callNumber] : [];
        }

        public Task<IReadOnlyList<AlarmEventModel>> PollKnownAlarmsAsync(
            string windfarmId, string turbineRdspp, string itemId, IReadOnlyList<long> alarmIds, CancellationToken cancellationToken)
            => Task.FromResult(PollResults);

        public Task<IReadOnlyList<AlarmEventModel>> GetAlarmsBetweenDatesAsync(
            string windfarmId, string turbineRdspp, string itemId, DateTime startUtc, DateTime endUtc,
            IReadOnlyList<string> alarmCodes, int pageSize, int offset, CancellationToken cancellationToken)
        {
            if (HistoricalPages is not null)
            {
                var index = _historicalPageCallIndex++;
                return Task.FromResult(index < HistoricalPages.Count ? HistoricalPages[index] : (IReadOnlyList<AlarmEventModel>)[]);
            }

            return Task.FromResult(offset == 0 ? HistoricalEvents : (IReadOnlyList<AlarmEventModel>)[]);
        }

        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            _objects[path] = content.ToArray();
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken) => Task.FromResult(_objects.ContainsKey(path));

        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken) => Task.FromResult(_objects[path]);

        public Task PublishAsync(RawAlarmBatchAvailableV1 message, CancellationToken cancellationToken)
        {
            if (FailPublish)
            {
                throw new InvalidOperationException("Simulated publish failure.");
            }
            Published.Add(message);
            return Task.CompletedTask;
        }
    }
}
