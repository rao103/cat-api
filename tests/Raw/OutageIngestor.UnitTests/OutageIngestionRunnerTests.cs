using CAT.Messaging.Contracts;
using OutageIngestor.Configuration;
using OutageIngestor.Contracts;
using OutageIngestor.Enums;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Monitoring;
using OutageIngestor.Operations.Discovery;
using OutageIngestor.Operations.Execution;
using OutageIngestor.Raw;
using OutageIngestor.Serialization;

namespace OutageIngestor.UnitTests;

/// <summary>
/// <see cref="OutageIngestionRunner"/> ordering/idempotency behavior: the live/changed and
/// deletion feeds are discovered, written and published independently under their own
/// checkpoints, but neither checkpoint pair is ever committed to the database until BOTH feeds
/// finish successfully (Incremental Refresh Spec, Sections 5.2 and 10) — one fenced commit per
/// run, never a per-window commit. A Refresh pass never advances the incremental cursor even
/// when it publishes successfully.
/// </summary>
public sealed class OutageIngestionRunnerTests
{
    private static readonly DateTime Start = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(false, IngestionMode.Incremental)]
    [InlineData(true, IngestionMode.Incremental)]
    [InlineData(false, IngestionMode.Refresh)]
    [InlineData(true, IngestionMode.Refresh)]
    public async Task ResolvesTurbinesOnceAndPropagatesFrozenScopeToEveryRead(bool empty, IngestionMode mode)
    {
        var names = empty ? Array.Empty<string>() : new[] { "wtg-1", "wtg-2'; --" };
        var expected = names.ToArray();
        var fixture = new Fixture
        {
            ActiveTurbineNames = names,
            MutateScopeAfterResolution = true,
            LatestSourceUpdatedDate = Start.AddHours(2),
            MaxWindowSize = TimeSpan.FromHours(1),
            IncludeDeletedEventPerWindow = true,
            RefreshEvents = [MakeEvent("OC-refresh")]
        };

        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(mode), default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(1, fixture.ScopeReadCount);
        Assert.Equal(mode == IngestionMode.Refresh ? 1 : 4, fixture.TurbineScopes.Count);
        Assert.All(fixture.TurbineScopes, call => Assert.Equal(expected, call.Names));
        Assert.Equal(Fixture.RetentionFloor, fixture.ObservedRetentionFloor);
        if (mode == IngestionMode.Incremental)
            Assert.Equal(new[] { "live", "live", "deleted", "deleted" }, fixture.TurbineScopes.Select(call => call.Feed));
        else
        {
            Assert.Equal("refresh", Assert.Single(fixture.TurbineScopes).Feed);
            Assert.Equal("Refresh", Assert.Single(fixture.Published).ExtractionMode);
            Assert.Equal(Start, result.CompletedCheckpointUpdatedUtc);
        }
        if (empty)
        {
            Assert.Equal(0, result.EventsWritten);
            Assert.DoesNotContain(fixture.ObjectPaths, path => path.EndsWith(".parquet"));
        }
    }

    [Theory]
    [InlineData(false, IngestionMode.Incremental)]
    [InlineData(true, IngestionMode.Incremental)]
    [InlineData(false, IngestionMode.Refresh)]
    [InlineData(true, IngestionMode.Refresh)]
    public async Task MissingScopeOrRepositoryFailsBeforeSourceAndPublication(bool missingRepository, IngestionMode mode)
    {
        var fixture = new Fixture { OmitScopeRepository = missingRepository, MissingScope = !missingRepository };
        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(mode), default);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.SourceCalls);
        Assert.Empty(fixture.ObjectPaths);
        Assert.Empty(fixture.Published);
        Assert.Empty(fixture.Commits);
        Assert.Null(Assert.Single(fixture.Finalizations).Result.CompletedCheckpointUpdatedUtc);
    }

    [Theory]
    [InlineData(false, IngestionMode.Incremental)]
    [InlineData(true, IngestionMode.Incremental)]
    [InlineData(false, IngestionMode.Refresh)]
    [InlineData(true, IngestionMode.Refresh)]
    public async Task LogsPlannedFeedPathsAndEveryDurableParquetPath(bool claimed, IngestionMode mode)
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1),
            IncludeDeletedEventPerWindow = true,
            RefreshEvents = [MakeEvent("OC-first"), MakeEvent("OC-second")],
            MaxEventsPerShard = 1
        };
        var result = claimed
            ? await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(mode), default)
            : await fixture.Runner.RunAsync(new(42, "incorrect-request", "Outage", mode, "run-1", Start, Start.AddDays(1)), default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        var messages = fixture.Logger.Entries.Where(entry => entry.ContainsKey("Message"))
            .Select(entry => Assert.IsType<string>(entry["Message"])).ToArray();
        var modeName = mode == IngestionMode.Refresh ? "refresh" : "incremental";
        var expectedPrefix = $"/raw/outage/windfarm=CHW01/mode={modeName}/extraction={fixture.LeaseToken}/";
        Assert.All(fixture.ObjectPaths, path => Assert.StartsWith(expectedPrefix, path));
        if (!claimed)
            Assert.Equal(expectedPrefix, fixture.OutputPath);
        Assert.All(fixture.Published, message => Assert.StartsWith(expectedPrefix, message.ManifestPath ?? message.BlobPath!));
        var files = fixture.ObjectPaths.Where(path => path.EndsWith(".parquet")).ToArray();
        Assert.Equal(2, files.Length);
        foreach (var path in files)
            Assert.Single(messages, message => message.StartsWith("Parquet file written and verified durable") && message.EndsWith(path));
        if (mode == IngestionMode.Incremental)
        {
            Assert.Contains(messages, message => message.Contains("Planned first Parquet part") && message.Contains("payload OutageEvents:")
                && message.Contains("/events/") && message.EndsWith(".parquet"));
            Assert.Contains(messages, message => message.Contains("Planned first Parquet part") && message.Contains("payload OutageDeletedEvents:")
                && message.Contains("/deleted-events/") && message.EndsWith(".parquet"));
        }
        else
        {
            Assert.Contains(messages, message => message.Contains("Planned first Parquet part") && message.Contains("/snapshot/"));
            Assert.Contains(messages, message => message.StartsWith("Snapshot manifest written and verified durable")
                && message.Contains(fixture.Published.Single().ManifestPath!));
        }
    }

    [Fact]
    public async Task EmptyDeletedFeedDoesNotLogAFileAsWritten()
    {
        var fixture = new Fixture { LatestSourceUpdatedDate = Start.AddHours(1) };
        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Incremental), default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        var messages = fixture.Logger.Entries.Where(entry => entry.ContainsKey("Message"))
            .Select(entry => Assert.IsType<string>(entry["Message"])).ToArray();
        Assert.Contains(messages, message => message.StartsWith("No rows for OutageDeletedEvents") && message.Contains("no Parquet file written"));
        Assert.DoesNotContain(messages, message => message.StartsWith("Parquet file written and verified durable") && message.Contains("/deleted-events/"));
    }

    [Theory]
    [InlineData(false, IngestionMode.Incremental)]
    [InlineData(true, IngestionMode.Incremental)]
    [InlineData(false, IngestionMode.Refresh)]
    [InlineData(true, IngestionMode.Refresh)]
    public async Task LogsAuthoritativeOutputPrefixOnceForManualAndClaimedRuns(bool claimed, IngestionMode mode)
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1),
            RefreshEvents = [MakeEvent("OC-refresh")]
        };

        var result = claimed
            ? await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(mode), default)
            : await fixture.Runner.RunAsync(
                new(42, "incorrect-request", "Outage", mode, "run-1", Start, Start.AddDays(1)), default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        var entry = Assert.Single(fixture.Logger.Entries, entry =>
            entry.TryGetValue("Message", out var message) && message is string text && text.Contains("Output path:"));
        var message = Assert.IsType<string>(entry["Message"]);
        Assert.Contains($"Output path: {RawPathBuilder.BuildBasePath("CHW01", fixture.LeaseToken, mode)}.", message);
        Assert.Contains($"source Outage, token {fixture.LeaseToken}", message);
        Assert.DoesNotContain("incorrect-request", message);
        var outputIndex = fixture.Logger.Entries.IndexOf(entry);
        var sourceIndex = fixture.Logger.Entries.FindIndex(log => log.TryGetValue("{OriginalFormat}", out var template)
            && template is string text && text.StartsWith("Source contact succeeded"));
        Assert.True(sourceIndex > outputIndex);
    }

    [Fact]
    public async Task MultipleCatchUpWindowsForTheChangedFeedWriteToDistinctArtifactPathsAndCommitOnceAtTheEnd()
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(2),
            MaxWindowSize = TimeSpan.FromHours(1),
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(2, fixture.Published.Count);
        Assert.All(fixture.Published, message => Assert.Equal(OutagePayloadKinds.OutageEvents, message.PayloadKind));
        Assert.NotEqual(fixture.Published[0].BlobPath, fixture.Published[1].BlobPath);
        Assert.Equal(RawPathBuilder.BuildEventsSingleArtifactPath("CHW01", fixture.LeaseToken, Start, Start.AddHours(1)), fixture.Published[0].BlobPath);
        Assert.Equal(RawPathBuilder.BuildEventsSingleArtifactPath("CHW01", fixture.LeaseToken, Start.AddHours(1), Start.AddHours(2)), fixture.Published[1].BlobPath);
        // Both feeds' windows are fully processed before the single fenced end-of-run commit.
        Assert.Single(fixture.Commits);
        Assert.Equal(Start.AddHours(2), fixture.Commits[0].MainUpdatedUtc);
        Assert.Equal(Start.AddHours(2), fixture.Commits[0].DeletedUpdatedUtc);
        Assert.True(fixture.RunCompleted);
    }

    [Fact]
    public async Task PublishFailureOnASecondWindowLeavesNoCommitAndFailsTheRun()
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(2),
            MaxWindowSize = TimeSpan.FromHours(1),
            FailPublishOnCallNumber = 2,
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        // Section 10/11: a failing feed must never leave a partial checkpoint committed.
        Assert.Empty(fixture.Commits);
        Assert.True(fixture.Released);
        Assert.True(fixture.RunFailed);
    }

    [Fact]
    public async Task DeletedFeedIsProcessedAfterTheChangedFeedAndPublishesItsOwnPayloadKind()
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1),
            IncludeDeletedEventPerWindow = true,
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(2, fixture.Published.Count);
        var eventsMessage = Assert.Single(fixture.Published, m => m.PayloadKind == OutagePayloadKinds.OutageEvents);
        var deletedMessage = Assert.Single(fixture.Published, m => m.PayloadKind == OutagePayloadKinds.OutageDeletedEvents);
        Assert.NotEqual(eventsMessage.BlobPath, deletedMessage.BlobPath);
        Assert.NotNull(deletedMessage.BlobPath);
        // Both feeds committed together, once.
        Assert.Single(fixture.Commits);
        Assert.True(fixture.RunCompleted);
    }

    [Fact]
    public async Task DeletedFeedFailureAfterAChangedFeedSuccessStillCommitsNeitherCheckpoint()
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1),
            IncludeDeletedEventPerWindow = true,
            FailPublishOnCallNumber = 2, // 1st publish = changed feed (succeeds), 2nd = deleted feed (fails)
        };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.Commits);
        Assert.True(fixture.RunFailed);
        Assert.True(fixture.Released);
        AssertTelemetryUsesDatabaseName(fixture);
        Assert.DoesNotContain(fixture.Logger.Entries, entry => entry.ContainsKey("LastCommittedId"));
    }

    [Fact]
    public async Task RefreshNeverAdvancesTheIncrementalCursorEvenOnSuccess()
    {
        var fixture = new Fixture { RefreshEvents = [MakeEvent("OC-1")] };

        var result = await fixture.Runner.RunAsync(
            new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Refresh, "run-1", Start, Start.AddDays(1)),
            CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        // Refresh commits with the unchanged snapshot as its own to-snapshot; it never moves the checkpoint value.
        Assert.Single(fixture.Commits);
        Assert.Equal(Start, fixture.Commits[0].MainUpdatedUtc);
        Assert.Single(fixture.Published);
        Assert.Equal("Refresh", fixture.Published[0].ExtractionMode);
        Assert.Equal(OutagePayloadKinds.OutageSnapshot, fixture.Published[0].PayloadKind);
        Assert.NotNull(fixture.Published[0].ManifestPath);
        Assert.Null(fixture.Published[0].BlobPath);
        Assert.True(fixture.RunCompleted);
    }

    [Fact]
    public async Task RefreshPublishFailureLeavesNoCursorMovementAndFailsTheRun()
    {
        var fixture = new Fixture { RefreshEvents = [MakeEvent("OC-1")], FailPublishOnCallNumber = 1 };

        var result = await fixture.Runner.RunAsync(
            new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Refresh, "run-1", Start, Start.AddDays(1)),
            CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.Commits);
        Assert.True(fixture.Released);
        Assert.True(fixture.RunFailed);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    public async Task ClaimedRefreshPublishesOneCompleteSnapshotIncludingEmptyScopes(int eventCount)
    {
        var fixture = new Fixture
        {
            RefreshEvents = Enumerable.Range(1, eventCount).Select(i => MakeEvent($"OC-{i}")).ToArray(),
            MaxEventsPerShard = 1,
            IncludeDeletedEventPerWindow = true,
            DeletedCheckpoint = Start.AddDays(-2)
        };
        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Refresh), default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        var message = Assert.Single(fixture.Published);
        Assert.Equal(OutagePayloadKinds.OutageSnapshot, message.PayloadKind);
        Assert.Equal(Start, message.QueryStartInclusiveUtc);
        Assert.Equal(Start.AddDays(1), message.QueryEndExclusiveUtc);
        Assert.Null(message.BlobPath);
        Assert.Equal(eventCount, message.RowCount);
        Assert.Equal(("refresh", "CHW01"), Assert.Single(fixture.SourceCalls));
        Assert.Equal(eventCount + 1, fixture.ObjectPaths.Count);
        Assert.All(fixture.ObjectPaths, path => Assert.Contains("/mode=refresh/extraction=73/snapshot/", path));
        Assert.Empty(fixture.Commits);
        var finalized = Assert.Single(fixture.Finalizations);
        Assert.Equal(Start, finalized.Result.CompletedCheckpointUpdatedUtc);
        Assert.Null(finalized.Result.CompletedDeletedCheckpointUpdatedUtc);
    }

    [Theory]
    [InlineData("part-0002.parquet")]
    [InlineData("manifest.json")]
    public async Task ClaimedRefreshStorageFailurePublishesNothingAndFinalizesFailed(string suffix)
    {
        var fixture = new Fixture
        {
            RefreshEvents = [MakeEvent("OC-1"), MakeEvent("OC-2")],
            MaxEventsPerShard = 1,
            FailUploadSuffix = suffix
        };
        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Refresh), default);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.Published);
        Assert.Empty(fixture.Commits);
        Assert.Equal(RunOutcome.Failed, Assert.Single(fixture.Finalizations).Result.Outcome);
    }

    [Fact]
    public async Task RefreshInvalidRowsCannotPublishAnAuthoritativeEmptySnapshot()
    {
        var fixture = new Fixture { RefreshEvents = [MakeEvent("")] };
        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Refresh), default);
        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.Published);
        Assert.Empty(fixture.ObjectPaths);
        Assert.Empty(fixture.Commits);
    }

    [Fact]
    public async Task RefreshRejectedFinalizationIsNotReportedAsSuccess()
    {
        var fixture = new Fixture { RejectFinalize = true };
        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Refresh), default);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Single(fixture.Published); // at-least-once publication may precede lost fencing
        Assert.Empty(fixture.Commits);
    }

    [Fact]
    public async Task NoDataIncrementalRunStillCompletesTheRunWithUnchangedCheckpoints()
    {
        var fixture = new Fixture { LatestSourceUpdatedDate = Start };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Single(fixture.Commits);
        Assert.Equal(Start, fixture.Commits[0].MainUpdatedUtc);
        Assert.Equal(Start, fixture.Commits[0].DeletedUpdatedUtc);
        Assert.Empty(fixture.Published);
        Assert.True(fixture.RunCompleted);
    }

    [Fact]
    public async Task CancellationDuringWindowProcessingFailsTheRunAndReleasesTheLeaseWithoutCommitting()
    {
        using var cts = new CancellationTokenSource();
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1),
            CancelDuringWindow = cts,
        };

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), cts.Token));

        Assert.True(fixture.RunFailed);
        Assert.True(fixture.Released);
        Assert.False(fixture.RunCompleted);
        Assert.Empty(fixture.Commits);
    }

    [Fact]
    public async Task NoDataRunWhoseCompletionIsRejectedReportsZombieAndFailsTheRunInsteadOfSuccess()
    {
        var fixture = new Fixture { LatestSourceUpdatedDate = Start, RejectCompleteRun = true };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.False(fixture.RunCompleted);
        Assert.True(fixture.RunFailed);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task NullStartRunReleasesTheLeaseAndDoesNotAttemptToFailANonExistentRun()
    {
        var fixture = new Fixture { FailStartRun = true };

        var result = await fixture.Runner.RunAsync(new IngestionRequestModel(42, "incorrect-request", "Outage", IngestionMode.Incremental), CancellationToken.None);

        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.True(fixture.Released);
        Assert.False(fixture.RunFailed);
        Assert.False(fixture.RunCompleted);
        Assert.Empty(fixture.SourceCalls);
        Assert.Empty(fixture.Published);
        Assert.Empty(fixture.Commits);
    }

    [Fact]
    public async Task IncrementalUsesDatabaseNameForBothFeedsPathsAndTelemetryButIntegerIdentityForRepositories()
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1),
            IncludeDeletedEventPerWindow = true,
            DeletedCheckpoint = Start.AddMinutes(-30)
        };

        var result = await fixture.Runner.RunAsync(new(42, "incorrect-request", "Outage", IngestionMode.Incremental), default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(new[] { "latestSourceUpdatedDate", "live", "deleted" }, fixture.SourceCalls.Select(call => call.Operation));
        Assert.All(fixture.SourceCalls, call => Assert.Equal("CHW01", call.Name));
        Assert.Equal(Start, fixture.ChangedLow);
        Assert.Equal(Start.AddMinutes(-30), fixture.DeletedLow);
        Assert.Equal(Fixture.RetentionFloor, fixture.ObservedRetentionFloor);
        Assert.All(fixture.RepositoryIds, id => Assert.Equal(42, id));
        Assert.Contains(42, fixture.RepositoryIds);
        Assert.Equal(RawPathBuilder.BuildBasePath("CHW01", fixture.LeaseToken), fixture.OutputPath);
        Assert.All(fixture.ObjectPaths, path => Assert.Contains("windfarm=CHW01/", path));
        Assert.Equal(2, fixture.Published.Count);
        Assert.All(fixture.Published, message => Assert.Equal("42", message.WindfarmId));
        Assert.Equal(RawPathBuilder.BuildEventsSingleArtifactPath("CHW01", fixture.LeaseToken, Start, Start.AddHours(1)), fixture.Published[0].BlobPath);
        Assert.Equal(RawPathBuilder.BuildDeletedEventsSingleArtifactPath("CHW01", fixture.LeaseToken, Start.AddMinutes(-30), Start.AddHours(1)), fixture.Published[1].BlobPath);
        var commit = Assert.Single(fixture.Commits);
        Assert.Equal("OC-window-1", commit.MainId);
        Assert.Equal("OC-deleted-window-1", commit.DeletedId);
        Assert.Equal(Start, commit.ExpectedMainUtc);
        Assert.Equal(fixture.DeletedCheckpoint, commit.ExpectedDeletedUtc);
        AssertTelemetryUsesDatabaseName(fixture);
    }

    [Fact]
    public async Task RefreshUsesDatabaseNameForSnapshotAndRawPathsWithoutReadingIncrementalFeeds()
    {
        var fixture = new Fixture { RefreshEvents = [MakeEvent("OC-snapshot")] };

        var result = await fixture.Runner.RunAsync(
            new(42, "incorrect-request", "Outage", IngestionMode.Refresh, null, Start, Start.AddDays(1)), default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(("refresh", "CHW01"), Assert.Single(fixture.SourceCalls));
        Assert.All(fixture.RepositoryIds, id => Assert.Equal(42, id));
        Assert.Equal(Fixture.RetentionFloor, fixture.ObservedRetentionFloor);
        Assert.Equal(Start, fixture.ObservedScopeStartUtc);
        Assert.Equal(Start.AddDays(1), fixture.ObservedScopeEndUtc);
        Assert.All(fixture.ObjectPaths, path => Assert.Contains("windfarm=CHW01/", path));
        Assert.Equal(RawPathBuilder.BuildBasePath("CHW01", fixture.LeaseToken, IngestionMode.Refresh), fixture.OutputPath);
        var message = Assert.Single(fixture.Published);
        Assert.Equal("42", message.WindfarmId);
        Assert.Equal("Refresh", message.ExtractionMode);
        Assert.Equal(OutagePayloadKinds.OutageSnapshot, message.PayloadKind);
        Assert.NotNull(message.ManifestPath);
        var commit = Assert.Single(fixture.Commits);
        Assert.Equal(Start, commit.MainUpdatedUtc);
        Assert.Equal("initial-live", commit.MainId);
        Assert.Null(commit.DeletedUpdatedUtc); // null means leave the deletion checkpoint untouched
        Assert.Null(commit.DeletedId);
        AssertTelemetryUsesDatabaseName(fixture);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public async Task MissingDatabaseNameFailsWithoutFallingBackToRequestNameOrId(string name)
    {
        var fixture = new Fixture { DatabaseName = name };

        var result = await fixture.Runner.RunAsync(new(42, "incorrect-request", "Outage", IngestionMode.Incremental), default);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.True(fixture.Released);
        Assert.Null(fixture.OutputPath);
        Assert.Empty(fixture.SourceCalls);
        Assert.Empty(fixture.ObjectPaths);
        Assert.Empty(fixture.Published);
        Assert.False(fixture.RunFailed); // no run row was started
    }

    [Theory]
    [InlineData(0, "CHW01")]
    [InlineData(-1, "CHW01")]
    [InlineData(42, "")]
    [InlineData(42, " ")]
    public async Task InvalidRequestIsRejectedBeforeAcquiringLease(int id, string name)
    {
        var fixture = new Fixture();
        await Assert.ThrowsAnyAsync<ArgumentException>(() => fixture.Runner.RunAsync(new(id, name, "Outage", IngestionMode.Incremental), default));
        Assert.Empty(fixture.RepositoryIds);
        Assert.Empty(fixture.SourceCalls);
    }

    [Theory]
    [InlineData(IngestionMode.Incremental)]
    [InlineData(IngestionMode.Refresh)]
    public async Task ClaimedRunUsesLeaseNameAndFinalizesThroughSchedulingRepository(IngestionMode mode)
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1), IncludeDeletedEventPerWindow = true,
            RefreshEvents = [MakeEvent("OC-refresh")]
        };
        var claim = fixture.CreateClaim(mode);

        var result = await fixture.Runner.RunClaimedAsync(claim, default);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        var attempt = Assert.Single(fixture.Finalizations);
        Assert.Equal(42, attempt.Claim.Lease.WindfarmId);
        Assert.Equal(42, attempt.Claim.Request.WindfarmId);
        Assert.Equal(RunOutcome.Success, attempt.Result.Outcome);
        Assert.All(fixture.SourceCalls, call => Assert.Equal("CHW01", call.Name));
        Assert.All(fixture.Published, message => Assert.Equal("42", message.WindfarmId));
        Assert.All(fixture.ObjectPaths, path => Assert.Contains("windfarm=CHW01/", path));
        Assert.All(fixture.RepositoryIds, id => Assert.Equal(42, id));
        Assert.Empty(fixture.Commits); // no competing cursor-repository finalization
        Assert.False(fixture.Released);
        Assert.False(fixture.RunFailed);
        if (mode == IngestionMode.Incremental)
        {
            Assert.Equal("OC-window-1", attempt.Result.CompletedCheckpointId);
            Assert.Equal("OC-deleted-window-1", attempt.Result.CompletedDeletedCheckpointId);
        }
        else
        {
            Assert.Equal(Start, attempt.Result.CompletedCheckpointUpdatedUtc);
            Assert.Null(attempt.Result.CompletedDeletedCheckpointUpdatedUtc);
        }
        AssertTelemetryUsesDatabaseName(fixture);
    }

    [Fact]
    public async Task ClaimedDeletedFeedFailureFinalizesFailedWithoutEitherCheckpoint()
    {
        var fixture = new Fixture
        {
            LatestSourceUpdatedDate = Start.AddHours(1), IncludeDeletedEventPerWindow = true, FailPublishOnCallNumber = 2
        };

        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Incremental), default);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        var attempt = Assert.Single(fixture.Finalizations);
        Assert.Equal(RunOutcome.Failed, attempt.Result.Outcome);
        Assert.Null(attempt.Result.CompletedCheckpointUpdatedUtc);
        Assert.Null(attempt.Result.CompletedDeletedCheckpointUpdatedUtc);
        Assert.Empty(fixture.Commits);
        Assert.False(fixture.RunFailed);
        AssertTelemetryUsesDatabaseName(fixture);
    }

    [Theory]
    [InlineData(IngestionMode.Incremental)]
    [InlineData(IngestionMode.Refresh)]
    public async Task ClaimedMissingDatabaseNameFailsBeforeSourceAndFinalizesFailed(IngestionMode mode)
    {
        var fixture = new Fixture { DatabaseName = " " };

        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(mode), default);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal(RunOutcome.Failed, Assert.Single(fixture.Finalizations).Result.Outcome);
        Assert.Empty(fixture.SourceCalls);
        Assert.Empty(fixture.ObjectPaths);
        Assert.Empty(fixture.Published);
    }

    [Fact]
    public async Task ClaimedRunCancellationFinalizesWithUncancelledCleanupToken()
    {
        using var cts = new CancellationTokenSource();
        var fixture = new Fixture { LatestSourceUpdatedDate = Start.AddHours(1), CancelDuringWindow = cts };

        await Assert.ThrowsAsync<OperationCanceledException>(() => fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Incremental), cts.Token));

        Assert.Equal(RunOutcome.Cancelled, Assert.Single(fixture.Finalizations).Result.Outcome);
        Assert.Empty(fixture.Commits);
        Assert.False(fixture.RunFailed);
    }

    [Fact]
    public async Task ClaimedRunRejectedFinalizationIsZombieNotSuccess()
    {
        var fixture = new Fixture { RejectFinalize = true };
        var result = await fixture.Runner.RunClaimedAsync(fixture.CreateClaim(IngestionMode.Incremental), default);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Empty(fixture.Commits);
        Assert.DoesNotContain(fixture.Logger.Entries, entry => entry.ContainsKey("LastCommittedId"));
    }

    private static void AssertTelemetryUsesDatabaseName(Fixture fixture)
    {
        var entries = fixture.Logger.Entries.Where(entry => entry.ContainsKey("WindfarmName")).ToArray();
        Assert.NotEmpty(entries);
        Assert.All(entries, entry => Assert.Equal("CHW01", entry["WindfarmName"]));
        Assert.All(fixture.Logger.Entries, entry => Assert.False(entry.ContainsKey("WindfarmId")));
        Assert.Contains(fixture.Logger.Scopes, scope => Equals(scope["WindfarmName"], "CHW01"));
    }

    private static OutageEventModel MakeEvent(string eventId) => new(
        EventId: eventId, WindfarmId: "CHW01", ItemId: "wtg-1", EventType: "Outage",
        OriginalStartUtc: Start, OriginalEndUtc: null, Status: "Open",
        SourceRevision: null, SourceWatermarkUtc: Start, IsTombstone: false,
        LastUpdatedDate: Start, OutageId: "OUT-1");

    private static OutageEventModel MakeTombstone(string eventId) => new(
        EventId: eventId, WindfarmId: "CHW01", ItemId: "wtg-1", EventType: "Outage",
        OriginalStartUtc: default, OriginalEndUtc: null, Status: "Deleted",
        SourceRevision: null, SourceWatermarkUtc: Start, IsTombstone: true,
        LastUpdatedDate: Start, OutageId: "OUT-2");

    /// <summary>One recorded call to <see cref="IExtractionCursorRepository.TryCompleteRunAsync"/>.</summary>
    private sealed record CommitCall(DateTime MainUpdatedUtc, DateTime? DeletedUpdatedUtc,
        string MainId, string? DeletedId, DateTime? ExpectedMainUtc, DateTime? ExpectedDeletedUtc);

    private sealed class Fixture : IExtractionCursorRepository, IOutageSourceAdapter, IRawObjectStore, IRawArtifactPublisher,
        IOutageScopeRepository, IExtractionSchedulingRepository
    {
        private readonly Dictionary<string, byte[]> _objects = new();
        private int _publishCallCount;
        private int _changedWindowCallCount;
        private int _deletedWindowCallCount;
        private long _nextRunId = 1;

        public DateTime LatestSourceUpdatedDate { get; init; } = Start;
        public TimeSpan MaxWindowSize { get; init; } = TimeSpan.FromDays(30);
        public int MaxEventsPerShard { get; init; } = 50_000;
        public int? FailPublishOnCallNumber { get; init; }
        public string? FailUploadSuffix { get; init; }
        public CancellationTokenSource? CancelDuringWindow { get; init; }
        public IReadOnlyList<OutageEventModel> RefreshEvents { get; init; } = [];
        public bool IncludeDeletedEventPerWindow { get; init; }
        public long LeaseToken { get; } = 73;
        public bool FailStartRun { get; init; }
        public bool RejectCompleteRun { get; init; }
        public bool RejectFinalize { get; init; }
        public string DatabaseName { get; init; } = "CHW01";
        public DateTime DeletedCheckpoint { get; init; } = Start;
        public string[] ActiveTurbineNames { get; init; } = ["wtg-1"];
        public bool OmitScopeRepository { get; init; }
        public bool MissingScope { get; init; }
        public bool MutateScopeAfterResolution { get; init; }
        public int ScopeReadCount { get; private set; }
        public List<(string Feed, string[] Names)> TurbineScopes { get; } = [];
        public static readonly DateTime RetentionFloor = Start.AddYears(-1);
        public List<(string Operation, string Name)> SourceCalls { get; } = [];
        public List<int> RepositoryIds { get; } = [];
        public IReadOnlyCollection<string> ObjectPaths => _objects.Keys;
        public List<ExtractionAttemptResultModel> Finalizations { get; } = [];
        public TestIngestionLogger<OutageIngestionRunner> Logger { get; } = new();
        public string? OutputPath { get; private set; }
        public DateTime? ChangedLow { get; private set; }
        public DateTime? DeletedLow { get; private set; }
        public DateTime? ObservedRetentionFloor { get; private set; }
        public DateTime? ObservedScopeStartUtc { get; private set; }
        public DateTime? ObservedScopeEndUtc { get; private set; }

        public List<CommitCall> Commits { get; } = new();
        public List<RawArtifactAvailableV2> Published { get; } = new();
        public bool Released { get; private set; }
        public bool RunCompleted { get; private set; }
        public bool RunFailed { get; private set; }

        public OutageIngestionRunner Runner => new(
            this, this,
            new OutageBatchWriter(this, MaxEventsPerShard),
            this,
            new CatchUpWindowPlanner(),
            new ChangeDiscoveryReader(this),
            new IngestionTelemetry<OutageIngestionRunner>(Logger),
            new IngestionOptions { MaxCatchUpWindowSize = MaxWindowSize },
            "test-owner", OmitScopeRepository ? null : this, this);

        public Task<ExtractionLeaseModel?> GetCurrentAsync(int windfarmId, string sourceType, CancellationToken cancellationToken)
            => Task.FromResult<ExtractionLeaseModel?>(null);

        public Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(int windfarmId, string sourceType, string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            RepositoryIds.Add(windfarmId);
            return Task.FromResult<ExtractionLeaseModel?>(CreateLease(windfarmId, sourceType));
        }

        private ExtractionLeaseModel CreateLease(int windfarmId = 42, string sourceType = "Outage") => new()
            {
                WindfarmId = windfarmId,
                WindfarmName = DatabaseName,
                SourceType = sourceType,
                LastUpdatedUtc = Start,
                LastCommittedId = "initial-live",
                DeletedLastUpdatedUtc = DeletedCheckpoint,
                DeletedLastCommittedId = "initial-deleted",
                LeaseToken = LeaseToken,
                LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(5),
            };

        public Task<bool> TryRenewLeaseAsync(int windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            RepositoryIds.Add(windfarmId);
            return Task.FromResult(true);
        }

        public Task<bool> TryCommitAsync(
            int windfarmId, string sourceType, long leaseToken, DateTime newLastUpdatedUtc, string newLastCommittedId,
            CancellationToken cancellationToken, DateTime? expectedLastUpdatedUtc = null)
            => throw new NotSupportedException("Per Section 10, Outage never commits an interim per-window checkpoint.");

        public Task ReleaseLeaseAsync(int windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken)
        {
            RepositoryIds.Add(windfarmId);
            Released = true;
            return Task.CompletedTask;
        }

        public Task<ExtractionRunModel?> StartRunAsync(
            int windfarmId, string sourceType, long leaseToken,
            DateTime fromUpdatedAtUtc, string fromCommittedId, string? outputPath, CancellationToken cancellationToken)
        {
            RepositoryIds.Add(windfarmId);
            OutputPath = outputPath;
            if (FailStartRun)
            {
                return Task.FromResult<ExtractionRunModel?>(null);
            }
            return Task.FromResult<ExtractionRunModel?>(new ExtractionRunModel(
                RunId: _nextRunId++, WindfarmId: windfarmId, SourceType: sourceType,
                Status: ExtractionRunStatus.Running, StartedAtUtc: DateTime.UtcNow, CompletedAtUtc: null,
                FromUpdatedAtUtc: fromUpdatedAtUtc, FromCommittedId: fromCommittedId,
                ToUpdatedAtUtc: null, ToCommittedId: null, OutputPath: outputPath, ErrorMessage: null));
        }

        public Task<bool> TryCompleteRunAsync(
            long runId, int windfarmId, string sourceType, long leaseToken,
            DateTime newLastUpdatedUtc, string newLastCommittedId, CancellationToken cancellationToken,
            DateTime? expectedLastUpdatedUtc = null,
            DateTime? newDeletedLastUpdatedUtc = null, string? newDeletedLastCommittedId = null,
            DateTime? expectedDeletedLastUpdatedUtc = null)
        {
            RepositoryIds.Add(windfarmId);
            if (RejectCompleteRun)
            {
                return Task.FromResult(false);
            }
            Commits.Add(new CommitCall(newLastUpdatedUtc, newDeletedLastUpdatedUtc, newLastCommittedId, newDeletedLastCommittedId,
                expectedLastUpdatedUtc, expectedDeletedLastUpdatedUtc));
            RunCompleted = true;
            return Task.FromResult(true);
        }

        public Task FailRunAsync(
            long runId, int windfarmId, string sourceType, long leaseToken,
            DateTime? partialUpdatedAtUtc, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken)
        {
            RepositoryIds.Add(windfarmId);
            RunFailed = true;
            return Task.CompletedTask;
        }

        public Task<DateTime> GetLatestSourceUpdatedDateByWindfarmIdAsync(string windfarmName, CancellationToken cancellationToken)
        {
            SourceCalls.Add(("latestSourceUpdatedDate", windfarmName));
            return Task.FromResult(LatestSourceUpdatedDate);
        }

        public async IAsyncEnumerable<OutageEventModel> GetChangedOutagesAsync(
            string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime low, DateTime end, DateTime minRetrieveDateUtc,
            (DateTime LastUpdatedDate, string EventId)? continuationKey,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            SourceCalls.Add(("live", windfarmName));
            RecordTurbineScope("live", activeTurbineNames);
            ChangedLow = low;
            ObservedRetentionFloor = minRetrieveDateUtc;
            _changedWindowCallCount++;
            await Task.Yield();
            if (CancelDuringWindow is { } cts)
            {
                cts.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (activeTurbineNames.Count == 0) yield break;
            yield return MakeEvent($"OC-window-{_changedWindowCallCount}") with { LastUpdatedDate = end };
        }

        public async IAsyncEnumerable<OutageEventModel> GetDeletedOutagesAsync(
            string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime low, DateTime end,
            (DateTime LastUpdatedDate, string EventId)? continuationKey,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            SourceCalls.Add(("deleted", windfarmName));
            RecordTurbineScope("deleted", activeTurbineNames);
            DeletedLow = low;
            _deletedWindowCallCount++;
            await Task.Yield();
            if (IncludeDeletedEventPerWindow && activeTurbineNames.Count > 0)
            {
                yield return MakeTombstone($"OC-deleted-window-{_deletedWindowCallCount}") with { LastUpdatedDate = end };
            }
        }

        public async IAsyncEnumerable<OutageEventModel> GetAllCurrentEventsAsync(
            string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, DateTime scopeStartUtc, DateTime scopeEndUtc,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            SourceCalls.Add(("refresh", windfarmName));
            RecordTurbineScope("refresh", activeTurbineNames);
            ObservedRetentionFloor = minRetrieveDateUtc;
            ObservedScopeStartUtc = scopeStartUtc;
            ObservedScopeEndUtc = scopeEndUtc;
            await Task.Yield();
            if (activeTurbineNames.Count == 0) yield break;
            foreach (var evt in RefreshEvents)
            {
                yield return evt;
            }
        }

        public Task<WindfarmScopeModel?> GetActiveScopeAsync(int windfarmId, DateTime nowUtc, CancellationToken cancellationToken)
        {
            RepositoryIds.Add(windfarmId);
            ScopeReadCount++;
            // The retention lookup cannot override the authoritative lease name either.
            return Task.FromResult<WindfarmScopeModel?>(MissingScope ? null : new(windfarmId, "scope-name-not-used", RetentionFloor, ActiveTurbineNames));
        }

        private void RecordTurbineScope(string feed, IReadOnlyList<string> names)
        {
            if (MutateScopeAfterResolution && ActiveTurbineNames.Length > 0)
                ActiveTurbineNames[0] = "changed-after-resolution";
            TurbineScopes.Add((feed, names.ToArray()));
        }

        public ExtractionClaimModel CreateClaim(IngestionMode mode) => new(
            mode == IngestionMode.Refresh
                ? new(42, "incorrect-request", "Outage", mode, "claimed-run", Start, Start.AddDays(1))
                : new(42, "incorrect-request", "Outage", mode, "claimed-run"),
            CreateLease(),
            new(99, 42, "Outage", ExtractionRunStatus.Running, Start, null, Start, "initial-live", null, null, null, null),
            "test-owner");

        public Task<bool> TryFinalizeAsync(ExtractionAttemptResultModel attempt, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Finalizations.Add(attempt);
            RepositoryIds.Add(attempt.Claim.Lease.WindfarmId);
            return Task.FromResult(!RejectFinalize);
        }

        public Task<bool> IsRegisteredAsync(ExtractionLookupModel lookup, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ExtractionSelectionStatusModel> GetSelectionStatusAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ExtractionClaimModel?> TryClaimCursorAndStartExtractionRunAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            if (FailUploadSuffix is not null && path.EndsWith(FailUploadSuffix))
                throw new InvalidOperationException("Simulated snapshot upload failure.");
            _objects[path] = content.ToArray();
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken) => Task.FromResult(_objects.ContainsKey(path));

        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken) => Task.FromResult(_objects[path]);

        public Task PublishAsync(RawArtifactAvailableV2 message, CancellationToken cancellationToken)
        {
            message.ValidateOutage();
            if (message.PayloadKind == OutagePayloadKinds.OutageSnapshot)
            {
                Assert.NotNull(message.ManifestPath);
                var manifest = System.Text.Json.JsonSerializer.Deserialize<OutageSnapshotManifestModel>(_objects[message.ManifestPath]);
                Assert.NotNull(manifest);
                Assert.Equal(message.RunId, manifest.RunId);
                Assert.Equal(message.QueryStartInclusiveUtc, manifest.ScopeStartUtc);
                Assert.Equal(message.QueryEndExclusiveUtc, manifest.ScopeEndUtc);
                Assert.All(manifest.Parts, path => Assert.True(_objects.ContainsKey(path)));
            }
            _publishCallCount++;
            if (FailPublishOnCallNumber == _publishCallCount)
            {
                throw new InvalidOperationException("Simulated publish failure.");
            }
            Published.Add(message);
            return Task.CompletedTask;
        }
    }
}

