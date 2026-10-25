using System.Runtime.CompilerServices;
using ProductionIngestor.Configuration;
using ProductionIngestor.Contracts;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Discovery;
using ProductionIngestor.Enums;
using ProductionIngestor.Operations.Execution;
using ProductionIngestor.Operations.Extraction;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Monitoring;
using ProductionIngestor.Raw;
using ProductionIngestor.Serialization;
using Microsoft.Extensions.Logging.Abstractions;

namespace ProductionIngestor.UnitTests;

public sealed class IngestionRunnerTests
{
    private static readonly DateTime Start = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task NoDataRunNeverQueriesLatestSourceTimestamp()
    {
        var fixture = new Fixture { NoSourceData = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.False(fixture.CalledGetLatestSourceUpdatedDate);
        Assert.Equal(0, fixture.Published);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task LeaseLossCancelsAnInFlightPublication()
    {
        var fixture = new Fixture { LoseLeaseDuringPublication = true };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), timeout.Token);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.True(fixture.PublicationStarted);
        Assert.Equal(0, fixture.Published);
        Assert.False(fixture.RunCompleted);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task FailedDayDoesNotCommitSuccessfulDayWithLaterUpdateTimestamp()
    {
        var fixture = new Fixture { FailSecondPublication = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Single(result.DaysWritten);
        Assert.False(fixture.RunCompleted);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task SuccessfulWindowCommitsAfterAllPublications()
    {
        var fixture = new Fixture();
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Success, result.Outcome);
        // Checkpoint advances to the maximum LastUpdatedUtc actually observed, not a window boundary.
        Assert.True(fixture.RunCompleted);
        Assert.Equal(Start.AddHours(12), fixture.LastCompleteNewUpdatedUtc);
        Assert.Equal(2, fixture.Published);
        Assert.True(fixture.Renewed);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task LostLeaseStopsBeforePublication()
    {
        var fixture = new Fixture { LoseLease = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Equal(0, fixture.Published);
        Assert.False(fixture.RunCompleted);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task CancellationDuringPublicationPropagatesAndReleasesLease()
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = new Fixture { CancelPublication = cancellation };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Runner.RunAsync(
            new(1, "farm", "Production", IngestionMode.Incremental), cancellation.Token));
        Assert.False(fixture.RunCompleted);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task RefreshPublishesButDoesNotMoveIncrementalCursor()
    {
        var fixture = new Fixture();
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Refresh,
            [new DateOnly(2026, 1, 1)]), CancellationToken.None);
        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.True(fixture.RunCompleted);
        Assert.Equal(Start, fixture.LastCompleteNewUpdatedUtc);
        Assert.Equal(1, fixture.Published);
        var ready = Assert.Single(fixture.ExtractionReadyMessages);
        Assert.Equal("Refresh", ready.ExtractionMode);
        Assert.Equal("ProductionSnapshot", ready.PayloadKind);
        Assert.Equal(ProductionParquetSchemaV2.Version, ready.PayloadSchemaVersion);
        Assert.Equal("ProductionTimestamp", ready.QueryAxis);
        Assert.Equal(1, ready.TotalRowCount);
        Assert.Single(ready.Artifacts);
    }

    [Fact]
    public async Task RefreshPlansExactMonthWindowsAndPublishesOnlyAfterEveryPartIsUploaded()
    {
        var fixture = new Fixture();
        var dates = new[]
        {
            new DateOnly(2026, 7, 15), new DateOnly(2026, 7, 16),
            new DateOnly(2026, 8, 1), new DateOnly(2026, 9, 10)
        };

        var result = await fixture.Runner.RunAsync(
            new(1, "farm", "Production", IngestionMode.Refresh, dates, "refresh-request"), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        var ready = Assert.Single(fixture.ExtractionReadyMessages);
        Assert.Equal("refresh-request", ready.RequestId);
        Assert.Equal(2, ready.EnvelopeVersion);
        Assert.Equal("1", ready.AttemptId);
        Assert.Equal(dates.Select(date => date.ToString("yyyy-MM-dd")),
            ready.RequestedScope.SelectMany(range => Enumerable.Range(0,
                DateOnly.Parse(range.EndDateInclusive).DayNumber - DateOnly.Parse(range.StartDate).DayNumber + 1)
                .Select(offset => DateOnly.Parse(range.StartDate).AddDays(offset).ToString("yyyy-MM-dd"))));
        Assert.Equal(3, ready.CompletedWindows.Count);
        Assert.All(ready.CompletedWindows, window => Assert.True(window.Completed));
        Assert.Equal(3, ready.Artifacts.Count);
        Assert.Equal(3, ready.TotalRowCount);
        Assert.All(ready.Artifacts, artifact => Assert.Contains("/mode=refresh/extraction=1/", artifact.BlobPath));
        Assert.Equal(ready.Artifacts.Count, fixture.UploadedPaths.Count);
        Assert.True(fixture.RunCompleted);
    }

    [Fact]
    public async Task EmptyRefreshPublishesExplicitZeroRowScopeWithoutInventingArtifacts()
    {
        var fixture = new Fixture { NoRefreshRows = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Refresh,
            [new DateOnly(2026, 9, 1), new DateOnly(2026, 9, 3)]), CancellationToken.None);

        Assert.Equal(RunOutcome.Success, result.Outcome);
        var ready = Assert.Single(fixture.ExtractionReadyMessages);
        Assert.True(ready.IsEmptyResult);
        Assert.Equal(0, ready.TotalRowCount);
        Assert.Equal(2, ready.RequestedScope.Count);
        Assert.Empty(ready.Artifacts);
        Assert.Equal(2, ready.CompletedWindows.Count);
        Assert.Equal(new[] { "turbine" }, ready.ScopeWtgIds);
    }

    [Fact]
    public async Task RefreshWithoutActiveWindfarmScopeDoesNotPublishReady()
    {
        var fixture = new Fixture { NoActiveScope = true };

        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Refresh,
            [new DateOnly(2026, 9, 1)]), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.ExtractionReadyMessages);
        Assert.False(fixture.RunCompleted);
    }

    [Theory]
    [InlineData("query")]
    [InlineData("upload")]
    [InlineData("publish")]
    public async Task RefreshFailureNeverPublishesReadyOrCompletesRun(string failure)
    {
        var fixture = new Fixture
        {
            FailRefreshQuery = failure == "query",
            FailRawWrite = failure == "upload",
            FailReadyPublication = failure == "publish",
        };

        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Refresh,
            [new DateOnly(2026, 9, 1), new DateOnly(2026, 9, 2)]), CancellationToken.None);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Empty(fixture.ExtractionReadyMessages);
        Assert.False(fixture.RunCompleted);
    }

    [Fact]
    public async Task SuccessfulRunStartsWithTokenScopedPrefixAndCompletesAtFinalPosition()
    {
        var fixture = new Fixture();
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(1, fixture.StartCallCount);
        Assert.Equal(RawPathBuilder.BuildWindowExtractionPrefix("farm", 1), fixture.LastStartOutputPath);
        Assert.Equal(1, fixture.LastStartLeaseToken);
        Assert.True(fixture.RunCompleted);
        Assert.False(fixture.RunFailed);
        Assert.Equal(Start.AddHours(12), fixture.LastCompleteNewUpdatedUtc);
        Assert.Equal("0", fixture.LastCompleteNewCommittedId);
    }

    [Fact]
    public async Task NoDataRunCompletesWithUnchangedCursorPosition()
    {
        var fixture = new Fixture { NoSourceData = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.True(fixture.RunCompleted);
        Assert.Equal(Start, fixture.LastCompleteNewUpdatedUtc);
        Assert.Equal("0", fixture.LastCompleteNewCommittedId);
    }

    [Fact]
    public async Task PartialFailurePreservesLastCommittedProgressNotOriginalSnapshot()
    {
        var fixture = new Fixture { FailSecondPublication = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.True(fixture.RunFailed);
        // No checkpoint had committed yet when the second day failed, so the failure audit must
        // reflect the run's original from-snapshot, not a fabricated advance.
        Assert.Equal(Start, fixture.LastFailPartialUpdatedAtUtc);
        Assert.Equal("0", fixture.LastFailPartialCommittedId);
        Assert.Contains("Test publication failure", fixture.LastFailErrorMessage);
    }

    [Fact]
    public async Task CancellationRecordsAFailedRunWithoutMaskingLeaseRelease()
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = new Fixture { CancelPublication = cancellation };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Runner.RunAsync(
            new(1, "farm", "Production", IngestionMode.Incremental), cancellation.Token));
        Assert.True(fixture.RunFailed);
        Assert.False(fixture.RunCompleted);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task StartRunFencingRejectionStopsBeforeAnyWorkAndReleasesLease()
    {
        var fixture = new Fixture { RejectStart = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.Equal(0, fixture.Published);
        Assert.False(fixture.RunCompleted);
        Assert.False(fixture.RunFailed);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task NoDataRunWithRejectedCompletionFailsInsteadOfReportingSuccess()
    {
        var fixture = new Fixture { NoSourceData = true, RejectCompleteRun = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), CancellationToken.None);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.True(fixture.RunFailed);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task RefreshWithRejectedCompletionFailsInsteadOfReportingSuccess()
    {
        var fixture = new Fixture { RejectCompleteRun = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Refresh,
            [new DateOnly(2026, 1, 1)]), CancellationToken.None);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.True(fixture.RunFailed);
        Assert.True(fixture.Released);
    }

    [Fact]
    public async Task ClaimedAssignmentDoesNotReacquireAndCompletesBeforeRelease()
    {
        var fixture = new Fixture();
        var lease = (await fixture.GetCurrentAsync(1, "Production", default))!;
        var claim = fixture.Claim(lease, "claimed-owner");
        var result = await fixture.Runner.RunClaimedAsync(claim, default);
        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(0, fixture.AcquisitionCount);
        Assert.True(fixture.WorkCompleted);
        Assert.Equal(0, fixture.StartCallCount);
        Assert.Equal("claimed-owner", fixture.RenewalOwner);
    }

    [Fact]
    public async Task RejectedWorkCompletionReturnsLeaseLoss()
    {
        var fixture = new Fixture { RejectWorkCompletion = true };
        var lease = (await fixture.GetCurrentAsync(1, "Production", default))!;
        var result = await fixture.Runner.RunClaimedAsync(fixture.Claim(lease, "owner"), default);
        Assert.Equal(RunOutcome.Zombie, result.Outcome);
        Assert.False(fixture.Released);
    }

    [Fact]
    public async Task RawFailureNeverPublishesOrAdvancesCursor()
    {
        var fixture = new Fixture { FailRawWrite = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), default);
        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.False(fixture.RunCompleted);
        Assert.Equal(0, fixture.Published);
    }

    [Fact]
    public async Task DiscoveryHasNoUpperBoundAndNeverQueriesLatestSourceTimestamp()
    {
        var fixture = new Fixture { RowsAtLatestTimestamp = true };
        var result = await fixture.Runner.RunAsync(new(1, "farm", "Production", IngestionMode.Incremental), default);
        Assert.Equal(RunOutcome.Success, result.Outcome);
        Assert.Equal(2, fixture.Published);
        // No upper bound on discovery, and no separate MAX(LastUpdatedUtc) source lookup.
        Assert.Equal(DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc), fixture.DiscoveryEnd);
        Assert.False(fixture.CalledGetLatestSourceUpdatedDate);
        Assert.True(fixture.RunCompleted);
        Assert.Equal(fixture.LatestSourceUpdatedUtc, fixture.LastCompleteNewUpdatedUtc);
    }

    private sealed class Fixture : IExtractionCursorRepository, IProductionSourceAdapter, IRawObjectStore, IRawArtifactPublisher, IExtractionSchedulingRepository, IProductionScopeRepository
    {
        private readonly Dictionary<string, byte[]> _objects = new();
        private readonly TaskCompletionSource<bool> _renewalResult = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool NoSourceData { get; init; }
        public bool LoseLeaseDuringPublication { get; init; }
        public bool PublicationStarted { get; private set; }
        public bool FailSecondPublication { get; init; }
        public bool LoseLease { get; init; }
        public CancellationTokenSource? CancelPublication { get; init; }
        public int Published { get; private set; }
        public List<ProductionExtractionReadyV1> ExtractionReadyMessages { get; } = [];
        public bool Released { get; private set; }
        public bool Renewed { get; private set; }
        public int AcquisitionCount { get; private set; }
        public bool WorkCompleted { get; private set; }
        public bool RejectWorkCompletion { get; init; }
        public bool FailRawWrite { get; init; }
        public bool NoRefreshRows { get; init; }
        public bool NoActiveScope { get; init; }
        public bool FailRefreshQuery { get; init; }
        public bool FailReadyPublication { get; init; }
        private int _refreshQueryCount;
        public List<string> UploadedPaths { get; } = [];
        public bool RowsAtLatestTimestamp { get; init; }
        public DateTime LatestSourceUpdatedUtc => Start.AddDays(1).AddTicks(-1);
        public DateTime DiscoveryEnd { get; private set; }
        public string? RenewalOwner { get; private set; }
        public ProductionIngestionRunner Runner => new(this, this, new WindfarmDayExtractor(this),
            new ProductionParquetWriter(this), this, new ChangeDiscoveryReader(this),
            new IngestionTelemetry<ProductionIngestionRunner>(NullLogger<ProductionIngestionRunner>.Instance),
            new IngestionOptions { ProductionChangesEnabled = false }, "test-owner", this, scopeRepository: this);

        public Task<WindfarmScopeModel?> GetActiveScopeAsync(int windfarmId, DateTime asOfUtc, CancellationToken cancellationToken)
            => Task.FromResult(NoActiveScope ? null : new WindfarmScopeModel(windfarmId, "farm", Start, ["turbine"]));

        public ExtractionClaimModel Claim(ExtractionLeaseModel lease, string owner)
            => new(new(1, "farm", "Production", IngestionMode.Incremental), lease,
                new(1, 1, "Production", ExtractionRunStatus.Running, Start, null, Start, "0", null, null,
                    RawPathBuilder.BuildWindowExtractionPrefix("farm", lease.LeaseToken), null), owner);
        public Task<bool> IsRegisteredAsync(ExtractionLookupModel lookup, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ExtractionClaimModel?> TryClaimCursorAndStartExtractionRunAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<ExtractionSelectionStatusModel> GetSelectionStatusAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<bool> TryFinalizeAsync(ExtractionAttemptResultModel completion, CancellationToken cancellationToken)
        {
            Assert.False(Released);
            Assert.False(RunCompleted);
            WorkCompleted = !RejectWorkCompletion;
            return Task.FromResult(WorkCompleted);
        }

        public Task<DateTime?> GetLatestLastUpdatedUtcAsync(string windfarmName, IReadOnlyList<string> activeTurbineNames,
            DateTime minRetrieveDateUtc, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public IAsyncEnumerable<ProductionChangeRowModel> GetProductionChangeRowsAsync(string windfarmName,
            IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, DateTime windowMinInclusiveUtc,
            DateTime windowMaxInclusiveUtc, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<ExtractionLeaseModel?> GetCurrentAsync(int windfarmId, string sourceType, CancellationToken cancellationToken)
            => Task.FromResult<ExtractionLeaseModel?>(new ExtractionLeaseModel
            {
                WindfarmId = windfarmId,
                WindfarmName = "farm",
                SourceType = sourceType, LastUpdatedUtc = Start,
                LastCommittedId = "0", LeaseToken = 1, LeaseExpiresAtUtc = Start.AddYears(1)
            });
        public Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(int windfarmId, string sourceType, string owner,
            TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            AcquisitionCount++;
            return GetCurrentAsync(windfarmId, sourceType, cancellationToken);
        }
        public Task<bool> TryRenewLeaseAsync(int windfarmId, string sourceType, string owner, long leaseToken,
            TimeSpan leaseDuration, CancellationToken cancellationToken)
        {
            Renewed = true;
            RenewalOwner = owner;
            return LoseLeaseDuringPublication ? _renewalResult.Task.WaitAsync(cancellationToken) : Task.FromResult(!LoseLease);
        }
        // The single fenced whole-execution commit now goes through TryCompleteRunAsync only.
        public Task<bool> TryCommitAsync(int windfarmId, string sourceType, long leaseToken, DateTime newLastUpdatedUtc,
            string newLastCommittedId, CancellationToken cancellationToken, DateTime? expectedLastUpdatedUtc = null)
            => throw new NotSupportedException("TryCommitAsync is no longer called by ProductionIngestionRunner.");
        public Task ReleaseLeaseAsync(int windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken)
        {
            Released = true;
            return Task.CompletedTask;
        }
        public Task<ExtractionRunModel?> StartRunAsync(int windfarmId, string sourceType, long leaseToken,
            DateTime fromUpdatedAtUtc, string fromCommittedId, string? outputPath, CancellationToken cancellationToken)
        {
            StartCallCount++;
            LastStartOutputPath = outputPath;
            LastStartLeaseToken = leaseToken;
            if (RejectStart)
            {
                return Task.FromResult<ExtractionRunModel?>(null);
            }
            return Task.FromResult<ExtractionRunModel?>(new ExtractionRunModel(1, windfarmId, sourceType, ExtractionRunStatus.Running,
                Start, null, fromUpdatedAtUtc, fromCommittedId, null, null, outputPath, null));
        }
        public bool RejectCompleteRun { get; init; }
        public Task<bool> TryCompleteRunAsync(long runId, int windfarmId, string sourceType, long leaseToken,
            DateTime newLastUpdatedUtc, string newLastCommittedId, CancellationToken cancellationToken, DateTime? expectedLastUpdatedUtc = null,
            DateTime? queryStartInclusiveUtc = null, DateTime? queryEndExclusiveUtc = null)
        {
            if (RejectCompleteRun)
            {
                return Task.FromResult(false);
            }
            RunCompleted = true;
            LastCompleteNewUpdatedUtc = newLastUpdatedUtc;
            LastCompleteNewCommittedId = newLastCommittedId;
            return Task.FromResult(true);
        }
        public Task FailRunAsync(long runId, int windfarmId, string sourceType, long leaseToken,
            DateTime? partialUpdatedAtUtc, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken,
            DateTime? queryStartInclusiveUtc = null, DateTime? queryEndExclusiveUtc = null)
        {
            RunFailed = true;
            LastFailPartialUpdatedAtUtc = partialUpdatedAtUtc;
            LastFailPartialCommittedId = partialCommittedId;
            LastFailErrorMessage = errorMessage;
            return Task.CompletedTask;
        }
        public bool RunCompleted { get; private set; }
        public bool RunFailed { get; private set; }
        public bool RejectStart { get; init; }
        public int StartCallCount { get; private set; }
        public string? LastStartOutputPath { get; private set; }
        public long LastStartLeaseToken { get; private set; }
        public DateTime? LastCompleteNewUpdatedUtc { get; private set; }
        public string? LastCompleteNewCommittedId { get; private set; }
        public DateTime? LastFailPartialUpdatedAtUtc { get; private set; }
        public string? LastFailPartialCommittedId { get; private set; }
        public string? LastFailErrorMessage { get; private set; }
        public bool CalledGetLatestSourceUpdatedDate { get; private set; }
        public Task<LatestSourceUpdatedDateModel> GetLatestSourceUpdatedDateByWindfarmIdAsync(
            WindfarmSourceQueryModel query, CancellationToken cancellationToken)
        {
            CalledGetLatestSourceUpdatedDate = true;
            return Task.FromResult(new LatestSourceUpdatedDateModel(NoSourceData ? null : LatestSourceUpdatedUtc));
        }
        public async IAsyncEnumerable<ChangedProductionRowModel> GetChangedRowsAsync(string windfarmId,
            DateTime low, DateTime end, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            DiscoveryEnd = end;
            if (NoSourceData) yield break;
            var firstUpdate = RowsAtLatestTimestamp ? LatestSourceUpdatedUtc : Start.AddHours(10);
            var secondUpdate = RowsAtLatestTimestamp ? LatestSourceUpdatedUtc : Start.AddHours(12);
            if (firstUpdate >= low && firstUpdate < end)
                yield return new(firstUpdate, "turbine", Start.AddMonths(-1).AddDays(1), DateOnly.FromDateTime(Start.AddMonths(-1).AddDays(1)));
            if (secondUpdate >= low && secondUpdate < end)
                yield return new(secondUpdate, "turbine", Start.AddMonths(-1), DateOnly.FromDateTime(Start.AddMonths(-1)));
        }
        public async IAsyncEnumerable<ProductionRowModel> GetWindfarmDaySnapshotAsync(string windfarmId,
            DateTime dayStartUtc, DateTime dayEndUtc, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            cancellationToken.ThrowIfCancellationRequested();
            Interlocked.Increment(ref _refreshQueryCount);
            if (FailRefreshQuery) throw new IOException("Test refresh query failure");
            if (NoRefreshRows) yield break;
            yield return new(windfarmId, "turbine", dayStartUtc, 1m, null,
                null, null, Start, null, dayEndUtc, "CurrentPowerCurvekWh");
        }
        public Task<bool> PublishProductionChangesAsync(IReadOnlyList<ProductionChangesV2> parts, CancellationToken cancellationToken)
            => throw new NotSupportedException();
        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            if (FailRawWrite) throw new IOException("Test raw write failure");
            _objects[path] = content.ToArray();
            UploadedPaths.Add(path);
            return Task.CompletedTask;
        }
        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken) => Task.FromResult(_objects.ContainsKey(path));
        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken) => Task.FromResult(_objects[path]);
        public async Task PublishAsync(RawArtifactAvailableV1 message, CancellationToken cancellationToken)
        {
            PublicationStarted = true;
            if (LoseLeaseDuringPublication)
            {
                _renewalResult.TrySetResult(false);
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            CancelPublication?.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
            if (FailSecondPublication && Published == 1) throw new IOException("Test publication failure");
            Published++;
        }

        public Task PublishExtractionReadyAsync(ProductionExtractionReadyV1 message, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (FailReadyPublication) throw new IOException("Test extraction-ready publication failure");
            Assert.Equal(message.Artifacts.Count, _objects.Count);
            ExtractionReadyMessages.Add(message);
            Published++;
            return Task.CompletedTask;
        }
    }
}