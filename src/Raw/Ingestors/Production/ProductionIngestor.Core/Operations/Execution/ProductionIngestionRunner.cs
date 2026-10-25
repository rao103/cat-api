using CAT.Messaging.Contracts;
using ProductionIngestor.Adapters.Messaging;
using ProductionIngestor.Configuration;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Contracts;
using ProductionIngestor.Repositories;
using ProductionIngestor.Enums;
using ProductionIngestor.Operations.Discovery;
using ProductionIngestor.Operations.Execution.Changes;
using ProductionIngestor.Operations.Extraction;
using ProductionIngestor.Operations.Refresh;
using ProductionIngestor.Monitoring;
using ProductionIngestor.Raw;
using ProductionIngestor.Serialization;
using ProductionIngestor.Validators;
//using Amazon.RDS;
using Common.Constants;

namespace ProductionIngestor.Operations.Execution;

/// <summary>
/// Owns the single ordering rule for one bounded run: lease -&gt; discovery -&gt; extraction -&gt;
/// Raw write -&gt; queue publication -&gt; fenced cursor commit. Runs one pass and returns; it is
/// not a long-lived poller (the process hosting this exits after the run completes).
/// </summary>
public sealed class ProductionIngestionRunner
{
    private readonly IExtractionCursorRepository _cursorRepository;
    private readonly IProductionSourceAdapter _sourceAdapter;
    private readonly WindfarmDayExtractor _dayExtractor;
    private readonly ProductionParquetWriter _writer;
    private readonly IRawArtifactPublisher _publisher;
    private readonly ChangeDiscoveryReader _discoveryReader;
    private readonly IIngestionTelemetry<ProductionIngestionRunner> _telemetry;
    private readonly IngestionOptions _options;
    private readonly string _ownerId;
    private readonly IExtractionSchedulingRepository? _schedulingRepository;
    private readonly ProductionChangesIncrementalStrategy? _changesStrategy;
    private readonly IProductionScopeRepository? _scopeRepository;

    public ProductionIngestionRunner(
        IExtractionCursorRepository cursorRepository,
        IProductionSourceAdapter sourceAdapter,
        WindfarmDayExtractor dayExtractor,
        ProductionParquetWriter writer,
        IRawArtifactPublisher publisher,
        ChangeDiscoveryReader discoveryReader,
        IIngestionTelemetry<ProductionIngestionRunner> telemetry,
        IngestionOptions options,
        string ownerId,
        IExtractionSchedulingRepository? schedulingRepository = null,
        ProductionChangesIncrementalStrategy? changesStrategy = null,
        IProductionScopeRepository? scopeRepository = null)
    {
        _cursorRepository = cursorRepository;
        _sourceAdapter = sourceAdapter;
        _dayExtractor = dayExtractor;
        _writer = writer;
        _publisher = publisher;
        _discoveryReader = discoveryReader;
        _telemetry = telemetry;
        _options = options;
        _ownerId = ownerId;
        _schedulingRepository = schedulingRepository;
        _changesStrategy = changesStrategy;
        _scopeRepository = scopeRepository;
    }

    public Task<RunResultModel> RunAsync(IngestionRequestModel request, CancellationToken cancellationToken)
        => RunRequestAsync(request, null, cancellationToken);

    public Task<RunResultModel> RunClaimedAsync(ExtractionClaimModel claim, CancellationToken cancellationToken)
        => RunRequestAsync(claim.Request, claim, cancellationToken);

    private async Task<RunResultModel> RunRequestAsync(
        IngestionRequestModel request, ExtractionClaimModel? claim, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ValidateRequest(request);
        _telemetry.IngestionInformation(request.WindfarmName, request.SourceType, "Starting production ingestion run");

        var acquisition = await AcquireLeaseAndStartRunAsync(request, claim, cancellationToken);
        if (acquisition.EarlyResult is not null)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return acquisition.EarlyResult;
            }
            finally
            {
                if (acquisition.Lease is not null)
                    await ReleaseLeaseBestEffortAsync(request, acquisition.Lease);
            }
        }

        var persistedOperationId = CAT.Messaging.Contracts.OperationCorrelation.Require(acquisition.Run!.OperationId);
        if (request.OperationId is not null && request.OperationId != persistedOperationId)
            throw new InvalidDataException("Requested OperationId differs from the persisted extraction run.");
        request = request with { OperationId = persistedOperationId };
        using var operationScope = _telemetry.BeginScope(new Dictionary<string, object?> { ["OperationId"] = persistedOperationId });
        System.Diagnostics.Activity.Current?.SetTag("cat.operation.id", persistedOperationId);

        return await ExecuteRunWithLeaseRenewalAsync(
            request, acquisition.Lease!, acquisition.Run!, acquisition.OutputPathPrefix!, claim, cancellationToken);
    }

    private void ValidateRequest(IngestionRequestModel request)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WindfarmName);
        CAT.Messaging.Contracts.OperationCorrelation.Validate(request.OperationId);

        if (request.SourceType != SourceType.Production || !Enum.IsDefined(request.Mode))
            throw new ArgumentException("Unsupported ingestion source or mode.", nameof(request));

        if (request.Mode == IngestionMode.Refresh && request.ExplicitDates is not { Count: > 0 })
            throw new ArgumentException("Refresh requires at least one production date.", nameof(request));

        IngestionOptionsValidator.ValidateAndThrow(_options);
    }

    /// <summary>Result of acquiring the lease and creating the run row: either everything needed to
    /// proceed, or a terminal <see cref="RunResultModel"/> to return immediately (lease held by
    /// another owner, run-start failure, or a fenced/zombie run row).</summary>
    private sealed record LeaseAcquisitionResult(
        ExtractionLeaseModel? Lease, ExtractionRunModel? Run, string? OutputPathPrefix, RunResultModel? EarlyResult);

    private async Task<LeaseAcquisitionResult> AcquireLeaseAndStartRunAsync(
        IngestionRequestModel request, ExtractionClaimModel? claim, CancellationToken cancellationToken)
    {
        if (claim is not null)
            return new(claim.Lease, claim.Run, claim.Run.OutputPath, null);

        var lease = claim?.Lease ?? await _cursorRepository.TryAcquireLeaseAsync(
            request.WindfarmId, request.SourceType, _ownerId, _options.LeaseDuration, cancellationToken);

        if (lease is null)
        {
            return new LeaseAcquisitionResult(null, null, null, new RunResultModel(RunOutcome.LeaseHeldByOther, Array.Empty<DateOnly>()));
        }

        // Run-owned prefix; a logical audit identifier stored as `extraction_run.output_path`, not
        // a literal prefix of the artifact paths (see RawPathBuilder.BuildWindowExtractionPrefix).
        var outputPathPrefix = RawPathBuilder.BuildExtractionPrefix(lease.WindfarmName, request.Mode, lease.LeaseToken);

        ExtractionRunModel? run;
        try
        {
            run = await _cursorRepository.StartRunAsync(
                request.WindfarmId, request.SourceType, lease.LeaseToken,
                lease.LastUpdatedUtc, lease.LastCommittedId, outputPathPrefix, cancellationToken, request.OperationId);
        }
        catch (Exception ex)
        {
            // Start failed before any renewal loop began: release the lease here directly
            // rather than relying on a finally that never runs for this early-return path.
            _telemetry.Error($"Failed to start extraction run for windfarm {request.WindfarmId} and source {request.SourceType}.",
                ex, request.RunId);
            return new LeaseAcquisitionResult(lease, null, outputPathPrefix, new RunResultModel(RunOutcome.Failed, Array.Empty<DateOnly>(), ex.Message));
        }

        if (run is null)
        {
            // Fenced insert rejected: the lease was already reassigned/expired before the run
            // row could be created. Nothing to finalize; release defensively (likely a no-op).
            return new LeaseAcquisitionResult(lease, null, outputPathPrefix, new RunResultModel(RunOutcome.Zombie, Array.Empty<DateOnly>()));
        }

        return new LeaseAcquisitionResult(lease, run, outputPathPrefix, null);
    }

    private async Task ReleaseLeaseBestEffortAsync(IngestionRequestModel request, ExtractionLeaseModel lease)
    {
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _cursorRepository.ReleaseLeaseAsync(request.WindfarmId, request.SourceType, lease.LeaseToken, cleanup.Token);
        }
        catch (Exception releaseError)
        {
            _telemetry.Error($"Failed to release extraction lease for windfarm {request.WindfarmId} and source {request.SourceType}.",
                releaseError, request.RunId);
        }
    }

    private async Task<RunResultModel> ExecuteRunWithLeaseRenewalAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run, string outputPathPrefix,
        ExtractionClaimModel? claim, CancellationToken cancellationToken)
    {
        var runFinalized = new RunFinalizationState(lease.LastUpdatedUtc, lease.LastCommittedId)
        {
            Claimed = claim is not null
        };
        var result = new RunResultModel(RunOutcome.Failed, Array.Empty<DateOnly>());

        using var processing = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var renewalStop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewalGuard = new LeaseRenewalGuard(
            _cursorRepository, _telemetry, request.WindfarmId, request.WindfarmName, request.SourceType, claim?.OwnerId ?? _ownerId,
            lease.LeaseToken, _options.LeaseDuration, processing, renewalStop);

        try
        {
            try
            {
                processing.Token.ThrowIfCancellationRequested();
                result = request.Mode == IngestionMode.Refresh
                    ? await RunRefreshAsync(request, lease, run, outputPathPrefix, runFinalized, processing.Token)
                    : await RunIncrementalAsync(request, lease, run, outputPathPrefix, runFinalized, processing.Token);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                result = new RunResultModel(RunOutcome.Cancelled, runFinalized.PublishedDates.ToArray());
                throw;
            }
            catch (OperationCanceledException) when (renewalGuard.LeaseLost)
            {
                result = new RunResultModel(RunOutcome.Zombie, Array.Empty<DateOnly>());
            }
            catch (Exception error)
            {
                await FailRunOnUnhandledExceptionAsync(run, request, lease, runFinalized, error);
                result = new RunResultModel(RunOutcome.Failed, runFinalized.PublishedDates.ToArray(), error.GetType().Name);
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (renewalGuard.LeaseLost)
                return result with { Outcome = RunOutcome.Zombie };
            if (claim is not null)
            {
                await renewalGuard.DisposeAsync();
                if (_schedulingRepository is null || !await _schedulingRepository.TryFinalizeAsync(new(claim, result), cancellationToken))
                    return result with { Outcome = RunOutcome.Zombie };
                runFinalized.Finalized = true;
            }
            return result;
        }
        finally
        {
            await renewalGuard.DisposeAsync();
            if (claim is null)
                await FinalizeAbandonedRunAsync(run, request, lease, runFinalized, renewalGuard.LeaseLost);
            else if (!runFinalized.Finalized && _schedulingRepository is not null)
            {
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                try
                {
                    var terminal = result.Outcome == RunOutcome.Cancelled ? result : result with { Outcome = RunOutcome.Failed };
                    await _schedulingRepository.TryFinalizeAsync(new(claim, terminal), cleanup.Token);
                }
                catch (Exception error)
                {
                    _telemetry.Error($"Failed to finalize scheduled extraction attempt {claim.Run.RunId}.",
                        error, request.RunId);
                }
            }
        }
    }

    private async Task FailRunOnUnhandledExceptionAsync(
        ExtractionRunModel run, IngestionRequestModel request, ExtractionLeaseModel lease,
        RunFinalizationState runFinalized, Exception ex)
    {
        _telemetry.Error($"Failed extraction run {run.RunId} for windfarm {request.WindfarmId} and source {request.SourceType}.",
            ex, request.RunId);
        if (runFinalized.Finalized || runFinalized.Claimed)
        {
            return;
        }

        using var failCleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                runFinalized.LastCommittedUpdatedUtc, runFinalized.LastCommittedId, ex.Message, failCleanup.Token);
        }
        catch (Exception error)
        {
            _telemetry.Error($"Failed to persist failure state for extraction run {run.RunId}.", error, request.RunId);
        }
        runFinalized.Finalized = true;
    }

    /// <summary>Best-effort fail-and-release for a run that never reached a normal completion/failure
    /// path (cancellation, an unexpected exception already handled above, or lease loss). Shares one
    /// timeout window across both calls, matching the outer <c>finally</c>'s original behavior.</summary>
    private async Task FinalizeAbandonedRunAsync(
        ExtractionRunModel run, IngestionRequestModel request, ExtractionLeaseModel lease,
        RunFinalizationState runFinalized, bool leaseLost)
    {
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        if (!runFinalized.Finalized)
        {
            try
            {
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    runFinalized.LastCommittedUpdatedUtc, runFinalized.LastCommittedId,
                    leaseLost
                        ? "Run terminated after the lease was lost or reassigned to another owner during processing."
                        : "Run terminated without reaching a normal completion/failure path (cancellation or unexpected exception).",
                    cleanup.Token);
            }
            catch (Exception error)
            {
                _telemetry.Error($"Failed to finalize abandoned extraction run {run.RunId}.", error, request.RunId);
            }
        }

        try
        {
            await _cursorRepository.ReleaseLeaseAsync(request.WindfarmId, request.SourceType, lease.LeaseToken, cleanup.Token);
        }
        catch (Exception error)
        {
            _telemetry.Error($"Failed to release lease for abandoned extraction run {run.RunId}.", error, request.RunId);
        }
    }

    /// <summary>Owns the periodic lease-renewal loop for one run: renews at 1/3 of the lease
    /// duration, cancels the shared <c>processing</c> token the moment renewal is rejected or
    /// errors, and exposes whether that happened so the caller can downgrade its outcome to
    /// <see cref="RunOutcome.Zombie"/>.</summary>
    private sealed class LeaseRenewalGuard : IAsyncDisposable
    {
        private readonly IExtractionCursorRepository _cursorRepository;
        private readonly IIngestionTelemetry<ProductionIngestionRunner> _telemetry;
        private readonly int _windfarmId;
        private readonly string _windfarmName;
        private readonly string _sourceType;
        private readonly string _ownerId;
        private readonly long _leaseToken;
        private readonly TimeSpan _leaseDuration;
        private readonly CancellationTokenSource _processing;
        private readonly CancellationTokenSource _renewalStop;
        private readonly Task _renewalTask;

        public LeaseRenewalGuard(
            IExtractionCursorRepository cursorRepository, IIngestionTelemetry<ProductionIngestionRunner> telemetry,
            int windfarmId, string windfarmName, string sourceType, string ownerId, long leaseToken, TimeSpan leaseDuration,
            CancellationTokenSource processing, CancellationTokenSource renewalStop)
        {
            _cursorRepository = cursorRepository;
            _telemetry = telemetry;
            _windfarmId = windfarmId;
            _windfarmName = windfarmName;
            _sourceType = sourceType;
            _ownerId = ownerId;
            _leaseToken = leaseToken;
            _leaseDuration = leaseDuration;
            _processing = processing;
            _renewalStop = renewalStop;
            _renewalTask = RenewLoopAsync();
        }

        public bool LeaseLost { get; private set; }

        private async Task RenewLoopAsync()
        {
            try
            {
                while (!_renewalStop.IsCancellationRequested)
                {
                    using var renewalAttempt = CancellationTokenSource.CreateLinkedTokenSource(_renewalStop.Token);
                    renewalAttempt.CancelAfter(TimeSpan.FromTicks(_leaseDuration.Ticks / 3));
                    if (!await _cursorRepository.TryRenewLeaseAsync(
                        _windfarmId, _sourceType, _ownerId, _leaseToken, _leaseDuration, renewalAttempt.Token))
                    {
                        LeaseLost = true;
                        await _processing.CancelAsync();
                        return;
                    }
                    await Task.Delay(TimeSpan.FromTicks(_leaseDuration.Ticks / 3), _renewalStop.Token);
                }
            }
            catch (OperationCanceledException) when (_renewalStop.IsCancellationRequested) { }
            catch (Exception error)
            {
                LeaseLost = true;
                _telemetry.Error($"Lease renewal failed for windfarm {_windfarmId} and source {_sourceType}.", error);
                await _processing.CancelAsync();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _renewalStop.CancelAsync();
            await _renewalTask;
        }
    }

    /// <summary>Mutable state shared with the outer <see cref="RunAsync"/> so its cleanup does not
    /// double-finalize a run already completed/failed by the inner Incremental/Refresh path, and so
    /// an outer cancellation/exception fails the run with the last actually-committed progress
    /// rather than silently re-asserting the run's original from-snapshot as if nothing advanced.</summary>
    private sealed class RunFinalizationState
    {
        public RunFinalizationState(DateTime initialCommittedUpdatedUtc, string initialCommittedId)
        {
            LastCommittedUpdatedUtc = initialCommittedUpdatedUtc;
            LastCommittedId = initialCommittedId;
        }

        public bool Finalized { get; set; }
        public bool Claimed { get; init; }
        public List<DateOnly> PublishedDates { get; } = [];
        public DateTime LastCommittedUpdatedUtc { get; set; }
        public string LastCommittedId { get; set; }
    }

    private Task<RunResultModel> RunIncrementalAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run, string outputPathPrefix,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
        => _options.ProductionChangesEnabled
            ? RunProductionChangesIncrementalAsync(request, lease, run, runFinalized, cancellationToken)
            : RunDaySnapshotIncrementalAsync(request, lease, run, outputPathPrefix, runFinalized, cancellationToken);

    /// <summary>The day-snapshot incremental flow: discovers affected production dates directly from
    /// rows with `LastUpdatedUtc` strictly past the committed checkpoint (no upper bound, no
    /// separate MAX(LastUpdatedUtc) lookup against the source), processes them in order, and
    /// commits the checkpoint once as the maximum `LastUpdatedUtc` actually observed. Selected only
    /// when `Ingestion:ProductionChangesEnabled` is false.</summary>
    private async Task<RunResultModel> RunDaySnapshotIncrementalAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run, string outputPathPrefix,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        if (lease.LastUpdatedUtc == DateTime.MinValue || lease.LastUpdatedUtc == DateTime.MaxValue)
        {
            var error = new InvalidDataException("Production requires a finite initialized extraction checkpoint.");
            await FailRunOnUnhandledExceptionAsync(run, request, lease, runFinalized, error);
            return new RunResultModel(RunOutcome.Failed, Array.Empty<DateOnly>(), nameof(InvalidDataException));
        }

        // No upper bound on LastUpdatedUtc: a single unbounded discovery pass past the checkpoint
        // replaces the old MAX(LastUpdatedUtc)-derived catch-up windows.
        var noUpperBoundUtc = DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc);
        var discovery = await _discoveryReader.DiscoverAsync(request.WindfarmName, lease.LastUpdatedUtc, noUpperBoundUtc, cancellationToken);
        _telemetry.SourceContactSucceeded(request.WindfarmName, request.SourceType);

        if (discovery.MaxObservedLastUpdatedUtc is null)
        {
            // No-data run: nothing to advance, but the run itself still completes normally.
            return await CompleteRunAsync(
                request, lease, run, lease.LastUpdatedUtc, lease.LastCommittedId,
                Array.Empty<DateOnly>(), runFinalized, cancellationToken);
        }

        var written = new List<DateOnly>();
        foreach (var date in discovery.AffectedDates.OrderBy(date => date))
        {
            try
            {
                await ExtractWriteAndPublishDayAsync(request, lease, date, cancellationToken);
                written.Add(date);
                runFinalized.PublishedDates.Add(date);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                // Stop at the first failing day; never commit past it.
                _telemetry.Error($"Failed to extract production day {date:yyyy-MM-dd} for windfarm {request.WindfarmId} and source {request.SourceType}.",
                    ex, request.RunId);
                if (runFinalized.Claimed)
                    return new RunResultModel(RunOutcome.Failed, written, ex.GetType().Name);
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    lease.LastUpdatedUtc, lease.LastCommittedId, ex.Message, cancellationToken);
                runFinalized.Finalized = true;
                return new RunResultModel(RunOutcome.Failed, written, ex.Message);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();

        var newCommittedUpdatedUtc = discovery.MaxObservedLastUpdatedUtc.Value;
        // Day-level discovery only tracks affected dates, not a per-row identity; "0" preserves the
        // pre-existing committed-id convention rather than inventing a tie-break that isn't needed
        // at this granularity (see report for the alternative considered).
        const string newCommittedId = "0";
        runFinalized.LastCommittedUpdatedUtc = newCommittedUpdatedUtc;
        runFinalized.LastCommittedId = newCommittedId;
        _telemetry.CursorAdvanced(request.WindfarmName, request.SourceType, newCommittedUpdatedUtc, 0);

        return await CompleteRunAsync(
            request, lease, run, newCommittedUpdatedUtc, newCommittedId, written, runFinalized, cancellationToken,
            expectedLastUpdatedUtc: lease.LastUpdatedUtc);
    }

    /// <summary>
    /// The new windowed changed-row incremental flow (`Ingestion:ProductionChangesEnabled = true`):
    /// delegates window planning, extraction, and part publication to
    /// <see cref="ProductionChangesIncrementalStrategy"/>, then performs the single fenced
    /// whole-execution checkpoint/audit commit. Window tasks never touch `extraction_cursor`.
    /// </summary>
    private async Task<RunResultModel> RunProductionChangesIncrementalAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        if (_changesStrategy is null)
        {
            throw new InvalidOperationException(
                "Ingestion:ProductionChangesEnabled is true but no ProductionChangesIncrementalStrategy was configured.");
        }

        if (lease.LastUpdatedUtc == DateTime.MinValue)
        {
            // Typed failure, not a generic exception: the registration has never been seeded
            // with a finite checkpoint ('-infinity' in the schema), so there is no safe query start.
            const string message = "CheckpointNotSeeded: the extraction cursor has not been seeded with a finite checkpoint.";
            await FailChangesRunAsync(run, request, lease, runFinalized, message, lease.LastUpdatedUtc, lease.LastUpdatedUtc, cancellationToken);
            return new RunResultModel(RunOutcome.Failed, Array.Empty<DateOnly>(), "CheckpointNotSeeded",
                lease.LastUpdatedUtc, lease.LastUpdatedUtc);
        }

        var strategyRequest = new ChangesExecutionRequest(
		    request.WindfarmId,
		    lease.WindfarmName,
		    request.SourceType,
		    request.RunId ?? lease.LeaseToken.ToString(
		        System.Globalization.CultureInfo.InvariantCulture),
		    lease.LeaseToken,
		    lease.LastUpdatedUtc,
		    lease.LastCommittedId,
		    OperationId: request.OperationId);

        var strategyResult = await _changesStrategy.ExecuteAsync(strategyRequest, cancellationToken);
        runFinalized.PublishedDates.AddRange(strategyResult.PublishedDates);
        runFinalized.LastCommittedUpdatedUtc = strategyResult.NewCommittedUpdatedUtc;
        runFinalized.LastCommittedId = strategyResult.NewCommittedId;

        if (strategyResult.Outcome == ChangesOutcome.Failed)
        {
            var message = strategyResult.FailureMessage ?? "Windowed extraction failed.";
            await FailChangesRunAsync(run, request, lease, runFinalized, message,
                strategyResult.QueryStartInclusiveUtc, strategyResult.QueryEndExclusiveUtc, cancellationToken);
            return new RunResultModel(RunOutcome.Failed, runFinalized.PublishedDates.ToArray(), message,
                strategyResult.QueryStartInclusiveUtc, strategyResult.QueryEndExclusiveUtc);
        }

        return await CompleteChangesRunAsync(request, lease, run, strategyResult, runFinalized, cancellationToken);
    }

    /// <summary>Best-effort terminal audit write for a failed windowed attempt: checkpoint stays at
    /// its pre-execution value, query bounds are persisted for diagnostics. A no-op for a claimed
    /// (scheduled) run, whose finalization is owned by the outer scheduling-repository path.</summary>
    private async Task FailChangesRunAsync(
        ExtractionRunModel run, IngestionRequestModel request, ExtractionLeaseModel lease, RunFinalizationState runFinalized,
        string errorMessage, DateTime? queryStartInclusiveUtc, DateTime? queryEndExclusiveUtc, CancellationToken cancellationToken)
    {
        if (runFinalized.Claimed)
        {
            return;
        }

        using var failCleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                runFinalized.LastCommittedUpdatedUtc, runFinalized.LastCommittedId, errorMessage, failCleanup.Token,
                queryStartInclusiveUtc, queryEndExclusiveUtc);
        }
        catch (Exception error)
        {
            _telemetry.Error($"Failed to persist failure state for extraction run {run.RunId}.", error, request.RunId);
        }
        runFinalized.Finalized = true;
    }

    /// <summary>Single fenced whole-execution commit for a successful windowed attempt: advances the
    /// checkpoint to the strategy's verified non-regressing position and records query bounds and
    /// published dates on the same `extraction_run` row. Zero rows updated means the lease was lost
    /// (<see cref="RunOutcome.Zombie"/>); the lease itself is not released again here, matching the
    /// existing fenced-release convention used by the legacy day-snapshot path.</summary>
    private async Task<RunResultModel> CompleteChangesRunAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        ChangesExecutionResult strategyResult, RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        if (runFinalized.Claimed)
        {
            return new RunResultModel(RunOutcome.Success, runFinalized.PublishedDates.ToArray(), null,
                strategyResult.QueryStartInclusiveUtc, strategyResult.QueryEndExclusiveUtc,
                strategyResult.NewCommittedUpdatedUtc, strategyResult.NewCommittedId);
        }

        var completed = await _cursorRepository.TryCompleteRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
            strategyResult.NewCommittedUpdatedUtc, strategyResult.NewCommittedId, cancellationToken,
            expectedLastUpdatedUtc: lease.LastUpdatedUtc,
            queryStartInclusiveUtc: strategyResult.QueryStartInclusiveUtc,
            queryEndExclusiveUtc: strategyResult.QueryEndExclusiveUtc);

        if (!completed)
        {
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                strategyResult.NewCommittedUpdatedUtc, strategyResult.NewCommittedId,
                "Final run completion was rejected (lease no longer held).", cancellationToken,
                strategyResult.QueryStartInclusiveUtc, strategyResult.QueryEndExclusiveUtc);
            runFinalized.Finalized = true;
            return new RunResultModel(RunOutcome.Zombie, runFinalized.PublishedDates.ToArray(), null,
                strategyResult.QueryStartInclusiveUtc, strategyResult.QueryEndExclusiveUtc);
        }

        runFinalized.Finalized = true;
        return new RunResultModel(RunOutcome.Success, runFinalized.PublishedDates.ToArray(), null,
            strategyResult.QueryStartInclusiveUtc, strategyResult.QueryEndExclusiveUtc,
            strategyResult.NewCommittedUpdatedUtc, strategyResult.NewCommittedId);
    }

    private async Task<RunResultModel> RunRefreshAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run, string outputPathPrefix,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        var snapshotAsOfUtc = DateTime.UtcNow;
        var requestedDates = (request.ExplicitDates ?? Array.Empty<DateOnly>()).Distinct().Order().ToArray();
        _telemetry.Information(
            $"Planning Refresh for windfarm {request.WindfarmId} across {requestedDates.Length} requested production date(s).",
            request.RunId);
        if (_scopeRepository is null)
            throw new InvalidOperationException("Refresh requires the authoritative production scope repository.");
        var scope = await _scopeRepository.GetActiveScopeAsync(request.WindfarmId, DateTime.UtcNow, cancellationToken);
        if (scope is null || !string.Equals(scope.WindfarmName, lease.WindfarmName, StringComparison.Ordinal))
            throw new InvalidOperationException("Refresh requires the active windfarm scope to match the extraction lease.");
        var scopeItemIds = scope.ActiveTurbineNames.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        var plannedWindows = ProductionRefreshWindowPlanner.Plan(requestedDates);
        _telemetry.Information(
            $"Starting {plannedWindows.Count} Refresh window(s) for windfarm {request.WindfarmId} across {scopeItemIds.Length} active turbines. " +
            $"Output path: {outputPathPrefix}",
            request.RunId);
        var results = new (ProductionQueryWindowV1 Window, IReadOnlyList<ProductionArtifactReferenceV1> Artifacts)[plannedWindows.Count];
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, _options.MaxParallelWindows),
            CancellationToken = cancellationToken,
        };

        await Parallel.ForEachAsync(Enumerable.Range(0, plannedWindows.Count), parallelOptions, async (index, token) =>
        {
            results[index] = await ProcessRefreshWindowAsync(
                request, lease, run, plannedWindows[index], index + 1, scopeItemIds, scope.MinRetrieveDateUtc, token);
        });

        cancellationToken.ThrowIfCancellationRequested();
        var allArtifacts = results.SelectMany(result => result.Artifacts)
            .OrderBy(artifact => artifact.QueryWindowNumber)
            .ThenBy(artifact => artifact.PartNumber)
            .ToArray();
        var totalRows = results.Sum(result => result.Window.RowCount);
        _telemetry.Information(
            $"Refresh execution completed for windfarm {request.WindfarmId}: {totalRows} source row(s), " +
            $"{allArtifacts.Length} Parquet part(s) across {plannedWindows.Count} window(s).",
            request.RunId);
        var runId = request.RunId ?? run.RunId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var envelope = new RawArtifactAvailableV2
        {
            SourceType = request.SourceType,
            WindfarmId = request.WindfarmId,
            WindfarmName = lease.WindfarmName,
            RunId = runId, OperationId = request.OperationId,
            AttemptId = RawArtifactConventions.AttemptId(lease.LeaseToken),
            ExtractionToken = lease.LeaseToken,
            ExtractionMode = "Refresh",
            PayloadKind = "ProductionSnapshot",
            PayloadSchemaVersion = ProductionParquetSchemaV2.Version,
            QueryAxis = RawArtifactConventions.ProductionTimestampUtc,
            QueryStartInclusiveUtc = plannedWindows[0].QueryStartUtc,
            QueryEndExclusiveUtc = plannedWindows[^1].QueryEndExclusiveUtc,
            SnapshotScope = new RawSnapshotScopeV2(RawArtifactConventions.SelectedIntervals,
                plannedWindows[0].QueryStartUtc, plannedWindows[^1].QueryEndExclusiveUtc),
            ManifestPath = RawPathBuilder.BuildExtractionPrefix(lease.WindfarmName, IngestionMode.Refresh, lease.LeaseToken) + "snapshot/manifest.json",
            ManifestSchemaVersion = RawSnapshotManifestV1.CurrentSchemaVersion,
            RowCount = totalRows,
            TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString(),
        };
        var manifest = new RawSnapshotManifestV1
        {
            Envelope = envelope,
            SnapshotAsOfUtc = snapshotAsOfUtc,
            RequestedIntervals = plannedWindows.Select(window => new RawSnapshotIntervalV1(window.QueryStartUtc, window.QueryEndExclusiveUtc)).ToArray(),
            ScopeWtgNames = scopeItemIds,
            MinRetrieveDateUtc = scope.MinRetrieveDateUtc,
            CompletedWindows = results.Select(result => new RawSnapshotWindowV1(result.Window.WindowNumber,
                result.Window.QueryStartInclusiveUtc, result.Window.QueryEndExclusiveUtc, result.Window.RowCount,
                result.Window.ArtifactCount, result.Window.Completed)).ToArray(),
            Parts = allArtifacts.Select(part => new RawSnapshotPartV1(part.BlobPath, part.QueryWindowNumber, part.PartNumber, part.RowCount)).ToArray(),
        };
        await _writer.WriteSnapshotManifestAsync(manifest, cancellationToken);
        _telemetry.Information(
            $"Publishing refresh manifest for windfarm {request.WindfarmId}: {allArtifacts.Length} artifact(s), {totalRows} row(s).", request.RunId);
        await _publisher.PublishAsync(envelope, cancellationToken);
        var written = requestedDates.ToList();
        runFinalized.PublishedDates.AddRange(written);

        // Refresh never advances the incremental cursor; the run still completes with its
        // unchanged from-snapshot as the to-snapshot.
        return await CompleteRunAsync(
            request, lease, run, lease.LastUpdatedUtc, lease.LastCommittedId, written, runFinalized, cancellationToken);
    }

    private async Task<(ProductionQueryWindowV1 Window, IReadOnlyList<ProductionArtifactReferenceV1> Artifacts)> ProcessRefreshWindowAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        ProductionRefreshWindow window, int windowNumber, IReadOnlyList<string> scopeWtgNames,
        DateTime minRetrieveDateUtc, CancellationToken cancellationToken)
    {
        var selectedScope = new[] { ToDateRange(window.SelectedDates) };
        var artifacts = new List<ProductionArtifactReferenceV1>();
        var buffer = new List<ProductionRowModel>(_options.MaxRowsPerShard);
        long rowCount = 0;
        _telemetry.Information(
            $"Starting Refresh query window {windowNumber} for windfarm {request.WindfarmId}: " +
            $"{window.QueryStartUtc:O} to {window.QueryEndExclusiveUtc:O}.",
            request.RunId);

        async Task FlushAsync()
        {
            if (buffer.Count == 0) return;
            var parts = await _writer.WriteRefreshPartAsync(
                lease.WindfarmName, request.WindfarmId, lease.LeaseToken,
                request.RunId ?? run.RunId.ToString(System.Globalization.CultureInfo.InvariantCulture),
                RawArtifactConventions.AttemptId(lease.LeaseToken),
                payloadSchemaVersion: ProductionParquetSchemaV2.Version, windowNumber, window.QueryStartUtc, window.QueryEndExclusiveUtc,
                firstPartNumber: artifacts.Count + 1, selectedScope, buffer.ToArray(), cancellationToken);
            artifacts.AddRange(parts);
            buffer.Clear();
        }

        await foreach (var row in _sourceAdapter.GetRefreshSnapshotAsync(
            lease.WindfarmName, scopeWtgNames, minRetrieveDateUtc, window.QueryStartUtc, window.QueryEndExclusiveUtc, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            buffer.Add(row);
            rowCount++;
            if (buffer.Count >= _options.MaxRowsPerShard)
                await FlushAsync();
        }
        await FlushAsync();

        var progress = new ProductionQueryWindowV1(
            windowNumber, window.QueryStartUtc, window.QueryEndExclusiveUtc, selectedScope,
            rowCount, artifacts.Count, Completed: true);
        _telemetry.Information(
            $"Refresh query window {windowNumber} completed for windfarm {request.WindfarmId}: " +
            $"{rowCount} row(s), {artifacts.Count} part(s), {window.QueryStartUtc:O} to {window.QueryEndExclusiveUtc:O}.",
            request.RunId);
        return (progress, artifacts);
    }

    private static ProductionDateRangeV1 ToDateRange(IReadOnlyList<DateOnly> dates)
        => new(dates[0].ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
            dates[^1].ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture));

    private async Task ExtractWriteAndPublishDayAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, DateOnly date, CancellationToken cancellationToken)
    {
        var rows = new List<ProductionRowModel>();
        await foreach (var row in _dayExtractor.GetDeduplicatedSnapshotAsync(request.WindfarmName, date, cancellationToken))
            rows.Add(row);

        var writeResult = await _writer.WriteDayAsync(lease.WindfarmName, date, lease.LeaseToken, rows,
            cancellationToken, request.WindfarmId);
        _telemetry.DayWritten(request.WindfarmName, date, writeResult.RowCount, writeResult.QuarantinedRows.Count);
        await _publisher.PublishAsync(new RawArtifactAvailableV1
        {
            SourceType = request.SourceType,
            WindfarmId = request.WindfarmId,
            WindfarmName = request.WindfarmName,
            DataDate = date.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
            ExtractionToken = lease.LeaseToken,
            BlobPath = writeResult.BlobPath,
            ShardPaths = writeResult.ShardPaths?.ToArray(),
            OperationId = request.OperationId,
            RunId = request.RunId ?? lease.LeaseToken.ToString(System.Globalization.CultureInfo.InvariantCulture),
        }, cancellationToken);
    }

    /// <summary>Shared "commit final cursor snapshot, complete the run row" tail used by every
    /// terminal path (no-data incremental run, incremental completion after all windows, and
    /// Refresh, which always completes with its unchanged from-snapshot as the to-snapshot).</summary>
    private async Task<RunResultModel> CompleteRunAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        DateTime toCommittedUpdatedUtc, string toCommittedId, IReadOnlyList<DateOnly> written,
        RunFinalizationState runFinalized, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedUtc = null)
    {
        if (runFinalized.Claimed)
            return new RunResultModel(RunOutcome.Success, written, CompletedCheckpointUpdatedUtc: toCommittedUpdatedUtc,
                CompletedCheckpointId: toCommittedId);

        var completed = await _cursorRepository.TryCompleteRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
            toCommittedUpdatedUtc, toCommittedId, cancellationToken, expectedLastUpdatedUtc ?? toCommittedUpdatedUtc);

        if (!completed)
        {
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                toCommittedUpdatedUtc, toCommittedId, "Final run completion was rejected (lease no longer held).", cancellationToken);
            runFinalized.Finalized = true;
            return new RunResultModel(RunOutcome.Zombie, written);
        }

        runFinalized.Finalized = true;
        return new RunResultModel(RunOutcome.Success, written, CompletedCheckpointUpdatedUtc: toCommittedUpdatedUtc,
            CompletedCheckpointId: toCommittedId);
    }
}
