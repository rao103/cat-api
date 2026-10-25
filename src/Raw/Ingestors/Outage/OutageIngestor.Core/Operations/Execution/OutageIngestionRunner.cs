using OutageIngestor.Resolvers;
using System.Globalization;
using OutageIngestor.Configuration;
using OutageIngestor.Contracts;
using CAT.Messaging.Contracts;
using OutageIngestor.Enums;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Operations.Discovery;
using OutageIngestor.Raw;
using OutageIngestor.Serialization;
using OutageIngestor.Validators;

namespace OutageIngestor.Operations.Execution;

/// <summary>
/// Owns the single ordering rule for one bounded run: lease -&gt; run start -&gt; discovery -&gt;
/// Raw write -&gt; queue publication -&gt; fenced checkpoint -&gt; atomic run completion. Runs one pass
/// and returns; it is not a long-lived poller. Unlike <c>ProductionIngestionRunner</c>, a
/// discovered batch already carries the full current state of each changed event (Section
/// 4.3.1), so there is no separate Windfarm-Day read-back step.
/// <para>
/// Supports two entry points, mirroring ProductionIngestionRunner: <see cref="RunAsync"/> for a
/// single unclaimed invocation that owns the whole lease/run lifecycle itself, and
/// <see cref="RunClaimedAsync"/> for a lease/run already claimed by
/// <see cref="IExtractionSchedulingRepository"/>, whose finalization (cursor advance + run status)
/// uses the scheduling repository instead of the cursor repository.
/// </para>
/// </summary>
public sealed class OutageIngestionRunner
{
    private readonly IExtractionCursorRepository _cursorRepository;
    private readonly IOutageSourceAdapter _sourceAdapter;
    private readonly OutageBatchWriter _writer;
    private readonly IRawArtifactPublisher _publisher;
    private readonly CatchUpWindowPlanner _windowPlanner;
    private readonly ChangeDiscoveryReader _discoveryReader;
    private readonly IIngestionTelemetry<OutageIngestionRunner> _telemetry;
    private readonly IngestionOptions _options;
    private readonly string _ownerId;
    private readonly IOutageScopeRepository? _scopeRepository;
    private readonly IExtractionSchedulingRepository? _schedulingRepository;

    public OutageIngestionRunner(
        IExtractionCursorRepository cursorRepository,
        IOutageSourceAdapter sourceAdapter,
        OutageBatchWriter writer,
        IRawArtifactPublisher publisher,
        CatchUpWindowPlanner windowPlanner,
        ChangeDiscoveryReader discoveryReader,
        IIngestionTelemetry<OutageIngestionRunner> telemetry,
        IngestionOptions options,
        string ownerId,
        IOutageScopeRepository? scopeRepository = null,
        IExtractionSchedulingRepository? schedulingRepository = null)
    {
        _cursorRepository = cursorRepository;
        _sourceAdapter = sourceAdapter;
        _writer = writer;
        _publisher = publisher;
        _windowPlanner = windowPlanner;
        _discoveryReader = discoveryReader;
        _telemetry = telemetry;
        _options = options;
        _ownerId = ownerId;
        _scopeRepository = scopeRepository;
        _schedulingRepository = schedulingRepository;
    }

    /// <summary>Unclaimed entry point: acquires its own lease and owns the full lifecycle. Used
    /// for local/manual invocations that do not go through <see cref="ScheduledIngestionWorker"/>.</summary>
    public Task<RunResultModel> RunAsync(IngestionRequestModel request, CancellationToken cancellationToken)
        => RunRequestAsync(request, null, cancellationToken);

    /// <summary>Claimed entry point: the lease and `extraction_run` row already exist; finalization
    /// is performed through <see cref="IExtractionSchedulingRepository"/> before returning.</summary>
    public Task<RunResultModel> RunClaimedAsync(ExtractionClaimModel claim, CancellationToken cancellationToken)
        => RunRequestAsync(claim.Request, claim, cancellationToken);

    private async Task<RunResultModel> RunRequestAsync(
        IngestionRequestModel request, ExtractionClaimModel? claim, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ValidateRequest(request);

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
                if (claim is null && acquisition.Lease is not null)
                    await ReleaseLeaseBestEffortAsync(request, acquisition.Lease);
            }
        }

        var persistedOperationId = CAT.Messaging.Contracts.OperationCorrelation.Require(acquisition.Run!.OperationId);
        if (request.OperationId is not null && request.OperationId != persistedOperationId)
            throw new InvalidDataException("Requested OperationId differs from the persisted extraction run.");
        request = request with { OperationId = persistedOperationId };
        using var operationScope = _telemetry.BeginScope(new Dictionary<string, object?> { ["OperationId"] = persistedOperationId });
        System.Diagnostics.Activity.Current?.SetTag("cat.operation.id", persistedOperationId);

        return await ExecuteRunWithLeaseRenewalAsync(request, acquisition.Lease!, acquisition.Run!, claim, cancellationToken);
    }

    private void ValidateRequest(IngestionRequestModel request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(request.WindfarmId);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WindfarmName);
        CAT.Messaging.Contracts.OperationCorrelation.Validate(request.OperationId);
        if (request.SourceType != "Outage" || !Enum.IsDefined(request.Mode))
            throw new ArgumentException("Unsupported ingestion source or mode.", nameof(request));
        if (request.Mode == IngestionMode.Refresh)
        {
            if (request.RefreshScopeStartUtc is not { } start || request.RefreshScopeEndUtc is not { } end)
                throw new ArgumentException("Refresh requires an explicit RefreshScopeStartUtc/RefreshScopeEndUtc.", nameof(request));
            if (start.Kind != DateTimeKind.Utc || end.Kind != DateTimeKind.Utc || end <= start)
                throw new ArgumentException("RefreshScopeEndUtc must be strictly after RefreshScopeStartUtc.", nameof(request));
        }
        IngestionOptionsValidator.ValidateAndThrow(_options);
    }

    private sealed record LeaseAcquisitionResult(
        ExtractionLeaseModel? Lease, ExtractionRunModel? Run, RunResultModel? EarlyResult);

    private async Task<LeaseAcquisitionResult> AcquireLeaseAndStartRunAsync(
        IngestionRequestModel request, ExtractionClaimModel? claim, CancellationToken cancellationToken)
    {
        if (claim is not null)
            return new(claim.Lease, claim.Run, null);

        var lease = await _cursorRepository.TryAcquireLeaseAsync(
            request.WindfarmId, request.SourceType, _ownerId, _options.LeaseDuration, cancellationToken);

        if (lease is null)
            return new LeaseAcquisitionResult(null, null, new RunResultModel(RunOutcome.LeaseHeldByOther, 0));

        ExtractionRunModel? run;
        try
        {
            // Validate the authoritative name before constructing paths or contacting DART.
            // Never substitute the internal database ID or the request-supplied name.
            ArgumentException.ThrowIfNullOrWhiteSpace(lease.WindfarmName);
            var outputPathPrefix = RawPathBuilder.BuildBasePath(lease.WindfarmName, lease.LeaseToken, request.Mode);
            run = await _cursorRepository.StartRunAsync(
                request.WindfarmId, request.SourceType, lease.LeaseToken,
                lease.LastUpdatedUtc, lease.LastCommittedId, outputPathPrefix, cancellationToken, request.OperationId);
        }
        catch (Exception ex)
        {
            _telemetry.SourceContactFailed(lease.WindfarmName, request.SourceType, ex);
            return new LeaseAcquisitionResult(lease, null, new RunResultModel(RunOutcome.Failed, 0, ex.Message));
        }

        // A fenced insert returning null means the lease expired or was reassigned.
        // No run exists to fail, but the caller still releases the lease defensively.
        if (run is null)
            return new LeaseAcquisitionResult(lease, null, new RunResultModel(RunOutcome.Zombie, 0));

        return new LeaseAcquisitionResult(lease, run, null);
    }

    private async Task ReleaseLeaseBestEffortAsync(IngestionRequestModel request, ExtractionLeaseModel lease)
    {
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _cursorRepository.ReleaseLeaseAsync(request.WindfarmId, request.SourceType, lease.LeaseToken, cleanup.Token);
        }
        catch (Exception error)
        {
            _telemetry.SourceContactFailed(lease.WindfarmName, request.SourceType, error);
        }
    }

    private async Task<RunResultModel> ExecuteRunWithLeaseRenewalAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        ExtractionClaimModel? claim, CancellationToken cancellationToken)
    {
        request = request with { WindfarmName = lease.WindfarmName };
        var runFinalized = new RunFinalizationState(lease.LastUpdatedUtc, lease.LastCommittedId) { Claimed = claim is not null };
        var result = new RunResultModel(RunOutcome.Failed, 0);
        var claimFinalized = false;
        using var executionScope = _telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["RunId"] = request.RunId,
            ["AttemptId"] = run.RunId,
            ["SourceType"] = request.SourceType,
            ["WindfarmName"] = lease.WindfarmName
        });

        using var processing = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var renewalStop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewalGuard = new LeaseRenewalGuard(
            _cursorRepository, _telemetry, request.WindfarmId, lease.WindfarmName, request.SourceType, claim?.OwnerId ?? _ownerId,
            lease.LeaseToken, _options.LeaseDuration, processing, renewalStop);

        try
        {
            try
            {
                processing.Token.ThrowIfCancellationRequested();
                ArgumentException.ThrowIfNullOrWhiteSpace(lease.WindfarmName);
                _telemetry.IngestionInformation(lease.WindfarmName, request.SourceType, "Starting outage ingestion run");
                _telemetry.Information(
                    $"Lease claimed for windfarm {lease.WindfarmName}, source {request.SourceType}, token {lease.LeaseToken}; " +
                    $"incremental checkpoints: {lease.LastUpdatedUtc:O} / {lease.LastCommittedId} " +
                    $"(deleted feed {lease.DeletedLastUpdatedUtc:O} / {lease.DeletedLastCommittedId}). " +
                    $"Output path: {RawPathBuilder.BuildBasePath(lease.WindfarmName, lease.LeaseToken, request.Mode)}.",
                    request.RunId);
                result = request.Mode == IngestionMode.Refresh
                    ? await RunRefreshAsync(request, lease, run, runFinalized, processing.Token)
                    : await RunIncrementalAsync(request, lease, run, runFinalized, processing.Token);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                result = new RunResultModel(RunOutcome.Cancelled, 0);
                throw;
            }
            catch (OperationCanceledException) when (renewalGuard.LeaseLost)
            {
                result = new RunResultModel(RunOutcome.Zombie, 0);
            }
            catch (Repositories.LeaseFencingException ex)
            {
                _telemetry.SourceContactFailed(lease.WindfarmName, request.SourceType, ex);
                await FailRunOnUnhandledExceptionAsync(run, request, lease, runFinalized, ex);
                result = new RunResultModel(RunOutcome.Zombie, 0, ex.Message);
            }
            catch (Exception error)
            {
                await FailRunOnUnhandledExceptionAsync(run, request, lease, runFinalized, error);
                result = new RunResultModel(RunOutcome.Failed, 0, error.GetType().Name);
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (claim is not null)
            {
                // Stop renewal before finalization clears the lease, just as Production does.
                await renewalGuard.DisposeAsync();
                if (renewalGuard.LeaseLost || _schedulingRepository is null
                    || !await _schedulingRepository.TryFinalizeAsync(new(claim, result), cancellationToken))
                    return result with { Outcome = RunOutcome.Zombie };
                claimFinalized = true;
            }
            else if (renewalGuard.LeaseLost)
                return result with { Outcome = RunOutcome.Zombie };
            if (result.Outcome == RunOutcome.Success && request.Mode == IngestionMode.Incremental
                && result.CompletedCheckpointUpdatedUtc is { } checkpoint && result.CompletedCheckpointId is { } committedId)
                _telemetry.CursorAdvanced(lease.WindfarmName, request.SourceType, checkpoint, committedId);
            return result;
        }
        finally
        {
            await renewalGuard.DisposeAsync();
            if (claim is null)
                await FinalizeAbandonedRunAsync(run, request, lease, runFinalized, renewalGuard.LeaseLost);
            else if (!claimFinalized && _schedulingRepository is not null)
            {
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                try
                {
                    var terminal = result.Outcome == RunOutcome.Cancelled ? result : result with { Outcome = RunOutcome.Failed };
                    await _schedulingRepository.TryFinalizeAsync(new(claim, terminal), cleanup.Token);
                }
                catch (Exception error)
                {
                    _telemetry.Error($"Failed to finalize scheduled extraction attempt {claim.Run.RunId}.", error, request.RunId);
                }
            }
        }
    }

    private async Task FailRunOnUnhandledExceptionAsync(
        ExtractionRunModel run, IngestionRequestModel request, ExtractionLeaseModel lease,
        RunFinalizationState runFinalized, Exception ex)
    {
        _telemetry.Error($"Failed extraction run {run.RunId} for windfarm {request.WindfarmName} and source {request.SourceType}.",
            ex, request.RunId);
        if (runFinalized.Finalized || runFinalized.Claimed)
            return;

        using var failCleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                runFinalized.LastCheckpointUpdatedUtc, runFinalized.LastCheckpointCommittedId, ex.Message, failCleanup.Token);
        }
        catch (Exception error)
        {
            _telemetry.Error($"Failed to persist failure state for extraction run {run.RunId}.", error, request.RunId);
        }
        runFinalized.Finalized = true;
    }

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
                    runFinalized.LastCheckpointUpdatedUtc, runFinalized.LastCheckpointCommittedId,
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
        private readonly IIngestionTelemetry<OutageIngestionRunner> _telemetry;
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
            IExtractionCursorRepository cursorRepository, IIngestionTelemetry<OutageIngestionRunner> telemetry,
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
                _telemetry.Error($"Lease renewal failed for windfarm {_windfarmName} and source {_sourceType}.", error);
                await _processing.CancelAsync();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _renewalStop.CancelAsync();
            await _renewalTask;
        }
    }

    /// <summary>Mutable state shared with the outer run so its cleanup does not double-finalize a
    /// run already completed/failed by the inner Incremental/Refresh path, and tracks the latest
    /// actually-committed checkpoint so an unfinalized-run failure audit reports real partial
    /// progress. <see cref="Claimed"/> gates whether the inner paths finalize directly against
    /// <see cref="IExtractionCursorRepository"/> (unclaimed) or only report the reached checkpoint
    /// for the scheduling repository to finalize at the run boundary (claimed).</summary>
    private sealed class RunFinalizationState(DateTime initialCommittedUpdatedUtc, string initialCommittedId)
    {
        public bool Finalized { get; set; }
        public bool Claimed { get; init; }
        public DateTime LastCheckpointUpdatedUtc { get; set; } = initialCommittedUpdatedUtc;
        public string LastCheckpointCommittedId { get; set; } = initialCommittedId;
    }

    /// <summary>Shared "commit final cursor snapshot, complete the run row" tail. For a claimed
    /// run, finalization is deferred to the scheduling repository, so this only reports the
    /// checkpoint reached; for an unclaimed run it performs the fenced completion itself. Both
    /// the live/changed and deletion feed checkpoints are committed together in the one fenced
    /// update (Section 10) — never independently.</summary>
    private async Task<RunResultModel> CompleteRunAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        DateTime toCommittedUpdatedUtc, string toCommittedId, int eventsWritten,
        RunFinalizationState runFinalized, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedUtc = null,
        DateTime? toDeletedCommittedUpdatedUtc = null, string? toDeletedCommittedId = null,
        DateTime? expectedDeletedLastUpdatedUtc = null)
    {
        if (runFinalized.Claimed)
        {
            runFinalized.Finalized = true;
            return new RunResultModel(RunOutcome.Success, eventsWritten,
                CompletedCheckpointUpdatedUtc: toCommittedUpdatedUtc, CompletedCheckpointId: toCommittedId,
                CompletedDeletedCheckpointUpdatedUtc: toDeletedCommittedUpdatedUtc, CompletedDeletedCheckpointId: toDeletedCommittedId);
        }

        _telemetry.Information(
            $"Updating database checkpoint for windfarm {request.WindfarmId} and source {request.SourceType} to " +
            $"{toCommittedUpdatedUtc:O} / {toCommittedId} (deleted feed {toDeletedCommittedUpdatedUtc:O} / {toDeletedCommittedId}).",
            request.RunId);
        var completed = await _cursorRepository.TryCompleteRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
            toCommittedUpdatedUtc, toCommittedId, cancellationToken, expectedLastUpdatedUtc ?? toCommittedUpdatedUtc,
            toDeletedCommittedUpdatedUtc, toDeletedCommittedId, expectedDeletedLastUpdatedUtc);

        if (!completed)
        {
            _telemetry.Error(
                $"Database checkpoint update rejected (lease no longer held) for windfarm {request.WindfarmId} and source {request.SourceType}.",
                new InvalidOperationException("Final run completion was rejected."), request.RunId);
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                toCommittedUpdatedUtc, toCommittedId, "Final run completion was rejected (lease no longer held).", cancellationToken);
            runFinalized.Finalized = true;
            return new RunResultModel(RunOutcome.Zombie, eventsWritten);
        }

        runFinalized.Finalized = true;
        _telemetry.Information(
            $"Extraction run succeeded for windfarm {request.WindfarmId}; database updated, checkpoint now " +
            $"{toCommittedUpdatedUtc:O} / {toCommittedId} (deleted feed {toDeletedCommittedUpdatedUtc:O} / {toDeletedCommittedId}).",
            request.RunId);
        return new RunResultModel(RunOutcome.Success, eventsWritten,
            CompletedCheckpointUpdatedUtc: toCommittedUpdatedUtc, CompletedCheckpointId: toCommittedId,
            CompletedDeletedCheckpointUpdatedUtc: toDeletedCommittedUpdatedUtc, CompletedDeletedCheckpointId: toDeletedCommittedId);
    }

    /// <summary>Resolves retention and turbine membership once per run. Missing scope fails
    /// closed; tests must supply a deliberate scope rather than falling back to all turbines.</summary>
    private async Task<WindfarmScopeModel> ResolveScopeAsync(int windfarmId, CancellationToken cancellationToken)
    {
        if (_scopeRepository is null)
            throw new InvalidOperationException("Outage ingestion requires a scope repository.");

        var scope = await _scopeRepository.GetActiveScopeAsync(windfarmId, DateTime.UtcNow, cancellationToken)
            ?? throw new InvalidOperationException("Outage ingestion requires an active windfarm scope.");
        return scope with { ActiveTurbineNames = scope.ActiveTurbineNames.ToArray() };
    }

    /// <summary>
    /// Runs both independent Outage feeds for one incremental pass, in the fixed order the
    /// Incremental Refresh Spec requires (Section 5.2): live/changed feed first, deletion feed
    /// second, then a single fenced commit of both checkpoint pairs together (Section 10). If
    /// either feed fails, neither checkpoint pair advances — the next attempt replays from the
    /// previously committed checkpoints, which is safe because Raw is append-only and downstream
    /// merge is an idempotent keyed upsert (Section 11).
    /// </summary>
    private async Task<RunResultModel> RunIncrementalAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        var scope = await ResolveScopeAsync(request.WindfarmId, cancellationToken);
        var latestSourceUpdatedDate = await _sourceAdapter.GetLatestSourceUpdatedDateByWindfarmIdAsync(lease.WindfarmName, cancellationToken);
        _telemetry.SourceContactSucceeded(lease.WindfarmName, request.SourceType);

        var changedWindows = _windowPlanner.PlanWindows(
            lease.LastUpdatedUtc, _options.SafetyLagInterval, latestSourceUpdatedDate, _options.MaxCatchUpWindowSize);
        var deletedWindows = _windowPlanner.PlanWindows(
            lease.DeletedLastUpdatedUtc, _options.SafetyLagInterval, latestSourceUpdatedDate, _options.MaxCatchUpWindowSize);

        if (changedWindows.Count == 0 && deletedWindows.Count == 0)
        {
            // No-data run for both feeds: nothing to advance, but the run itself still completes normally.
            return await CompleteRunAsync(
                request, lease, run, lease.LastUpdatedUtc, lease.LastCommittedId, 0, runFinalized, cancellationToken,
                expectedLastUpdatedUtc: lease.LastUpdatedUtc,
                toDeletedCommittedUpdatedUtc: lease.DeletedLastUpdatedUtc, toDeletedCommittedId: lease.DeletedLastCommittedId,
                expectedDeletedLastUpdatedUtc: lease.DeletedLastUpdatedUtc);
        }

        var changed = await ProcessFeedAsync(
            request, lease, run, runFinalized, changedWindows, scope,
            (windfarmName, window, ct) => _discoveryReader.DiscoverChangedAsync(windfarmName, scope.ActiveTurbineNames, window.Start, window.End, scope.MinRetrieveDateUtc, ct),
            OutagePayloadKinds.OutageEvents, isDeletedFeed: false,
            lease.LastUpdatedUtc, lease.LastCommittedId, cancellationToken);
        if (changed.EarlyFailure is { } changedFailure)
            return changedFailure;

        var deleted = await ProcessFeedAsync(
            request, lease, run, runFinalized, deletedWindows, scope,
            (windfarmName, window, ct) => _discoveryReader.DiscoverDeletedAsync(windfarmName, scope.ActiveTurbineNames, window.Start, window.End, ct),
            OutagePayloadKinds.OutageDeletedEvents, isDeletedFeed: true,
            lease.DeletedLastUpdatedUtc, lease.DeletedLastCommittedId, cancellationToken);
        if (deleted.EarlyFailure is { } deletedFailure)
            return deletedFailure;

        _telemetry.Information(
            $"Next checkpoint for windfarm {request.WindfarmId} will advance to {changed.CommittedUpdatedUtc:O} / {changed.CommittedId} " +
            $"(deleted feed {deleted.CommittedUpdatedUtc:O} / {deleted.CommittedId}).",
            request.RunId);

        return await CompleteRunAsync(
            request, lease, run, changed.CommittedUpdatedUtc, changed.CommittedId,
            changed.EventsWritten + deleted.EventsWritten, runFinalized, cancellationToken,
            expectedLastUpdatedUtc: lease.LastUpdatedUtc,
            toDeletedCommittedUpdatedUtc: deleted.CommittedUpdatedUtc, toDeletedCommittedId: deleted.CommittedId,
            expectedDeletedLastUpdatedUtc: lease.DeletedLastUpdatedUtc);
    }

    /// <summary>Outcome of running every planned catch-up window of one feed. <see cref="EarlyFailure"/>
    /// is set only when a Raw write or publish failed, meaning the run must stop immediately without
    /// processing the other feed or committing any checkpoint.</summary>
    private sealed record FeedProcessingResult(
        int EventsWritten, DateTime CommittedUpdatedUtc, string CommittedId, RunResultModel? EarlyFailure);

    /// <summary>
    /// Discovers, writes and publishes every planned window of one feed (live/changed or
    /// deletion), advancing an in-memory checkpoint as it goes. Nothing is committed to
    /// <see cref="IExtractionCursorRepository"/> here — per Section 10, the two feeds' checkpoints
    /// are only ever committed together, once, by the caller after both feeds finish successfully.
    /// </summary>
    private async Task<FeedProcessingResult> ProcessFeedAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, IReadOnlyList<CatchUpWindow> windows, WindfarmScopeModel scope,
        Func<string, CatchUpWindow, CancellationToken, Task<DiscoveryResult>> discover,
        string payloadKind, bool isDeletedFeed,
        DateTime initialCommittedUpdatedUtc, string initialCommittedId,
        CancellationToken cancellationToken)
    {
        var eventsWritten = 0;
        var committedUpdatedUtc = initialCommittedUpdatedUtc;
        var committedId = initialCommittedId;

        for (var windowSequence = 0; windowSequence < windows.Count; windowSequence++)
        {
            var window = windows[windowSequence];
            var firstPartPath = isDeletedFeed
                ? RawPathBuilder.BuildDeletedEventsSingleArtifactPath(lease.WindfarmName, lease.LeaseToken, window.Start, window.End)
                : RawPathBuilder.BuildEventsSingleArtifactPath(lease.WindfarmName, lease.LeaseToken, window.Start, window.End);
            _telemetry.Information(
                $"Extraction in progress for windfarm {lease.WindfarmName}, source {request.SourceType}, payload {payloadKind}: " +
                $"window {windowSequence + 1} of {windows.Count} ({window.Start:O} to {window.End:O}). " +
                $"Planned first Parquet part (if valid rows exist): {firstPartPath}",
                request.RunId);
            var discovery = await discover(lease.WindfarmName, window, cancellationToken);

            if (discovery.ChangedEvents.Count == 0)
            {
                _telemetry.Information(
                    $"No rows for {payloadKind}, windfarm {lease.WindfarmName}, window {windowSequence + 1}; no Parquet file written.",
                    request.RunId);
                // No observed row: retain the checkpoint. The exclusive query horizon is not a source timestamp.
                continue;
            }

            RawWriteResultModel writeResult;
            try
            {
                writeResult = await _writer.WriteBatchAsync(lease.WindfarmName, lease.LeaseToken, window.Start, window.End, discovery.ChangedEvents.Select(evt => OutageIdentityResolver.Resolve(evt, scope)).ToArray(), cancellationToken);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                _telemetry.SourceContactFailed(lease.WindfarmName, request.SourceType, ex);
                if (!runFinalized.Claimed)
                    await _cursorRepository.FailRunAsync(
                        run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                        runFinalized.LastCheckpointUpdatedUtc, runFinalized.LastCheckpointCommittedId, ex.Message, cancellationToken);
                runFinalized.Finalized = true;
                return new FeedProcessingResult(eventsWritten, committedUpdatedUtc, committedId, new RunResultModel(RunOutcome.Failed, eventsWritten, ex.Message));
            }

            var (blobPath, shardPaths, rowCount) = isDeletedFeed
                ? (writeResult.DeletedBlobPath, writeResult.DeletedShardPaths, writeResult.DeletedRowCount)
                : (writeResult.BlobPath, writeResult.ShardPaths, writeResult.RowCount);

            // Every discovered row was quarantined: nothing durable to announce for this window,
            // but the window was still fully read, so its horizon still advances.
            if (blobPath is null && (shardPaths is null || shardPaths.Count == 0))
            {
                _telemetry.BatchWritten(lease.WindfarmName, 0, writeResult.QuarantinedEvents.Count);
                _telemetry.Information(
                    $"No valid rows for {payloadKind}, windfarm {lease.WindfarmName}, window {windowSequence + 1}; no feed Parquet file written.",
                    request.RunId);
                if (discovery.LastObservedKey is { } quarantinedKey && quarantinedKey.LastUpdatedDate >= committedUpdatedUtc)
                {
                    committedUpdatedUtc = quarantinedKey.LastUpdatedDate;
                    committedId = quarantinedKey.EventId;
                }
                continue;
            }

            _telemetry.BatchWritten(lease.WindfarmName, rowCount, writeResult.QuarantinedEvents.Count);
            if (blobPath is not null)
                LogParquetPath(request, payloadKind, blobPath);
            if (shardPaths is not null)
                foreach (var path in shardPaths)
                    LogParquetPath(request, payloadKind, path);

            try
            {
                var paths = shardPaths ?? (IReadOnlyList<string>)new[] { blobPath! };
                var counts = isDeletedFeed ? writeResult.DeletedPartRowCounts : writeResult.PartRowCounts;
                if (counts is null || counts.Count != paths.Count || counts.Sum() != rowCount)
                    throw new InvalidOperationException("Outage writer did not supply consistent per-part row counts.");
                var messages = new List<RawArtifactAvailableV2>();
                for (var part = 0; part < paths.Count; part++)
                {
                    var message = new RawArtifactAvailableV2
                    {
                        SourceType = request.SourceType,
                        WindfarmId = request.WindfarmId,
                        WindfarmName = lease.WindfarmName,
                        ExtractionToken = lease.LeaseToken,
                        AttemptId = RawArtifactConventions.AttemptId(lease.LeaseToken),
                        TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString(),
                        PayloadKind = payloadKind,
                        PayloadSchemaVersion = RawArtifactConventions.OutagePayloadSchemaVersion,
                        ExtractionMode = "Incremental",
                        QueryAxis = RawArtifactConventions.LastUpdatedUtc,
                        QueryStartInclusiveUtc = window.Start,
                        QueryEndExclusiveUtc = window.End,
                        QueryWindowNumber = windowSequence + 1,
                        PartNumber = part + 1,
                        BlobPath = paths[part],
                        RowCount = counts[part],
                        OperationId = request.OperationId,
            RunId = request.RunId ?? run.RunId.ToString(CultureInfo.InvariantCulture),
                    };
                    messages.Add(message);
                }
                if (!await _publisher.PublishBatchAsync(messages, cancellationToken))
                    throw new InvalidOperationException("Not all Outage parts were confirmed by SQS.");
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                _telemetry.SourceContactFailed(lease.WindfarmName, request.SourceType, ex);
                if (!runFinalized.Claimed)
                    await _cursorRepository.FailRunAsync(
                        run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                        runFinalized.LastCheckpointUpdatedUtc, runFinalized.LastCheckpointCommittedId, ex.Message, cancellationToken);
                runFinalized.Finalized = true;
                return new FeedProcessingResult(eventsWritten, committedUpdatedUtc, committedId, new RunResultModel(RunOutcome.Failed, eventsWritten, ex.Message));
            }

            eventsWritten += rowCount;

            if (discovery.LastObservedKey is { } lastKey && lastKey.LastUpdatedDate >= committedUpdatedUtc)
            {
                committedUpdatedUtc = lastKey.LastUpdatedDate;
                committedId = lastKey.EventId;
            }
        }

        return new FeedProcessingResult(eventsWritten, committedUpdatedUtc, committedId, null);
    }

    private void LogParquetPath(IngestionRequestModel request, string payloadKind, string path) =>
        _telemetry.Information(
            $"Parquet file written and verified durable for windfarm {request.WindfarmName}, source {request.SourceType}, payload {payloadKind}: {path}",
            request.RunId);

    private async Task<RunResultModel> RunRefreshAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        // Validated in ValidateRequest: both bounds are present and end is strictly after start.
        var scopeStartUtc = request.RefreshScopeStartUtc!.Value;
        var scopeEndUtc = request.RefreshScopeEndUtc!.Value;

        var scope = await ResolveScopeAsync(request.WindfarmId, cancellationToken);

        _telemetry.Information(
            $"Extraction in progress for windfarm {lease.WindfarmName}, source {request.SourceType}, payload {OutagePayloadKinds.OutageSnapshot}: " +
            $"scope {scopeStartUtc:O} to {scopeEndUtc:O}. Planned first Parquet part (if valid rows exist): " +
            $"{RawPathBuilder.BuildSnapshotPartPath(lease.WindfarmName, lease.LeaseToken, scopeStartUtc, scopeEndUtc, 1)}; " +
            $"planned manifest: {RawPathBuilder.BuildSnapshotManifestPath(lease.WindfarmName, lease.LeaseToken)}",
            request.RunId);
        var events = _sourceAdapter.GetAllCurrentEventsAsync(
            lease.WindfarmName, scope.ActiveTurbineNames, scope.MinRetrieveDateUtc, scopeStartUtc, scopeEndUtc, cancellationToken);
        var runId = request.RunId ?? run.RunId.ToString(CultureInfo.InvariantCulture);

        RawSnapshotWriteResultModel writeResult;
        try
        {
            writeResult = await _writer.WriteSnapshotAsync(
                request.WindfarmId, lease.WindfarmName, lease.LeaseToken,
                scopeStartUtc, scopeEndUtc, runId, OutageIdentityResolver.ResolveAsync(events, scope, cancellationToken), scope.ActiveTurbineNames, scope.MinRetrieveDateUtc, cancellationToken, request.OperationId);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _telemetry.SourceContactFailed(lease.WindfarmName, request.SourceType, ex);
            if (!runFinalized.Claimed)
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    lease.LastUpdatedUtc, lease.LastCommittedId, ex.Message, cancellationToken);
            runFinalized.Finalized = true;
            return new RunResultModel(RunOutcome.Failed, 0, ex.Message);
        }

        _telemetry.SourceContactSucceeded(lease.WindfarmName, request.SourceType);
        _telemetry.BatchWritten(lease.WindfarmName, writeResult.RowCount, writeResult.QuarantinedEvents.Count);
        foreach (var path in writeResult.PartPaths)
            LogParquetPath(request, OutagePayloadKinds.OutageSnapshot, path);
        _telemetry.Information(
            $"Snapshot manifest written and verified durable for windfarm {lease.WindfarmName}, source {request.SourceType}: " +
            $"{writeResult.ManifestPath}; {writeResult.PartPaths.Count} Parquet part(s).",
            request.RunId);

        // Refresh reads the scoped live table and never advances an incremental checkpoint.
        // The manifest declares complete extraction, not source-version evidence for deleting
        // missing events. Fan-out emits a receipt barrier until such evidence is available.
        var message = new RawArtifactAvailableV2
        {
            SourceType = request.SourceType,
            WindfarmId = request.WindfarmId,
            WindfarmName = lease.WindfarmName,
            ExtractionToken = lease.LeaseToken,
            AttemptId = RawArtifactConventions.AttemptId(lease.LeaseToken),
            TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString(),
            PayloadKind = OutagePayloadKinds.OutageSnapshot,
            PayloadSchemaVersion = RawArtifactConventions.OutagePayloadSchemaVersion,
            ExtractionMode = "Refresh",
            QueryAxis = RawArtifactConventions.EventOverlapUtc,
            QueryStartInclusiveUtc = scopeStartUtc,
            QueryEndExclusiveUtc = scopeEndUtc,
            SnapshotScope = new RawSnapshotScopeV2(RawArtifactConventions.EventsOverlappingInterval, scopeStartUtc, scopeEndUtc),
            RowCount = writeResult.RowCount,
            ManifestPath = writeResult.ManifestPath,
            ManifestSchemaVersion = RawSnapshotManifestV1.CurrentSchemaVersion,
            RunId = runId, OperationId = request.OperationId,
        };

        try
        {
            await _publisher.PublishAsync(message, cancellationToken);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _telemetry.SourceContactFailed(lease.WindfarmName, request.SourceType, ex);
            if (!runFinalized.Claimed)
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    lease.LastUpdatedUtc, lease.LastCommittedId, ex.Message, cancellationToken);
            runFinalized.Finalized = true;
            return new RunResultModel(RunOutcome.Failed, writeResult.RowCount, ex.Message);
        }

        // Refresh re-pulls the requested scope's authoritative event set; it never advances the
        // incremental cursor, but the run still completes with its unchanged from-snapshot as
        // the to-snapshot.
        return await CompleteRunAsync(
            request, lease, run, lease.LastUpdatedUtc, lease.LastCommittedId, writeResult.RowCount, runFinalized, cancellationToken);
    }
}
