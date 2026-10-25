using CAT.Messaging.Contracts;
using System.Globalization;
using CurtailmentIngestor.Configuration;
using CurtailmentIngestor.Contracts;
using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Interfaces;
using CurtailmentIngestor.Models;
using CurtailmentIngestor.Operations.Discovery;
using CurtailmentIngestor.Raw;
using CurtailmentIngestor.Repositories;
using CurtailmentIngestor.Serialization;
using CurtailmentIngestor.Validators;

namespace CurtailmentIngestor.Operations.Execution;

/// <summary>
/// Owns the single ordering rule for one bounded run: lease -&gt; discovery -&gt; Raw write ->
/// queue publication -&gt; fenced cursor commit. Runs one pass and returns; it is not a
/// long-lived poller. A discovered batch already carries the full current state of each
/// changed event (Section 4.3.1), so there is no separate Windfarm-Day read-back step. Unlike
/// <c>OutageIngestionRunner</c>, this runner also renews its lease in the background (as
/// <c>ProductionIngestionRunner</c> does), cancelling processing and reporting
/// <see cref="RunOutcome.Zombie"/> if renewal fails.
/// </summary>
public sealed class CurtailmentIngestionRunner
{
    private readonly IExtractionCursorRepository _cursorRepository;
    private readonly ICurtailmentSourceAdapter _sourceAdapter;
    private readonly CurtailmentBatchWriter _writer;
    private readonly IRawArtifactPublisher _publisher;
    private readonly CatchUpWindowPlanner _windowPlanner;
    private readonly ChangeDiscoveryReader _discoveryReader;
    private readonly IIngestionTelemetry _telemetry;
    private readonly IngestionOptions _options;
    private readonly string _ownerId;

    public CurtailmentIngestionRunner(
        IExtractionCursorRepository cursorRepository,
        ICurtailmentSourceAdapter sourceAdapter,
        CurtailmentBatchWriter writer,
        IRawArtifactPublisher publisher,
        CatchUpWindowPlanner windowPlanner,
        ChangeDiscoveryReader discoveryReader,
        IIngestionTelemetry telemetry,
        IngestionOptions options,
        string ownerId)
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
    }

    /// <summary>The source's own id is numeric; the shared cursor column is TEXT, so committed
    /// ids are parsed/compared as numbers here rather than lexically (e.g. "9" &gt; "10" as text).</summary>
    private static long ParseCommittedId(string value)
        => long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new InvalidDataException($"Committed cursor id '{value}' is not a valid Curtailment numeric id.");

    public async Task<RunResultModel> RunAsync(IngestionRequestModel request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WindfarmId);
        OperationCorrelation.Validate(request.OperationId);
        if (request.SourceType != "Curtailment" || !Enum.IsDefined(request.Mode))
            throw new ArgumentException("Unsupported ingestion source or mode.", nameof(request));
        IngestionOptionsValidator.ValidateAndThrow(_options);

        var lease = await _cursorRepository.TryAcquireLeaseAsync(
            request.WindfarmId, request.SourceType, _ownerId, _options.LeaseDuration, cancellationToken);

        if (lease is null)
        {
            return new RunResultModel(RunOutcome.LeaseHeldByOther, 0);
        }

        var runFinalized = new RunFinalizationState();
        var checkpoint = new CheckpointTracker { LastUpdatedDate = lease.LastUpdatedDate, LastCommittedId = lease.LastCommittedId };
        ExtractionRunModel? run = null;

        using var processing = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var renewalStop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var leaseLost = false;

        async Task RenewLeaseAsync()
        {
            try
            {
                while (!renewalStop.IsCancellationRequested)
                {
                    using var renewalAttempt = CancellationTokenSource.CreateLinkedTokenSource(renewalStop.Token);
                    renewalAttempt.CancelAfter(TimeSpan.FromTicks(_options.LeaseDuration.Ticks / 3));
                    if (!await _cursorRepository.TryRenewLeaseAsync(request.WindfarmId, request.SourceType,
                        _ownerId, lease.LeaseToken, _options.LeaseDuration, renewalAttempt.Token))
                    {
                        leaseLost = true;
                        await processing.CancelAsync();
                        return;
                    }
                    await Task.Delay(TimeSpan.FromTicks(_options.LeaseDuration.Ticks / 3), renewalStop.Token);
                }
            }
            catch (OperationCanceledException) when (renewalStop.IsCancellationRequested) { }
            catch (Exception error)
            {
                leaseLost = true;
                _telemetry.SourceContactFailed(request.WindfarmId, request.SourceType, error);
                await processing.CancelAsync();
            }
        }

        var renewal = RenewLeaseAsync();
        try
        {
            processing.Token.ThrowIfCancellationRequested();

            // Exactly RawPathBuilder.BuildBasePath: the run-owned prefix covering every shard this
            // run writes (there is no date segment to vary, unlike Production). Started inside this
            // try so a fencing/DB failure here still runs the finally below and releases the lease.
            var outputPathPrefix = RawPathBuilder.BuildBasePath(request.WindfarmId, lease.LeaseToken);
            run = await _cursorRepository.StartRunAsync(
                request.WindfarmId, request.SourceType, lease.LeaseToken,
                lease.LastUpdatedDate, lease.LastCommittedId, outputPathPrefix, processing.Token, request.OperationId);

            var result = request.Mode == IngestionMode.Refresh
                ? await RunRefreshAsync(request, lease, run, runFinalized, processing.Token)
                : await RunIncrementalAsync(request, lease, run, runFinalized, checkpoint, processing.Token);
            cancellationToken.ThrowIfCancellationRequested();
            return leaseLost ? result with { Outcome = RunOutcome.Zombie } : result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (leaseLost)
        {
            return new RunResultModel(RunOutcome.Zombie, 0);
        }
        catch (LeaseFencingException ex)
        {
            _telemetry.SourceContactFailed(request.WindfarmId, request.SourceType, ex);
            return new RunResultModel(RunOutcome.Zombie, 0, ex.Message);
        }
        catch (Exception ex)
        {
            _telemetry.SourceContactFailed(request.WindfarmId, request.SourceType, ex);
            return new RunResultModel(RunOutcome.Failed, 0, ex.Message);
        }
        finally
        {
            await renewalStop.CancelAsync();
            await renewal;
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            if (!runFinalized.Finalized && run is not null)
            {
                try
                {
                    await _cursorRepository.FailRunAsync(
                        run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                        partialUpdatedAt: checkpoint.LastUpdatedDate, partialCommittedId: checkpoint.LastCommittedId,
                        errorMessage: "Run terminated without reaching a normal completion/failure path (cancellation or unexpected exception).",
                        cleanup.Token);
                }
                catch (Exception error)
                {
                    _telemetry.SourceContactFailed(request.WindfarmId, request.SourceType, error);
                }
            }
            try
            {
                await _cursorRepository.ReleaseLeaseAsync(request.WindfarmId, request.SourceType, lease.LeaseToken, cleanup.Token);
            }
            catch (Exception error)
            {
                _telemetry.SourceContactFailed(request.WindfarmId, request.SourceType, error);
            }
        }
    }

    /// <summary>Mutable flag shared with the outer <see cref="RunAsync"/> so its cleanup does not
    /// double-finalize a run already completed/failed by the inner Incremental/Refresh path.</summary>
    private sealed class RunFinalizationState
    {
        public bool Finalized { get; set; }
    }

    /// <summary>Tracks the latest successfully committed checkpoint so the outer cleanup's audit
    /// write records real progress instead of the run's original (pre-run) lease snapshot.</summary>
    private sealed class CheckpointTracker
    {
        public DateTime LastUpdatedDate { get; set; }
        public string LastCommittedId { get; set; } = "0";
    }

    private async Task<RunResultModel> RunIncrementalAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, CheckpointTracker checkpoint, CancellationToken cancellationToken)
    {
        var highWatermark = await _sourceAdapter.GetHighWatermarkAsync(request.WindfarmId, cancellationToken);
        _telemetry.SourceContactSucceeded(request.WindfarmId, request.SourceType);

        var windows = _windowPlanner.PlanWindows(
            lease.LastUpdatedDate, _options.SafetyLagInterval, highWatermark, _options.MaxCatchUpWindowSize);

        var totalWritten = 0;
        var currentCommittedDate = lease.LastUpdatedDate;
        var currentCommittedId = ParseCommittedId(lease.LastCommittedId);

        foreach (var window in windows)
        {
            DiscoveryResult discovery;
            try
            {
                discovery = await _discoveryReader.DiscoverAsync(request.WindfarmId, window.Start, window.End, cancellationToken);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    currentCommittedDate, currentCommittedId.ToString(CultureInfo.InvariantCulture), ex.Message, cancellationToken);
                runFinalized.Finalized = true;
                return new RunResultModel(RunOutcome.Failed, totalWritten, ex.Message);
            }

            if (discovery.ChangedEvents.Count == 0)
            {
                // The safety-lag rewind can place an empty window below the committed cursor; never move it backwards.
                if (window.End > currentCommittedDate)
                {
                    var advanced = await _cursorRepository.TryCommitAsync(
                        request.WindfarmId, request.SourceType, lease.LeaseToken,
                        window.End, "0", cancellationToken, currentCommittedDate);

                    if (!advanced)
                    {
                        await _cursorRepository.FailRunAsync(
                            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                            currentCommittedDate, currentCommittedId.ToString(CultureInfo.InvariantCulture),
                            "Checkpoint commit was rejected (lease no longer held).", cancellationToken);
                        runFinalized.Finalized = true;
                        return new RunResultModel(RunOutcome.Zombie, totalWritten);
                    }

                    currentCommittedDate = window.End;
                    currentCommittedId = 0;
                    checkpoint.LastUpdatedDate = currentCommittedDate;
                    checkpoint.LastCommittedId = currentCommittedId.ToString(CultureInfo.InvariantCulture);
                    _telemetry.CursorAdvanced(request.WindfarmId, request.SourceType, currentCommittedDate, currentCommittedId);
                }

                continue;
            }

            RawWriteResultModel writeResult;
            try
            {
                writeResult = await _writer.WriteBatchAsync(request.WindfarmId, lease.LeaseToken, discovery.ChangedEvents, cancellationToken);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    currentCommittedDate, currentCommittedId.ToString(CultureInfo.InvariantCulture), ex.Message, cancellationToken);
                runFinalized.Finalized = true;
                return new RunResultModel(RunOutcome.Failed, totalWritten, ex.Message);
            }
            _telemetry.BatchWritten(request.WindfarmId, writeResult.RowCount, writeResult.QuarantinedEvents.Count);

            var message = new RawCurtailmentBatchAvailableV1
            {
                SourceType = request.SourceType,
                WindfarmId = request.WindfarmId,
                ExtractionToken = lease.LeaseToken,
                BlobPath = writeResult.BlobPath,
                ShardPaths = writeResult.ShardPaths?.ToArray(),
                RunId = request.RunId ?? Guid.NewGuid().ToString("N"),
                OperationId = OperationCorrelation.Require(run.OperationId),
            };

            await _publisher.PublishAsync(message, cancellationToken);
            totalWritten += writeResult.RowCount;

            if (discovery.LastObservedKey is not { } lastKey)
            {
                continue;
            }

            // Composite (LastUpdatedDate, Id) monotonicity: the lag window may re-read already-committed rows.
            var movesForward = lastKey.LastUpdatedDate > currentCommittedDate
                || (lastKey.LastUpdatedDate == currentCommittedDate && lastKey.Id > currentCommittedId);
            if (!movesForward)
            {
                continue;
            }

            var committed = await _cursorRepository.TryCommitAsync(
                request.WindfarmId, request.SourceType, lease.LeaseToken,
                lastKey.LastUpdatedDate, lastKey.Id.ToString(CultureInfo.InvariantCulture), cancellationToken, currentCommittedDate);

            if (!committed)
            {
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    currentCommittedDate, currentCommittedId.ToString(CultureInfo.InvariantCulture),
                    "Checkpoint commit was rejected (lease no longer held).", cancellationToken);
                runFinalized.Finalized = true;
                return new RunResultModel(RunOutcome.Zombie, totalWritten);
            }

            currentCommittedDate = lastKey.LastUpdatedDate;
            currentCommittedId = lastKey.Id;
            checkpoint.LastUpdatedDate = currentCommittedDate;
            checkpoint.LastCommittedId = currentCommittedId.ToString(CultureInfo.InvariantCulture);
            _telemetry.CursorAdvanced(request.WindfarmId, request.SourceType, currentCommittedDate, currentCommittedId);
        }

        var completed = await _cursorRepository.TryCompleteRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
            currentCommittedDate, currentCommittedId.ToString(CultureInfo.InvariantCulture), cancellationToken, currentCommittedDate);
        if (!completed)
        {
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                currentCommittedDate, currentCommittedId.ToString(CultureInfo.InvariantCulture),
                "Final run completion was rejected (lease no longer held).", cancellationToken);
            runFinalized.Finalized = true;
            return new RunResultModel(RunOutcome.Zombie, totalWritten);
        }

        runFinalized.Finalized = true;
        return new RunResultModel(RunOutcome.Success, totalWritten);
    }

    private async Task<RunResultModel> RunRefreshAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        var events = new List<CurtailmentEventModel>();
        try
        {
            await foreach (var evt in _sourceAdapter.GetAllCurrentEventsAsync(request.WindfarmId, cancellationToken))
            {
                events.Add(evt);
                if (events.Count > _options.MaxSnapshotEvents)
                {
                    throw new InvalidDataException("Refresh snapshot event limit exceeded; review the configured source bounds.");
                }
            }

            var writeResult = await _writer.WriteBatchAsync(request.WindfarmId, lease.LeaseToken, events, cancellationToken);
            _telemetry.BatchWritten(request.WindfarmId, writeResult.RowCount, writeResult.QuarantinedEvents.Count);

            var message = new RawCurtailmentBatchAvailableV1
            {
                SourceType = request.SourceType,
                WindfarmId = request.WindfarmId,
                ExtractionToken = lease.LeaseToken,
                BlobPath = writeResult.BlobPath,
                ShardPaths = writeResult.ShardPaths?.ToArray(),
                RunId = request.RunId ?? Guid.NewGuid().ToString("N"),
                OperationId = OperationCorrelation.Require(run.OperationId),
            };

            await _publisher.PublishAsync(message, cancellationToken);

            // Refresh re-pulls the full authoritative event set; it never advances the incremental cursor.
            var refreshCompleted = await _cursorRepository.TryCompleteRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                lease.LastUpdatedDate, lease.LastCommittedId, cancellationToken, lease.LastUpdatedDate);
            if (!refreshCompleted)
            {
                await _cursorRepository.FailRunAsync(
                    run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                    lease.LastUpdatedDate, lease.LastCommittedId, "Final run completion was rejected (lease no longer held).", cancellationToken);
                runFinalized.Finalized = true;
                return new RunResultModel(RunOutcome.Zombie, writeResult.RowCount);
            }
            runFinalized.Finalized = true;

            return new RunResultModel(RunOutcome.Success, writeResult.RowCount);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            await _cursorRepository.FailRunAsync(
                run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken,
                partialUpdatedAt: null, partialCommittedId: null, ex.Message, cancellationToken);
            runFinalized.Finalized = true;
            throw;
        }
    }
}

