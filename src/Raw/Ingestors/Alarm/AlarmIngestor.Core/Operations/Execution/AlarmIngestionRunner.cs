using CAT.Messaging.Contracts;
using System.Globalization;
using AlarmIngestor.Configuration;
using AlarmIngestor.Contracts;
using AlarmIngestor.Enums;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;
using AlarmIngestor.Operations.Discovery;
using AlarmIngestor.Operations.Polling;
using AlarmIngestor.Raw;
using AlarmIngestor.Serialization;
using AlarmIngestor.Validators;

namespace AlarmIngestor.Operations.Execution;

/// <summary>
/// Owns the single ordering rule for one bounded run: lease -&gt; freeze every configured
/// turbine's discovery upper bound up front -&gt; per-turbine discovery (all starting from the
/// same OLD global checkpoint) -&gt; Raw write -&gt; publication -&gt; fenced open-alarm enrollment
/// -&gt; known-open polling -&gt; fenced open-alarm removal -&gt; ONE global checkpoint advance,
/// fenced together with marking the run Completed. The committed AlarmId checkpoint is a single
/// value per windfarm (Section "global incremental AlarmId checkpoint per windfarm") — turbine
/// identity never affects it, and no turbine's successful advance is ever committed while
/// another configured turbine has not also completed successfully in the same run. Runs one pass
/// and returns; scheduling is external. Historical mode never advances the discovery checkpoint
/// and is not an authoritative snapshot (Section "Historical Retrieval").
/// </summary>
public sealed class AlarmIngestionRunner
{
    private readonly IExtractionCursorRepository _cursorRepository;
    private readonly IAlarmSourceAdapter _sourceAdapter;
    private readonly AlarmDiscoveryReader _discoveryReader;
    private readonly KnownOpenAlarmPoller _poller;
    private readonly AlarmBatchWriter _writer;
    private readonly IRawArtifactPublisher _publisher;
    private readonly IIngestionTelemetry _telemetry;
    private readonly IngestionOptions _options;
    private readonly string _ownerId;

    public AlarmIngestionRunner(
        IExtractionCursorRepository cursorRepository,
        IAlarmSourceAdapter sourceAdapter,
        AlarmDiscoveryReader discoveryReader,
        KnownOpenAlarmPoller poller,
        AlarmBatchWriter writer,
        IRawArtifactPublisher publisher,
        IIngestionTelemetry telemetry,
        IngestionOptions options,
        string ownerId)
    {
        _cursorRepository = cursorRepository;
        _sourceAdapter = sourceAdapter;
        _discoveryReader = discoveryReader;
        _poller = poller;
        _writer = writer;
        _publisher = publisher;
        _telemetry = telemetry;
        _options = options;
        _ownerId = ownerId;
    }

    /// <summary>Tracks whether the inner Incremental/Historical path already finalized (Completed
    /// or Failed) the run row, so the outer cleanup does not double-finalize it.</summary>
    private sealed class RunFinalizationState
    {
        public bool Finalized { get; set; }
    }

    public async Task<RunResultModel> RunAsync(IngestionRequestModel request, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WindfarmId);
        OperationCorrelation.Validate(request.OperationId);
        if (request.SourceType != "Alarm" || !Enum.IsDefined(request.Mode))
            throw new ArgumentException("Unsupported ingestion source or mode.", nameof(request));
        IngestionOptionsValidator.ValidateAndThrow(_options);

        var windfarm = _options.Windfarms.FirstOrDefault(w => w.WindfarmId == request.WindfarmId)
            ?? throw new ArgumentException($"No Ingestion:Windfarms configuration for '{request.WindfarmId}'.", nameof(request));

        var lease = await _cursorRepository.TryAcquireLeaseAsync(
            request.WindfarmId, request.SourceType, _ownerId, _options.LeaseDuration, cancellationToken);

        if (lease is null)
        {
            return new RunResultModel(RunOutcome.LeaseHeldByOther, 0);
        }

        var outputPrefix = RawPathBuilder.BuildRunPrefix(request.WindfarmId, lease.LeaseToken);
        var runFinalized = new RunFinalizationState();
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
                    await Task.Delay(TimeSpan.FromTicks(_options.LeaseDuration.Ticks / 3), renewalStop.Token);

                    using var renewalAttempt = CancellationTokenSource.CreateLinkedTokenSource(renewalStop.Token);
                    renewalAttempt.CancelAfter(TimeSpan.FromTicks(_options.LeaseDuration.Ticks / 3));

                    bool renewed;
                    try
                    {
                        renewed = await _cursorRepository.TryRenewLeaseAsync(
                            request.WindfarmId, request.SourceType, _ownerId, lease.LeaseToken, _options.LeaseDuration, renewalAttempt.Token);
                    }
                    catch (OperationCanceledException) when (!renewalStop.IsCancellationRequested)
                    {
                        renewed = false;
                    }

                    if (!renewed)
                    {
                        leaseLost = true;
                        await processing.CancelAsync();
                        return;
                    }
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
            run = await _cursorRepository.StartRunAsync(
                request.WindfarmId, request.SourceType, lease.LeaseToken, lease.LastCommittedId, outputPrefix, processing.Token, request.OperationId);
            var result = request.Mode == IngestionMode.Historical
                ? await RunHistoricalAsync(request, windfarm, lease, run, runFinalized, processing.Token)
                : await RunIncrementalAsync(request, windfarm, lease, run, runFinalized, processing.Token);
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
                        run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken, lease.LastCommittedId,
                        "Run terminated without reaching a normal completion/failure path (cancellation or unexpected exception).",
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

    private static long ParseCommittedAlarmId(string value)
        => long.Parse(value, NumberStyles.None, CultureInfo.InvariantCulture);

    private async Task<RunResultModel> FailAndReturnAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run, RunFinalizationState runFinalized,
        int totalWritten, string errorMessage, CancellationToken cancellationToken)
    {
        _telemetry.SourceContactFailed(request.WindfarmId, request.SourceType, new InvalidOperationException(errorMessage));
        await _cursorRepository.FailRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken, lease.LastCommittedId, errorMessage, cancellationToken);
        runFinalized.Finalized = true;
        return new RunResultModel(RunOutcome.Failed, totalWritten, errorMessage);
    }

    private async Task<RunResultModel> ZombieAndReturnAsync(
        IngestionRequestModel request, ExtractionLeaseModel lease, ExtractionRunModel run, RunFinalizationState runFinalized,
        int totalWritten, string reason, CancellationToken cancellationToken)
    {
        await _cursorRepository.FailRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken, lease.LastCommittedId, reason, cancellationToken);
        runFinalized.Finalized = true;
        return new RunResultModel(RunOutcome.Zombie, totalWritten);
    }

    /// <summary>
    /// Bounded, page/batch-scoped incremental run. A SINGLE global cutoff — the MAX of every
    /// configured turbine's own max AlarmId — is frozen up front (before any turbine starts
    /// paging), and every turbine discovers from the SAME old global checkpoint up to that SAME
    /// cutoff. Per-page open-alarm enrollment is fenced and applied immediately (it never touches
    /// the checkpoint id), but the global checkpoint itself only ever advances once, at the very
    /// end, and only after every configured turbine's discovery has fully succeeded — a failure
    /// partway through turbine B can never leave turbine A's higher progress committed while
    /// turbine B's lower ids are skipped on retry, and no per-turbine bound frozen at a different
    /// time can miss new low ids in an earlier turbine.
    /// </summary>
    private async Task<RunResultModel> RunIncrementalAsync(
        IngestionRequestModel request, WindfarmAlarmOptions windfarm, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        var runId = request.RunId ?? Guid.NewGuid().ToString("N");
        var afterAlarmId = ParseCommittedAlarmId(lease.LastCommittedId);
        var turbineRdspps = windfarm.Turbines.Select(t => t.TurbineRdspp).ToList();

        var totalWritten = 0;
        var windowSequence = 0;

        // Gather every turbine's own max AlarmId, then freeze ONE global MAX cutoff across all
        // of them BEFORE any turbine starts paging; every turbine discovers up to this SAME
        // cutoff (Section "single coherent frozen upper: gather maxima -> one global MAX cutoff").
        var perTurbineMaxima = new Dictionary<string, long?>();
        foreach (var turbine in windfarm.Turbines)
        {
            perTurbineMaxima[turbine.TurbineRdspp] = await _sourceAdapter.GetMaxAlarmIdAsync(
                turbine.TurbineRdspp, windfarm.MinRetrievalDateUtc, windfarm.AlarmCodes, cancellationToken);
        }

        long? globalUpperBound = perTurbineMaxima.Values.Any(b => b.HasValue)
            ? perTurbineMaxima.Values.Where(b => b.HasValue).Select(b => b!.Value).Max()
            : null;

        foreach (var turbine in windfarm.Turbines)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await foreach (var page in _discoveryReader.DiscoverTurbinePagesAsync(
                request.WindfarmId, turbine, afterAlarmId, globalUpperBound, windfarm.MinRetrievalDateUtc,
                windfarm.AlarmCodes, _options.SourceQueryPageSize, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();
                _telemetry.SourceContactSucceeded(request.WindfarmId, request.SourceType);

                var seq = windowSequence++;
                RawWriteResultModel writeResult;
                try
                {
                    writeResult = await _writer.WriteBatchAsync(request.WindfarmId, lease.LeaseToken, seq, page, cancellationToken);
                }
                catch (Exception ex)
                {
                    return await FailAndReturnAsync(request, lease, run, runFinalized, totalWritten, ex.Message, cancellationToken);
                }
                _telemetry.BatchWritten(request.WindfarmId, writeResult.RowCount, writeResult.QuarantinedEvents.Count);

                if (writeResult.PublishedEvents.Count == 0)
                {
                    continue;
                }

                try
                {
                    await _publisher.PublishAsync(new RawAlarmBatchAvailableV1
                    {
                        SourceType = request.SourceType,
                        WindfarmId = request.WindfarmId,
                        ExtractionToken = lease.LeaseToken,
                        WindowSequence = seq,
                        BlobPath = writeResult.BlobPath,
                        ShardPaths = writeResult.ShardPaths?.ToArray(),
                        RunId = runId,
                        OperationId = OperationCorrelation.Require(run.OperationId),
                        BatchKind = "Discovery",
                    }, cancellationToken);
                }
                catch (Exception ex)
                {
                    return await FailAndReturnAsync(request, lease, run, runFinalized, totalWritten, ex.Message, cancellationToken);
                }

                totalWritten += writeResult.RowCount;

                var newOpenAlarms = writeResult.PublishedEvents
                    .Where(e => e.EndTimeUtc is null && !e.IsTombstone)
                    .Select(e => (turbine.TurbineRdspp, e.AlarmId))
                    .ToList();

                if (newOpenAlarms.Count > 0)
                {
                    var enrolled = await _cursorRepository.TryEnrollOpenAlarmsAsync(
                        request.WindfarmId, lease.LeaseToken, newOpenAlarms, cancellationToken);
                    if (!enrolled)
                    {
                        return await ZombieAndReturnAsync(
                            request, lease, run, runFinalized, totalWritten,
                            "Open-alarm enrollment was rejected (lease no longer held).", cancellationToken);
                    }
                }
            }
        }

        // Poll durable known-open IDs every run, even when nothing new was discovered. The DB
        // enumeration itself is bounded (fail-closed) per turbine.
        var knownOpen = await _cursorRepository.GetKnownOpenAlarmsAsync(
            request.WindfarmId, turbineRdspps, _options.MaxOpenAlarmsPerTurbine, cancellationToken);

        foreach (var turbine in windfarm.Turbines)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var openIds = knownOpen[turbine.TurbineRdspp];
            if (openIds.Count == 0)
            {
                continue;
            }

            await foreach (var batch in _poller.PollBatchesAsync(request.WindfarmId, turbine, openIds, cancellationToken))
            {
                if (batch.Count == 0)
                {
                    continue;
                }

                var seq = windowSequence++;
                RawWriteResultModel writeResult;
                try
                {
                    writeResult = await _writer.WriteBatchAsync(request.WindfarmId, lease.LeaseToken, seq, batch, cancellationToken);
                }
                catch (Exception ex)
                {
                    return await FailAndReturnAsync(request, lease, run, runFinalized, totalWritten, ex.Message, cancellationToken);
                }
                _telemetry.BatchWritten(request.WindfarmId, writeResult.RowCount, writeResult.QuarantinedEvents.Count);

                if (writeResult.PublishedEvents.Count == 0)
                {
                    continue;
                }

                try
                {
                    await _publisher.PublishAsync(new RawAlarmBatchAvailableV1
                    {
                        SourceType = request.SourceType,
                        WindfarmId = request.WindfarmId,
                        ExtractionToken = lease.LeaseToken,
                        WindowSequence = seq,
                        BlobPath = writeResult.BlobPath,
                        ShardPaths = writeResult.ShardPaths?.ToArray(),
                        RunId = runId,
                        OperationId = OperationCorrelation.Require(run.OperationId),
                        BatchKind = "Polling",
                    }, cancellationToken);
                }
                catch (Exception ex)
                {
                    return await FailAndReturnAsync(request, lease, run, runFinalized, totalWritten, ex.Message, cancellationToken);
                }

                totalWritten += writeResult.RowCount;

                var resolved = writeResult.PublishedEvents
                    .Where(e => e.EndTimeUtc is not null || e.IsTombstone)
                    .Select(e => (turbine.TurbineRdspp, e.AlarmId))
                    .ToList();

                if (resolved.Count > 0)
                {
                    var removed = await _cursorRepository.TryRemoveResolvedOpenAlarmsAsync(
                        request.WindfarmId, lease.LeaseToken, resolved, cancellationToken);
                    if (!removed)
                    {
                        return await ZombieAndReturnAsync(
                            request, lease, run, runFinalized, totalWritten,
                            "Resolved open-alarm removal was rejected (lease no longer held).", cancellationToken);
                    }
                }
            }
        }

        // Every configured turbine's discovery succeeded, so the single frozen global cutoff is
        // now known to have been read through for every turbine (a turbine with no data below it
        // simply ended its paging early — that is not a gap; see AlarmDiscoveryReader). Commit
        // that same cutoff as the new global checkpoint.
        var newCommittedAlarmId = globalUpperBound is { } bound && bound > afterAlarmId ? bound : afterAlarmId;
        var newCommittedId = newCommittedAlarmId.ToString(CultureInfo.InvariantCulture);

        var completed = await _cursorRepository.TryCompleteRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken, newCommittedId, cancellationToken);
        if (!completed)
        {
            return await ZombieAndReturnAsync(
                request, lease, run, runFinalized, totalWritten,
                "Final run completion was rejected (lease no longer held).", cancellationToken);
        }

        runFinalized.Finalized = true;
        _telemetry.CursorAdvanced(request.WindfarmId, newCommittedAlarmId);
        return new RunResultModel(RunOutcome.Success, totalWritten);
    }

    /// <summary>
    /// Bounded historical retrieval: each turbine's start-time range is streamed page-at-a-time
    /// and each page is written/published/enrolled independently. The global checkpoint is NEVER
    /// advanced here (Section "Historical no cursor advance"); the run still completes normally,
    /// including when nothing at all was found ("no-data completes").
    /// </summary>
    private async Task<RunResultModel> RunHistoricalAsync(
        IngestionRequestModel request, WindfarmAlarmOptions windfarm, ExtractionLeaseModel lease, ExtractionRunModel run,
        RunFinalizationState runFinalized, CancellationToken cancellationToken)
    {
        if (request.HistoricalStartUtc is not { } startUtc || request.HistoricalEndUtc is not { } endUtc)
        {
            throw new ArgumentException("Historical mode requires HistoricalStartUtc and HistoricalEndUtc.", nameof(request));
        }

        var runId = request.RunId ?? Guid.NewGuid().ToString("N");
        var totalWritten = 0;
        var windowSequence = 0;

        foreach (var turbine in windfarm.Turbines)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var offset = 0;
            long? lastAlarmId = null;

            while (true)
            {
                var page = await _sourceAdapter.GetAlarmsBetweenDatesAsync(
                    request.WindfarmId, turbine.TurbineRdspp, turbine.ItemId, startUtc, endUtc,
                    windfarm.AlarmCodes, _options.SourceQueryPageSize, offset, cancellationToken);

                if (page.Count == 0)
                {
                    break;
                }

                if (page.Count > _options.SourceQueryPageSize)
                {
                    return await FailAndReturnAsync(
                        request, lease, run, runFinalized, totalWritten,
                        $"Historical alarm page for turbine {turbine.TurbineRdspp} returned {page.Count} rows exceeding requested page size {_options.SourceQueryPageSize}.",
                        cancellationToken);
                }

                foreach (var alarm in page)
                {
                    if (lastAlarmId is { } previous && alarm.AlarmId <= previous)
                    {
                        return await FailAndReturnAsync(
                            request, lease, run, runFinalized, totalWritten,
                            $"Historical alarm page for turbine {turbine.TurbineRdspp} returned AlarmId {alarm.AlarmId} not strictly greater than previous {previous}.",
                            cancellationToken);
                    }

                    lastAlarmId = alarm.AlarmId;
                }

                var seq = windowSequence++;
                RawWriteResultModel writeResult;
                try
                {
                    writeResult = await _writer.WriteBatchAsync(request.WindfarmId, lease.LeaseToken, seq, page, cancellationToken);
                }
                catch (Exception ex)
                {
                    return await FailAndReturnAsync(request, lease, run, runFinalized, totalWritten, ex.Message, cancellationToken);
                }
                _telemetry.BatchWritten(request.WindfarmId, writeResult.RowCount, writeResult.QuarantinedEvents.Count);

                if (writeResult.PublishedEvents.Count > 0)
                {
                    try
                    {
                        await _publisher.PublishAsync(new RawAlarmBatchAvailableV1
                        {
                            SourceType = request.SourceType,
                            WindfarmId = request.WindfarmId,
                            ExtractionToken = lease.LeaseToken,
                            WindowSequence = seq,
                            BlobPath = writeResult.BlobPath,
                            ShardPaths = writeResult.ShardPaths?.ToArray(),
                            RunId = runId,
                        OperationId = OperationCorrelation.Require(run.OperationId),
                            BatchKind = "Historical",
                        }, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        return await FailAndReturnAsync(request, lease, run, runFinalized, totalWritten, ex.Message, cancellationToken);
                    }

                    totalWritten += writeResult.RowCount;

                    var newOpenAlarms = writeResult.PublishedEvents
                        .Where(e => e.EndTimeUtc is null && !e.IsTombstone)
                        .Select(e => (turbine.TurbineRdspp, e.AlarmId))
                        .ToList();

                    if (newOpenAlarms.Count > 0)
                    {
                        var enrolled = await _cursorRepository.TryEnrollOpenAlarmsAsync(
                            request.WindfarmId, lease.LeaseToken, newOpenAlarms, cancellationToken);
                        if (!enrolled)
                        {
                            return await ZombieAndReturnAsync(
                                request, lease, run, runFinalized, totalWritten,
                                "Open-alarm enrollment was rejected (lease no longer held).", cancellationToken);
                        }
                    }
                }

                if (page.Count < _options.SourceQueryPageSize)
                {
                    break;
                }

                offset += _options.SourceQueryPageSize;
            }
        }

        // Historical never advances the checkpoint: complete with the unchanged from-snapshot as
        // the to-snapshot, whether or not anything was found ("no-data completes").
        var completed = await _cursorRepository.TryCompleteRunAsync(
            run.RunId, request.WindfarmId, request.SourceType, lease.LeaseToken, lease.LastCommittedId, cancellationToken);
        if (!completed)
        {
            return await ZombieAndReturnAsync(
                request, lease, run, runFinalized, totalWritten,
                "Final run completion was rejected (lease no longer held).", cancellationToken);
        }

        runFinalized.Finalized = true;
        return new RunResultModel(RunOutcome.Success, totalWritten);
    }
}
