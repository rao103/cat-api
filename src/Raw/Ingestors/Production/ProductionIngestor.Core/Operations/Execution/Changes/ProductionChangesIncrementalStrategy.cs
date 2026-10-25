using Azure;
using ProductionIngestor.Configuration;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Monitoring;

namespace ProductionIngestor.Operations.Execution.Changes;

public enum ChangesOutcome
{
    Succeeded,
    Failed,
}

/// <summary>Identity and committed checkpoint needed to run one windowed ProductionChanges incremental attempt.</summary>
public sealed record ChangesExecutionRequest(
    int WindfarmId,
    string WindfarmName,
    string SourceType,
    string RunId,
    long ExtractionToken,
    DateTime CommittedUpdatedUtc,
    string CommittedId,
    string? OperationId);

/// <summary>
/// The whole-execution decision: the non-regressing checkpoint to commit (unchanged from the
/// request when no rows were observed), the exact executed query bounds, and informational
/// published dates. Never partially populated for a Failed outcome.
/// </summary>
public sealed record ChangesExecutionResult(
    ChangesOutcome Outcome,
    DateTime NewCommittedUpdatedUtc,
    string NewCommittedId,
    IReadOnlyList<DateOnly> PublishedDates,
    DateTime? QueryStartInclusiveUtc,
    DateTime? QueryEndExclusiveUtc,
    string? FailureMessage = null);

/// <summary>
/// Owns the new windowed changed-row incremental flow end to end: an aggregate planning query
/// against the source groups rows past the checkpoint into UTC calendar-month production-time
/// windows, then bounded in-process parallel window execution streams and writes each window's
/// rows, and finally a whole-execution non-regressing checkpoint decision (the maximum
/// `LastUpdatedUtc` actually observed across every window). Never touches `extraction_cursor`
/// itself; the caller performs the single fenced commit using the values this returns. Isolated
/// from the legacy day-snapshot path (<see cref="ProductionIngestionRunner"/> only calls into this
/// class when `Ingestion:ProductionChangesEnabled` is true).
/// </summary>
public sealed class ProductionChangesIncrementalStrategy
{
    private readonly IProductionSourceAdapter _sourceAdapter;
    private readonly IProductionScopeRepository _scopeRepository;
    private readonly ProductionChangeWindowProcessor _windowProcessor;
    private readonly IIngestionTelemetry<ProductionChangesIncrementalStrategy> _telemetry;
    private readonly IngestionOptions _options;
    private readonly string _sourceSystemName;

    public ProductionChangesIncrementalStrategy(
        IProductionSourceAdapter sourceAdapter,
        IProductionScopeRepository scopeRepository,
        ProductionChangeWindowProcessor windowProcessor,
        IIngestionTelemetry<ProductionChangesIncrementalStrategy> telemetry,
        IngestionOptions options,
        string sourceSystemName)
    {
        _sourceAdapter = sourceAdapter;
        _scopeRepository = scopeRepository;
        _windowProcessor = windowProcessor;
        _telemetry = telemetry;
        _options = options;
        _sourceSystemName = sourceSystemName;
    }

    public async Task<ChangesExecutionResult> ExecuteAsync(ChangesExecutionRequest request, CancellationToken cancellationToken)
    {
        var checkpointExclusiveUtc = request.CommittedUpdatedUtc;

        var scope = await _scopeRepository.GetActiveScopeAsync(request.WindfarmId, DateTime.UtcNow, cancellationToken);
        if (scope is null || scope.ActiveTurbineNames.Count == 0)
        {
            // Inactive windfarm or no eligible turbine: complete as no data without advancing the checkpoint.
            _telemetry.Information(
                $"No eligible active turbines for windfarm {request.WindfarmId}; ProductionChanges source query skipped.", request.RunId);
            return NoDataResult(request, checkpointExclusiveUtc);
        }

        _telemetry.Information(
            $"Planning ProductionChanges source pull for windfarm {request.WindfarmId} from checkpoint {checkpointExclusiveUtc:O} " +
            $"across {scope.ActiveTurbineNames.Count} active turbines.", request.RunId);
        var frozenMaxInclusiveUtc = await _sourceAdapter.GetLatestLastUpdatedUtcAsync(
            scope.WindfarmName, scope.ActiveTurbineNames, scope.MinRetrieveDateUtc, cancellationToken);
        _telemetry.SourceContactSucceeded(request.WindfarmName, request.SourceType);

        if (frozenMaxInclusiveUtc is null || frozenMaxInclusiveUtc.Value <= checkpointExclusiveUtc)
        {
            // No eligible source row past the checkpoint: nothing to advance, no windows to execute.
            _telemetry.Information(
                $"ProductionChanges source pull has no rows past checkpoint {checkpointExclusiveUtc:O} for windfarm {request.WindfarmId}.",
                request.RunId);
            return NoDataResult(request, checkpointExclusiveUtc);
        }

        var plannedWindows = ProductionChangeWindowPlanner.PlanCalendarMonthWindows(checkpointExclusiveUtc, frozenMaxInclusiveUtc.Value);
        if (plannedWindows.Count == 0)
        {
            _telemetry.Information(
                $"ProductionChanges planner returned no windows for frozen source upper bound {frozenMaxInclusiveUtc.Value:O}.", request.RunId);
            return NoDataResult(request, checkpointExclusiveUtc);
        }

        _telemetry.Information(
            $"Starting {plannedWindows.Count} ProductionChanges window(s) for windfarm {request.WindfarmId}, " +
            $"LastUpdatedUtc range {plannedWindows[0].MinInclusiveUtc:O} to {plannedWindows[^1].MaxInclusiveUtc:O}.", request.RunId);

        var executionRequests = plannedWindows
            .Select((window, index) => new ExtractionWindowExecutionRequest(
                WindfarmId: request.WindfarmId,
                WindfarmName: request.WindfarmName,
                SourceType: request.SourceType,
                SourceSystem: _sourceSystemName,
                RunId: request.RunId,
                ExtractionToken: request.ExtractionToken,
                ActiveTurbineNames: scope.ActiveTurbineNames,
                MinRetrieveDateUtc: scope.MinRetrieveDateUtc,
                WindowNumber: index + 1,
                WindowMinInclusiveUtc: window.MinInclusiveUtc,
                WindowMaxInclusiveUtc: window.MaxInclusiveUtc,
                OperationId: request.OperationId))
            .ToList();

        var results = await ExecuteWindowsAsync(executionRequests, cancellationToken);

        var failed = executionRequests.FirstOrDefault(window => !results.TryGetValue(window, out var result) || result.Outcome == WindowOutcome.Failed);
        if (failed is not null)
        {
            results.TryGetValue(failed, out var failedResult);
            var message = failedResult?.FailureMessage
                ?? $"Window {failed.WindowMinInclusiveUtc:O}-{failed.WindowMaxInclusiveUtc:O} did not complete.";
            _telemetry.Error(
                $"Production changes window failed for windfarm {request.WindfarmId}, source {request.SourceType}, " +
                $"range {failed.WindowMinInclusiveUtc:O} to {failed.WindowMaxInclusiveUtc:O}.",
                failedResult?.FailureException ?? new InvalidOperationException(message), request.RunId);
            return new ChangesExecutionResult(ChangesOutcome.Failed, request.CommittedUpdatedUtc, request.CommittedId,
                Array.Empty<DateOnly>(), failed.WindowMinInclusiveUtc, failed.WindowMaxInclusiveUtc.AddTicks(1), message);
        }

        var publishedDates = new HashSet<DateOnly>();
        SourceRowKey? finalKey = null;
        foreach (var window in executionRequests)
        {
            var result = results[window];
            foreach (var date in result.PublishedDates) publishedDates.Add(date);

            // Windows use update-time calendar months and complete in parallel, so the
            // overall maximum must be computed across every window rather than assumed from the
            // last window processed.
            if (result.Outcome == WindowOutcome.Succeeded && result.LastObservedRowKey is { } observed
                && (finalKey is null || observed.LastUpdatedUtc > finalKey.LastUpdatedUtc))
            {
                finalKey = observed;
            }
        }

        if (finalKey is null)
        {
            _telemetry.Information(
                $"ProductionChanges completed with no source rows across {executionRequests.Count} window(s) for windfarm {request.WindfarmId}.",
                request.RunId);
            return NoDataResult(request, checkpointExclusiveUtc, publishedDates,
                executionRequests[0].WindowMinInclusiveUtc, executionRequests[^1].WindowMaxInclusiveUtc.AddTicks(1));
        }

        var totalRows = executionRequests.Sum(window => results[window].RowCount);
        var totalParts = executionRequests.Sum(window => results[window].PartCount);
        _telemetry.Information(
            $"ProductionChanges execution completed for windfarm {request.WindfarmId}: {totalRows} source row(s), " +
            $"{totalParts} Parquet part(s), {publishedDates.Count} production date(s) across {executionRequests.Count} window(s).",
            request.RunId);
        _telemetry.IngestionInformation(request.WindfarmName, request.SourceType,
            $"Windowed checkpoint advancing to {finalKey.LastUpdatedUtc:O}.");
        return new ChangesExecutionResult(ChangesOutcome.Succeeded, finalKey.LastUpdatedUtc, finalKey.ToCommittedIdText(),
            publishedDates.ToArray(), executionRequests[0].WindowMinInclusiveUtc, executionRequests[^1].WindowMaxInclusiveUtc.AddTicks(1));
    }

    private async Task<Dictionary<ExtractionWindowExecutionRequest, WindowProcessingResult>> ExecuteWindowsAsync(
        IReadOnlyList<ExtractionWindowExecutionRequest> windows, CancellationToken cancellationToken)
    {
        var results = new Dictionary<ExtractionWindowExecutionRequest, WindowProcessingResult>();
        var resultsLock = new object();

        // A window reporting Failed signals this source (not the caller's shutdown token) so
        // Parallel.ForEachAsync stops scheduling further windows and already-running windows
        // observe cancellation at their next checkpoint; it never throws for this expected case.
        using var failureSignal = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, failureSignal.Token);

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, _options.MaxParallelWindows),
            CancellationToken = linked.Token,
        };

        try
        {
            await Parallel.ForEachAsync(windows, parallelOptions, async (window, windowCancellationToken) =>
            {
                var result = await _windowProcessor.ProcessWindowAsync(window, windowCancellationToken);
                lock (resultsLock)
                {
                    results[window] = result;
                }
                if (result.Outcome == WindowOutcome.Failed)
                {
                    await failureSignal.CancelAsync();
                }
            });
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Cancellation was triggered internally by a failed window; fall through so the
            // caller sees the recorded per-window results (including the failure) below.
        }

        return results;
    }

    private static ChangesExecutionResult NoDataResult(
        ChangesExecutionRequest request, DateTime checkpointExclusiveUtc, IReadOnlyCollection<DateOnly>? publishedDates = null,
        DateTime? queryStartInclusiveUtc = null, DateTime? queryEndExclusiveUtc = null)
        => new(ChangesOutcome.Succeeded, request.CommittedUpdatedUtc, request.CommittedId,
            publishedDates?.ToArray() ?? Array.Empty<DateOnly>(), queryStartInclusiveUtc, queryEndExclusiveUtc);
}
