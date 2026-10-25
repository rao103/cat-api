using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using ProductionIngestor.Enums;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.Operations.Execution;

public sealed class ScheduledIngestionWorker(
    IExtractionSchedulingRepository repository,
    IProductionAssignmentExecutor executor,
    ILogger<ScheduledIngestionWorker> logger)
{
    public const string TelemetryName = "ProductionIngestor.Scheduling";
    public static readonly ActivitySource Activities = new(TelemetryName);
    private static readonly Meter Metrics = new(TelemetryName);
    private static readonly Counter<int> Attempts = Metrics.CreateCounter<int>("extraction_attempt_count");

    public async Task<ScheduledIngestionResult> RunAsync(
        ExtractionClaimRequestModel request, CancellationToken cancellationToken)
    {
        using var activity = Activities.StartActivity("ScheduledRun");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(request.Policy.MaxInvocationDuration);
        var results = new List<ExtractionAttemptResultModel>();
        var budgetExhausted = false;
        while (results.Count < request.Policy.MaxClaimsPerInvocation)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (deadline.IsCancellationRequested)
            {
                budgetExhausted = true;
                break;
            }
            ExtractionClaimModel? claim = null;
            try
            {
                claim = await repository.TryClaimCursorAndStartExtractionRunAsync(request, deadline.Token);
                if (claim is null)
                    break;
                using var attemptActivity = Activities.StartActivity("ExtractionAttempt");
                using var attemptScope = logger.BeginScope(new Dictionary<string, object?>
                {
                    ["OperationId"] = claim.Run.OperationId,
                    ["RunId"] = claim.Request.RunId,
                    ["AttemptId"] = claim.Run.RunId,
                    ["SourceType"] = claim.Request.SourceType,
                    ["WindfarmId"] = claim.Request.WindfarmId,
                    ["JobName"] = request.JobName
                });
                logger.LogInformation("Extraction attempt {AttemptId} started for {SourceType} for WindfarmId:{WindfarmId} with token {LeaseToken}.",
                    claim.Run.RunId, claim.Request.SourceType, claim.Request.WindfarmId, claim.Lease.LeaseToken);
                var result = await executor.ExecuteAsync(claim, deadline.Token);
                results.Add(new(claim, result));
                if (result.Outcome != RunOutcome.Success)
                    logger.LogError("Extraction attempt {AttemptId} for {SourceType}/{WindfarmId} finished with {Outcome}.",
                        claim.Run.RunId, claim.Request.SourceType, claim.Request.WindfarmId, result.Outcome);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException) when (deadline.IsCancellationRequested)
            {
                budgetExhausted = true;
                if (claim is not null)
                    results.Add(new(claim, new(RunOutcome.Failed, Array.Empty<DateOnly>(), "InvocationDeadline")));
                break;
            }
            catch (Exception error) when (claim is not null)
            {
                logger.LogError(error, "Extraction attempt {AttemptId} failed for {SourceType}/{WindfarmId}.",
                    claim.Run.RunId, claim.Request.SourceType, claim.Request.WindfarmId);
                var result = new RunResultModel(RunOutcome.Failed, Array.Empty<DateOnly>(), error.GetType().Name);
                results.Add(new(claim, result));
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await repository.TryFinalizeAsync(new(claim, result), cleanup.Token);
            }
        }
        budgetExhausted |= results.Count >= request.Policy.MaxClaimsPerInvocation;
        foreach (var attempt in results)
            Attempts.Add(1, new KeyValuePair<string, object?>("outcome", attempt.Result.Outcome.ToString()));
        var selection = await repository.GetSelectionStatusAsync(request, cancellationToken);
        if (selection.RegisteredCount == 0)
            logger.LogWarning("No enabled, finite-checkpoint registrations for {SourceType}.", request.Scope.SourceType);
        logger.LogInformation("Scheduled invocation finished: {AttemptCount} attempts, {RegisteredCount} registered, {BusyCount} busy, {DueCount} due, budget exhausted: {BudgetExhausted}.",
            results.Count, selection.RegisteredCount, selection.BusyCount, selection.DueCount, budgetExhausted);
        return new(results, selection, budgetExhausted);
    }
}