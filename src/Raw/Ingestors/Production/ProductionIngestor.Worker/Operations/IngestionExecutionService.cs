using CAT.Messaging.Contracts;
using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Enums;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Execution;
using ProductionIngestor.Worker.Configuration;

namespace ProductionIngestor.Worker.Operations;

public sealed record IngestionExecutionResult(int ExitCode);

public sealed class IngestionExecutionService(
    IConfiguration configuration,
    IOptions<IngestionOptions> ingestionOptions,
    IExtractionSchedulingRepository repository,
    IProductionAssignmentExecutor executor,
    ScheduledIngestionWorker scheduledWorker,
    IIngestionTelemetry<IngestionExecutionService> telemetry)
{
    // Normal Kubernetes Job entry point. Each Job supplies its own environment.
    public Task<IngestionExecutionResult> RunAsync(CancellationToken cancellationToken) => RunCoreAsync(
        new IngestionRequestInput(
            WindfarmId: null,
            RefreshWindfarmId: configuration["REFRESH_WINDFARM_ID"],
            RefreshWindfarmName: configuration["REFRESH_WINDFARM_NAME"],
            RefreshDates: configuration["REFRESH_DATA_DATES"],
            RefreshRunId: configuration["REFRESH_RUN_ID"],
            OperationId: configuration["OPERATION_ID"]),
        configuration["REFRESH_REQUESTED_BY"],
        configuration["REFRESH_REASON"],
        configuration["Ingestion:JobName"],
        cancellationToken);

    // Development listener entry point. Each request supplies its own inputs.
    public Task<IngestionExecutionResult> RunRefreshAsync(
        string windfarmId, string windfarmName, string dataDates,
        string runId, string requestedBy, string? reason,
        CancellationToken cancellationToken, string? operationId = null) => RunCoreAsync(
            new IngestionRequestInput(
                WindfarmId: null,
                RefreshWindfarmId: windfarmId,
                RefreshWindfarmName: windfarmName,
                RefreshDates: dataDates,
                RefreshRunId: runId,
                OperationId: operationId),
            requestedBy, reason, $"local-refresh-{runId}", cancellationToken);

    private async Task<IngestionExecutionResult> RunCoreAsync(
        IngestionRequestInput input, string? requestedBy, string? reason,
        string? jobName, CancellationToken cancellationToken)
    {
        OperationCorrelation.Validate(input.OperationId);
        var request = new ExtractionClaimRequestModel(new("Production"),
            $"{Environment.MachineName}-{Guid.NewGuid():N}",
            ingestionOptions.Value,
            RequestedBy: requestedBy,
            Reason: reason,
            JobName: jobName, OperationId: input.OperationId);

        if (!input.IsRefresh)
        {
            var scheduled = await scheduledWorker.RunAsync(request, cancellationToken);
            return new(scheduled.Succeeded ? 0 : 1);
        }

        var parsed = IngestionRequestParser.TryParse(input);
        if (!parsed.Succeeded)
        {
            telemetry.Warning(
                $"Invalid refresh request: {parsed.Error}",
                input.RefreshRunId, jobName, nameof(RunCoreAsync));
            return new(2);
        }
        if (!await repository.IsRegisteredAsync(new(request.Scope, parsed.Request.WindfarmId), cancellationToken))
        {
            telemetry.Warning(
                $"Refresh requires an enabled Production cursor with a finite checkpoint for wind farm {parsed.Request.WindfarmId}.",
                parsed.Request.RunId, jobName, nameof(RunCoreAsync));
            return new(2);
        }
        var claim = await repository.TryClaimCursorAndStartExtractionRunAsync(request with { Refresh = parsed.Request }, cancellationToken);
        if (claim is null)
        {
            telemetry.Information(
                $"Refresh {parsed.Request.RunId} for {parsed.Request.WindfarmId} is busy or registration is no longer enabled; no dates were processed.",
                parsed.Request.RunId, jobName, nameof(RunCoreAsync));
            return new(1);
        }
        using var activity = ScheduledIngestionWorker.Activities.StartActivity("RefreshRun");
        using var executionScope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["OperationId"] = claim.Run.OperationId,
            ["RunId"] = parsed.Request.RunId,
            ["AttemptId"] = RawArtifactConventions.AttemptId(claim.Lease.LeaseToken),
            ["ExtractionRunId"] = claim.Run.RunId,
            ["SourceType"] = claim.Request.SourceType,
            ["WindfarmId"] = claim.Request.WindfarmId,
            ["JobName"] = jobName
        });
        RunResultModel result;
        try
        {
            result = await executor.ExecuteAsync(claim, cancellationToken);
        }
        catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            telemetry.Error("Refresh execution failed.", error, parsed.Request.RunId);
            throw;
        }

        telemetry.Information(
            $"Refresh attempt {RawArtifactConventions.AttemptId(claim.Lease.LeaseToken)} finished with {result.Outcome} and {result.DaysWritten.Count} published dates.",
            parsed.Request.RunId, jobName);
        return new(result.Outcome == RunOutcome.Success ? 0 : 1);
    }
}
