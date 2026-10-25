using Microsoft.Extensions.Options;
using OutageIngestor.Configuration;
using OutageIngestor.Enums;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Operations.Execution;
using OutageIngestor.Worker.Configuration;

namespace OutageIngestor.Worker.Operations;

public sealed record IngestionExecutionResult(int ExitCode);

/// <summary>Selects scheduled-claim vs. targeted-refresh execution for one Kubernetes Job
/// invocation, mirroring ProductionIngestor's IngestionExecutionService.</summary>
public sealed class IngestionExecutionService(
    IConfiguration configuration,
    IOptions<IngestionOptions> ingestionOptions,
    IExtractionSchedulingRepository repository,
    IOutageAssignmentExecutor executor,
    ScheduledIngestionWorker scheduledWorker,
    IIngestionTelemetry<IngestionExecutionService> telemetry)
{
    // Normal Kubernetes Job entry point. Each Job supplies its own environment.
    public Task<IngestionExecutionResult> RunAsync(CancellationToken cancellationToken) => RunCoreAsync(
        new IngestionRequestInput(
			WindfarmId: null,
			RefreshWindfarmId: configuration["REFRESH_WINDFARM_ID"],
			RefreshWindfarmName: configuration["REFRESH_WINDFARM_NAME"],
			RefreshRunId: configuration["REFRESH_RUN_ID"],
            RefreshScopeStartUtc: configuration["REFRESH_SCOPE_START_UTC"],
			RefreshScopeEndUtc: configuration["REFRESH_SCOPE_END_UTC"],
			OperationId: configuration["OPERATION_ID"]),
        configuration["REFRESH_REQUESTED_BY"],
		configuration["REFRESH_REASON"],
		configuration["Ingestion:JobName"],
        cancellationToken);

    // Development listener entry point. Never mutate global IConfiguration per request.
    public Task<IngestionExecutionResult> RunRefreshAsync(
        string windfarmId, string windfarmName, string runId, string scopeStartUtc, string scopeEndUtc, string requestedBy, string? reason,
        CancellationToken cancellationToken, string? operationId = null) => RunCoreAsync(
            new IngestionRequestInput(
				WindfarmId: null,
				RefreshWindfarmId: windfarmId,
				RefreshWindfarmName: windfarmName,
				RefreshRunId: runId, 
				RefreshScopeStartUtc: scopeStartUtc, 
				RefreshScopeEndUtc: scopeEndUtc,
				OperationId: operationId),
            requestedBy, reason, $"local-refresh-{runId}", cancellationToken);

    private async Task<IngestionExecutionResult> RunCoreAsync(
        IngestionRequestInput input, string? requestedBy, string? reason,
        string? jobName, CancellationToken cancellationToken)
    {
        CAT.Messaging.Contracts.OperationCorrelation.Validate(input.OperationId);
        var request = new ExtractionClaimRequestModel(new("Outage"),
            $"{Environment.MachineName}-{Guid.NewGuid():N}",
            ingestionOptions.Value,
            RequestedBy: requestedBy,
            Reason: reason,
            JobName: jobName, OperationId: input.OperationId);

        var hasRefreshInput = input.IsRefresh || request.RequestedBy is not null || request.Reason is not null;
        if (!hasRefreshInput)
        {
            var scheduled = await scheduledWorker.RunAsync(request, cancellationToken);
            return new(scheduled.Succeeded ? 0 : 1);
        }

        var parsed = IngestionRequestParser.TryParse(input);
        if (!input.IsRefresh || !parsed.Succeeded)
        {
            telemetry.Warning(
                $"Invalid refresh request: {parsed.Error ?? "A windfarm identifier is required."}",
                input.RefreshRunId, jobName, nameof(RunCoreAsync));
            return new(2);
        }
        if (!await repository.IsRegisteredAsync(new(request.Scope, parsed.Request.WindfarmId), cancellationToken))
        {
            telemetry.Warning(
                $"Refresh requires an enabled Outage cursor with a finite checkpoint for wind farm {parsed.Request.WindfarmName}.",
                parsed.Request.RunId, jobName, nameof(RunCoreAsync));
            return new(2);
        }
        var claim = await repository.TryClaimCursorAndStartExtractionRunAsync(request with { Refresh = parsed.Request }, cancellationToken);
        if (claim is null)
        {
            telemetry.Information(
                $"Refresh {parsed.Request.RunId} for {parsed.Request.WindfarmName} is busy or registration is no longer enabled.",
                parsed.Request.RunId, jobName, nameof(RunCoreAsync));
            return new(1);
        }
        using var activity = ScheduledIngestionWorker.Activities.StartActivity("RefreshRun");
        using var executionScope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["OperationId"] = claim.Run.OperationId,
            ["RunId"] = parsed.Request.RunId,
            ["AttemptId"] = claim.Run.RunId,
            ["SourceType"] = claim.Request.SourceType,
            ["WindfarmName"] = claim.Lease.WindfarmName,
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
            $"Refresh attempt {claim.Run.RunId} finished with {result.Outcome} and {result.EventsWritten} events written.",
            parsed.Request.RunId, jobName);
        return new(result.Outcome == RunOutcome.Success ? 0 : 1);
    }
}
