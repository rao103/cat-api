using System.Diagnostics;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Observability;
using Harmonizer.Core.Operations;
using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Harmonizer.Core.SourceTypes.Production.Models;
using Microsoft.Extensions.Options;

namespace Harmonizer.Core.SourceTypes.Production.Operations;

public sealed class ProductionBatchProcessor(
    IProductionControlStateRepository state,
    IProductionArtifactStore artifacts,
    ProductionMergeService merger,
    IOptions<HarmonizerOptions> options,
    ITelemetry<ProductionBatchProcessor> telemetry,
    ITelemetry<AsyncHeartbeat> heartbeatTelemetry)
{
    public async Task ProcessAsync(ProductionWork work, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var outcome = "failed";
        using var scope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["SourceType"] = "Production",
            ["OperationId"] = work.Operation.OperationId,
            ["ExecutionId"] = work.Operation.ExecutionId,
            ["PipelineStage"] = "harmonizer",
            ["ArtifactId"] = work.Batch.ArtifactId,
            ["BatchId"] = work.Batch.BatchId,
            ["RequestId"] = work.RequestId,
            ["AttemptId"] = work.AttemptId,
            ["WindfarmId"] = work.Windfarm.WindfarmId,
            ["WindfarmName"] = work.Windfarm.WindfarmName,
            ["DataDate"] = work.DataDate,
            ["SourceTraceId"] = work.TraceId
        });
        using var activity = HarmonizerDiagnostics.Activities.StartActivity("harmonizer.production.batch");
        activity?.SetTag("batch.id", work.Batch.BatchId);
        activity?.SetTag("windfarm.id", work.Windfarm.WindfarmId);
        activity?.SetTag("cat.operation.id", work.Operation.OperationId);
        activity?.SetTag("cat.execution.id", work.Operation.ExecutionId);
        activity?.SetTag("cat.pipeline.stage", "harmonizer");
        activity?.SetTag("cat.source.type", "Production");
        activity?.SetTag("cat.windfarm.id", work.Windfarm.WindfarmId);
        activity?.SetTag("cat.windfarm.name", work.Windfarm.WindfarmName);
        activity?.SetTag("cat.batch.id", work.Batch.BatchId);
        // The wire field is correlation evidence, not a W3C traceparent to invent a parent from.
        activity?.SetTag("source.trace_id", work.TraceId);

        try
        {
            await ApplyPendingItemsAsync(work, cancellationToken);
            outcome = "succeeded";
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            outcome = "cancelled";
            throw;
        }
        catch (Exception error)
        {
            activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name);
            throw;
        }
        finally
        {
            HarmonizerDiagnostics.BatchDuration.Record(
                Stopwatch.GetElapsedTime(started).TotalSeconds,
                new KeyValuePair<string, object?>("outcome", outcome));
        }
    }

    private async Task ApplyPendingItemsAsync(ProductionWork work, CancellationToken cancellationToken)
    {
        await state.RegisterBatchAsync(work, cancellationToken);
        var pending = await GetPendingTurbinesAsync(work, cancellationToken);
        if (pending.Count == 0)
        {
            telemetry.Information("Batch replay already has durable receipts for every turbine.");
            return;
        }

        var rows = await ReadRawAndRecordFailuresAsync(work, pending, cancellationToken);
        var rowsByTurbine = rows.ToLookup(row => row.WtgId);
        var failures = new List<Exception>();
        foreach (var turbine in pending)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var scope = telemetry.BeginScope(new Dictionary<string, object?>
            {
                ["WtgId"] = turbine.WtgId,
                ["WtgName"] = turbine.WtgName
            });
            try
            {
                await ProcessItemAsync(work, turbine, rowsByTurbine[turbine.WtgId].ToArray(), cancellationToken);
            }
            catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                failures.Add(error);
                telemetry.Warning(error, "Production member failed. ErrorCode={ErrorCode}", error.GetType().Name);
            }
        }

        if (failures.Count > 0)
        {
            throw new AggregateException("Production batch remains retryable until every member is durable.", failures);
        }
    }

    private async Task<IReadOnlyList<TurbineIdentity>> GetPendingTurbinesAsync(
        ProductionWork work, CancellationToken cancellationToken)
    {
        var pending = new List<TurbineIdentity>();
        foreach (var turbine in work.Turbines)
        {
            if (!await state.IsItemAppliedAsync(work.Batch, turbine.WtgId, cancellationToken))
            {
                pending.Add(turbine);
            }
        }

        return pending;
    }

    private async Task<IReadOnlyList<ProductionRow>> ReadRawAndRecordFailuresAsync(
        ProductionWork work, IReadOnlyList<TurbineIdentity> pending, CancellationToken cancellationToken)
    {
        try
        {
            return await artifacts.ReadRawAsync(work, cancellationToken);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            // Capture the first blocker even when decoding fails before any per-turbine merge.
            foreach (var turbine in pending)
            {
                try
                {
                    var key = new WtgDayKey(work.Windfarm.WindfarmId, turbine.WtgId, work.DataDate);
                    var lease = await state.ClaimAsync(key, work.Batch, Guid.NewGuid(), cancellationToken);
                    await state.FailAsync(lease, work, error.GetType().Name, cancellationToken);
                }
                catch (LeaseUnavailableException)
                {
                    // The earlier owner/blocker remains authoritative.
                }
            }

            throw;
        }
    }

    private async Task ProcessItemAsync(
        ProductionWork work, TurbineIdentity turbine, IReadOnlyList<ProductionRow> rows,
        CancellationToken cancellationToken)
    {
        var key = new WtgDayKey(work.Windfarm.WindfarmId, turbine.WtgId, work.DataDate);
        var lease = await state.ClaimAsync(key, work.Batch, Guid.NewGuid(), cancellationToken);
        using var operation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await using var heartbeat = new AsyncHeartbeat(
            TimeSpan.FromSeconds(options.Value.HeartbeatSeconds),
            token => state.RenewAsync(lease, token), operation, heartbeatTelemetry);
        try
        {
            var previous = lease.SnapshotPath is null
                ? new ProductionSnapshot([])
                : await artifacts.ReadSnapshotAsync(lease.SnapshotPath, operation.Token);
            if (lease.CanonicalHash is not null && merger.Merge(key, previous, []).CanonicalHash != lease.CanonicalHash)
            {
                throw new WorkValidationException("Committed snapshot does not match its canonical hash.");
            }

            var result = merger.Merge(key, previous, rows);
            var changed = result.Changed || lease.SnapshotPath is null;
            var candidate = changed
                ? await artifacts.WriteCandidateAsync(lease, work, result, operation.Token)
                : GetExistingCandidate(lease);

            // Stop renewal while still owning the lease; SQL verifies time, owner and fence on commit.
            await heartbeat.StopAsync();
            operation.Token.ThrowIfCancellationRequested();
            var revision = lease.Revision + (changed ? 1 : 0);
            var commit = new ComponentCommit(
                lease, revision, result.CanonicalHash, candidate,
                result.Snapshot.Rows.Count, result.Complete, changed) { Operation = work.Operation };
            await state.CommitAsync(commit, cancellationToken);
            HarmonizerDiagnostics.Committed.Add(
                1, new KeyValuePair<string, object?>("status", result.Complete ? "complete" : "partial"));
            telemetry.Information(
                "Production component committed. Revision={Revision} RecordCount={RecordCount} Complete={Complete} Changed={Changed}",
                revision, result.Snapshot.Rows.Count, result.Complete, changed);
        }
        catch (Exception error)
        {
            if (error is LeaseLostException)
            {
                HarmonizerDiagnostics.FenceRejected.Add(1);
            }

            if (!cancellationToken.IsCancellationRequested)
            {
                await RecordFailureAsync(lease, work, error);
            }

            throw;
        }
    }

    private static ComponentCandidate GetExistingCandidate(ComponentLease lease) => new(
        lease.ParquetPath,
        lease.SnapshotPath ?? throw new WorkValidationException("No snapshot for no-op."),
        lease.ManifestPath ?? throw new WorkValidationException("No lineage for no-op."));

    private async Task RecordFailureAsync(ComponentLease lease, ProductionWork work, Exception error)
    {
        using var failureTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await state.FailAsync(lease, work, error.GetType().Name, failureTimeout.Token);
        }
        catch (Exception failureError)
        {
            // Preserve the processing exception for retry even when recording its failure also fails.
            telemetry.Error(failureError, "Could not record failure. ErrorCode={ErrorCode}", failureError.GetType().Name);
        }
    }
}
