using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
namespace CAT.RawFanOut.Operations.Retention;

public sealed class RetentionCleanupJob
{
    private readonly IArtifactRetentionCleanupStore store;
    private readonly RetentionCleanupOptions options;
    private readonly ITelemetry<RetentionCleanupJob> telemetry;
    private readonly TimeProvider clock;
    private readonly IDeadLetterRetentionStore? incidents;

    public RetentionCleanupJob(
        IArtifactRetentionCleanupStore store,
        RetentionCleanupOptions options,
        ITelemetry<RetentionCleanupJob> telemetry,
        TimeProvider? clock = null,
        IDeadLetterRetentionStore? incidents = null)
    {
        this.store = store;
        this.options = options;
        this.telemetry = telemetry;
        this.clock = clock ?? TimeProvider.System;
        this.incidents = incidents;
    }

    public async Task<RetentionCleanupSummary> ExecuteAsync(CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        var cutoffUtc = now.AddMonths(-options.RetentionMonths);
        var incidentCutoffUtc = now.AddMonths(-options.ResolvedIncidentRetentionMonths);
        var stopwatch = Stopwatch.StartNew();
        var deletedArtifacts = 0;
        var deletedBatches = 0;
        var deletedIncidents = 0;
        telemetry.Information($"Retention cleanup starting. CutoffUtc={cutoffUtc:O} IncidentCutoffUtc={incidentCutoffUtc:O} MaxParentsPerBatch={options.MaxParentsPerBatch}");
        try
        {
            RetentionCleanupBatchResult result;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();
                result = await store.DeleteEligibleBatchAsync(cutoffUtc, options.MaxParentsPerBatch, cancellationToken);
                deletedArtifacts += result.DeletedArtifacts;
                deletedBatches += result.DeletedBatches;
            }
            while (result.DeletedArtifacts > 0);

            if (incidents is not null)
            {
                int deleted;
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    deleted = await incidents.DeleteResolvedBatchAsync(incidentCutoffUtc, options.MaxParentsPerBatch, cancellationToken);
                    deletedIncidents += deleted;
                }
                while (deleted > 0);
            }

            telemetry.Information(
                $"Retention cleanup completed. CutoffUtc={cutoffUtc:O} DeletedArtifacts={deletedArtifacts} " +
                $"DeletedBatches={deletedBatches} DeletedResolvedIncidents={deletedIncidents} ElapsedMs={stopwatch.ElapsedMilliseconds}");
            return new RetentionCleanupSummary(cutoffUtc, deletedArtifacts, deletedBatches, stopwatch.Elapsed, deletedIncidents);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            telemetry.Error(
                $"Retention cleanup failed. CutoffUtc={cutoffUtc:O} DeletedArtifacts={deletedArtifacts} " +
                $"DeletedBatches={deletedBatches} DeletedResolvedIncidents={deletedIncidents} ElapsedMs={stopwatch.ElapsedMilliseconds}",
                error);
            throw;
        }
    }
}
