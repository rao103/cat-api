using System.Diagnostics;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Observability;
using Harmonizer.Core.Operations;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Harmonizer.Core.SourceTypes.Outage.Serialization;
using Microsoft.Extensions.Options;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

public sealed class OutageBatchProcessor(IOutageControlRepository state, IOutageArtifacts artifacts,
    OutageCanonicalReader canonicalReader, OutageMergeService merger, OutageRangePlanner ranges,
    OutageLifecycleProcessor lifecycle, OutageScopeProjector scopeProjector, TimeProvider clock, IOptions<HarmonizerOptions> options,
    ITelemetry<OutageBatchProcessor> telemetry, ITelemetry<AsyncHeartbeat> heartbeatTelemetry)
{
    public async Task ProcessAsync(OutageWork work, CancellationToken cancellationToken)
    {
        using var scope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["SourceType"] = "Outage", ["WindfarmId"] = work.Windfarm.WindfarmId,
            ["WindfarmName"] = work.Windfarm.WindfarmName, ["ArtifactId"] = work.Batch.ArtifactId,
            ["BatchId"] = work.Batch.BatchId, ["OperationId"] = work.Operation.OperationId,
            ["ExecutionId"] = work.Operation.ExecutionId, ["ExtractionToken"] = work.Message.ExtractionToken,
            ["AttemptId"] = work.Message.AttemptId, ["WorkVersion"] = work.Message.SchemaVersion,
            ["WorkKind"] = work.Message.WorkKind, ["SourceTraceId"] = work.Message.TraceId, ["PipelineStage"] = "harmonizer"
        });
        using var activity = HarmonizerDiagnostics.Activities.StartActivity("harmonizer.outage.batch");
        activity?.SetTag("cat.operation.id", work.Operation.OperationId);
        activity?.SetTag("cat.execution.id", work.Operation.ExecutionId);
        activity?.SetTag("source.trace_id", work.Message.TraceId);
        var started = Stopwatch.GetTimestamp();
        var outcome = "failed";
        try
        {
            if (await state.RegisterAsync(work, cancellationToken)) { outcome = "replayed"; return; }
            if (work.IsReconciliation && !await state.HasExpectedReceiptsAsync(work, cancellationToken))
                throw new LeaseUnavailableException("Snapshot reconciliation is waiting for successful canonical receipts from every expected batch.");
            if (work.IsReconciliation && work.Message.ReconciliationMode == CAT.Messaging.Contracts.EventReconciliationModes.ReceiptOnly)
            {
                await state.RecordObservationAsync(work, cancellationToken);
                outcome = "receipt-only";
                return;
            }
            var alreadyApplied = await state.HasCanonicalReceiptAsync(work.Batch, cancellationToken);
            var incoming = alreadyApplied ? [] : await artifacts.ReadRawAsync(work, cancellationToken);
            if (work.IsObservation)
            {
                if (incoming.Count != 0) throw new WorkValidationException("ArtifactObserved contains unexpected Outage records.");
                await state.RecordObservationAsync(work, cancellationToken);
                outcome = "observed";
                return;
            }
            var lease = await state.ClaimAsync(work.Windfarm, cancellationToken);
            using var operation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            await using var heartbeat = new AsyncHeartbeat(TimeSpan.FromSeconds(options.Value.HeartbeatSeconds),
                token => state.RenewAsync(lease, token), operation, heartbeatTelemetry,
                () => new Dictionary<string, object?> { ["OperationId"] = work.Operation.OperationId,
                    ["ExecutionId"] = work.Operation.ExecutionId, ["SourceType"] = "Outage", ["FenceToken"] = lease.Token });
            try
            {
                // Another replica may have applied this batch while Raw was being decoded.
                if (!await state.HasCanonicalReceiptAsync(work.Batch, operation.Token))
                {
                    var loaded = await canonicalReader.ReadAsync(lease, operation.Token);
                    var publication = work.IsReconciliation
                        ? await BuildReconciliationAsync(lease, work, incoming, loaded, operation.Token)
                        : await BuildCanonicalAsync(lease, work, incoming, loaded, operation.Token);
                    await state.PublishAsync(lease, work, publication, operation.Token);
                    lease = lease with { Revision = lease.Revision + 1, CatalogPath = publication.CatalogPath };
                }
                await lifecycle.ProcessClaimedAsync(lease, operation.Token);
                var completed = await state.CompleteBatchAsync(work.Batch, operation.Token);
                await heartbeat.StopAsync();
                outcome = "succeeded";
                activity?.SetStatus(ActivityStatusCode.Ok);
                telemetry.Information("Outage work durably accepted. BatchId={BatchId} MaterializationCompleted={MaterializationCompleted}", work.Batch.BatchId, completed);
            }
            catch (Exception error) when (error is not LeaseUnavailableException)
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    using var failureTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    try { await state.RecordFailureAsync(lease, error.GetType().Name, failureTimeout.Token); }
                    catch (Exception failure) { telemetry.Error(failure, "Could not record Outage failure."); }
                }
                throw;
            }
            finally
            {
                try { await heartbeat.StopAsync(); }
                finally
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    await state.ReleaseAsync(lease, timeout.Token);
                }
            }
        }
        catch (LeaseUnavailableException)
        {
            outcome = "pending";
            telemetry.Debug("Outage batch retained while durable dependencies or materialization remain pending.");
            throw;
        }
        catch (Exception error)
        {
            if (error is not OperationCanceledException)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                try { await state.RecordSourceFailureAsync(work, error.GetType().Name, timeout.Token); }
                catch (Exception failure) { telemetry.Error(failure, "Could not record source batch failure."); }
            }
            if (error is LeaseLostException) HarmonizerDiagnostics.FenceRejected.Add(1);
            activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name);
            telemetry.Warning(error, "Outage batch failed. ErrorCode={ErrorCode}", error.GetType().Name);
            throw;
        }
        finally
        {
            HarmonizerDiagnostics.BatchDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds,
                new KeyValuePair<string, object?>("source.type", "Outage"), new KeyValuePair<string, object?>("outcome", outcome));
        }
    }

    private async Task<OutagePublication> BuildCanonicalAsync(OutageCatalogLease lease, OutageWork work,
        IReadOnlyList<CanonicalOutage> incoming, LoadedOutageCatalog loaded, CancellationToken cancellationToken)
    {
        var partitionRows = loaded.Partitions.ToDictionary(pair => pair.Key, pair => pair.Value.Events.ToList(), StringComparer.Ordinal);
        var changedPartitions = new HashSet<string>(StringComparer.Ordinal);
        var existingStates = (await state.ReadLifecycleAsync(work.Windfarm.WindfarmId, cancellationToken)).ToDictionary(item => item.WorkId, StringComparer.Ordinal);
        var updates = new List<OutageLifecycleUpdate>();
        var exclusions = loaded.Catalog.Exclusions.ToList();
        foreach (var candidate in incoming)
        {
            var id = candidate.State.Key.OutageClassificationId;
            loaded.Events.TryGetValue(id, out var previous);
            if (IsAtOrBeforeSnapshotFence(candidate, previous, loaded.Catalog.Captures))
                throw new WorkValidationException("Outage event version is at or before an authoritative snapshot absence fence.");
            var result = merger.Merge(previous?.State, candidate.State.LatestChange);
            var workId = OutageWorkKeys.Event(id);
            existingStates.TryGetValue(workId, out var existingState);
            var previousPlan = existingState is null ? null : await artifacts.ReadPlanAsync(existingState.PlanPath, cancellationToken);
            if (!result.Changed)
            {
                if (existingState is not null) updates.Add(new(existingState, previousPlan!));
                telemetry.Debug("Older or identical Outage source change ignored. EventId={EventId}", id);
                continue;
            }
            var eventRevision = (previous?.EventRevision ?? 0) + 1;
            var canonical = new CanonicalOutage(result.State, eventRevision, candidate.Origins);
            var oldPartition = loaded.References.GetValueOrDefault(id)?.PartitionKey;
            var partition = result.State.CurrentEvent?.StartMonth.ToString("yyyy-MM") ?? oldPartition ?? "_tombstones";
            if (oldPartition is not null)
            {
                partitionRows[oldPartition].RemoveAll(item => item.State.Key.OutageClassificationId == id);
                changedPartitions.Add(oldPartition);
            }
            if (!partitionRows.TryGetValue(partition, out var rows)) partitionRows.Add(partition, rows = []);
            rows.Add(canonical);
            changedPartitions.Add(partition);
            exclusions.RemoveAll(exclusion => exclusion.EventId == id);
            var footprint = ranges.Current(result.State.CurrentEvent, lifecycle.ProcessableDate);
            var pending = ranges.Union(previousPlan?.Footprint ?? [], previousPlan?.Pending ?? [], footprint);
            var plan = new OutageLifecyclePlan(workId, id, (previousPlan?.Generation ?? 0) + 1, pending, footprint,
                work.Operation, "SourceChange", work.Batch, candidate.Origins.Select(origin => origin.RawPath).Distinct().ToArray(), work.Message.OriginalMessageBody);
            var planPath = await artifacts.WritePlanAsync(lease, plan, cancellationToken);
            var active = result.State.CurrentEvent;
            var continuation = active is not null && (active.EndUtc is null || active.EndUtc >
                lifecycle.ProcessableDate.AddDays(1).ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc));
            updates.Add(new(new(workId, id, partition, eventRevision, planPath, continuation,
                lifecycle.ProcessableDate, Due(pending.Count > 0)), plan));
            telemetry.Information("Outage mutation planned. EventId={EventId} EventRevision={EventRevision} IsDeleted={IsDeleted} PendingRanges={PendingRanges}",
                id, eventRevision, result.State.IsDeleted, pending.Count);
        }
        var changed = new List<EventMonthReference>();
        var months = loaded.Catalog.Months.ToDictionary(month => month.PartitionKey, StringComparer.Ordinal);
        if (partitionRows.Count > options.Value.EventLifecycle.MaximumPartitions)
            throw new WorkValidationException("Outage canonical partition bound exceeded.");
        foreach (var partition in changedPartitions.Order(StringComparer.Ordinal))
        {
            var revision = months.GetValueOrDefault(partition)?.Revision ?? 0;
            var reference = await artifacts.WriteMonthAsync(lease, partition, revision + 1,
                partitionRows[partition].OrderBy(item => item.State.Key.OutageClassificationId, StringComparer.Ordinal).ToArray(), cancellationToken);
            months[partition] = reference;
            changed.Add(reference);
        }
        var catalog = new OutageCatalog(1, months.Values.OrderBy(month => month.PartitionKey, StringComparer.Ordinal).ToArray(), exclusions, loaded.Catalog.Captures);
        var path = await artifacts.WriteCatalogAsync(lease, catalog, cancellationToken);
        return new(catalog, path, changed, updates, work.Message.Members.Select(member => member.MemberKey).ToArray());
    }

    private async Task<OutagePublication> BuildReconciliationAsync(OutageCatalogLease lease, OutageWork work,
        IReadOnlyList<CanonicalOutage> incoming, LoadedOutageCatalog loaded, CancellationToken cancellationToken)
    {
        if (work.Message.ReconciliationMode != CAT.Messaging.Contracts.EventReconciliationModes.Authoritative ||
            work.Message.SourceVersionCutoffUtc is not { } sourceVersionCutoff)
            throw new WorkValidationException("Authoritative Outage reconciliation requires a verified source-version cutoff.");
        var scope = work.Message.SnapshotScope!;
        var present = incoming.Select(item => item.State.Key.OutageClassificationId).ToHashSet(StringComparer.Ordinal);
        var exclusions = loaded.Catalog.Exclusions.ToList();
        var effective = scopeProjector.EffectiveScope(work, loaded.Catalog);
        foreach (var interval in effective)
            exclusions = exclusions.SelectMany(exclusion => present.Contains(exclusion.EventId)
                ? scopeProjector.RestorePresence(exclusion, interval) : [exclusion]).ToList();
        foreach (var item in loaded.Events.Values)
        {
            var active = item.State.CurrentEvent;
            if (active is null || present.Contains(item.State.Key.OutageClassificationId) || !scope.WtgIds.Contains(active.Turbine.WtgId) ||
                !active.Overlaps(scope.StartInclusiveUtc.UtcDateTime, scope.EndExclusiveUtc.UtcDateTime) ||
                item.State.LastUpdatedUtc > sourceVersionCutoff.UtcDateTime) continue;
            // Capture tokens fence snapshot order; source timestamps fence event versions.
            if (item.Origins.Any(origin => origin.ExtractionToken > work.Message.ExtractionToken) ||
                sourceVersionCutoff.UtcDateTime < item.State.LastUpdatedUtc) continue;
            foreach (var interval in effective.Where(interval => interval.WtgId == active.Turbine.WtgId))
                exclusions.Add(new(item.State.Key.OutageClassificationId, item.EventRevision, active.Turbine.WtgId,
                    interval.StartInclusiveUtc, interval.EndExclusiveUtc, interval.ExtractionToken));
        }
        var captures = (loaded.Catalog.Captures ?? []).Concat(effective.Select(capture =>
            capture with { SourceVersionCutoffUtc = sourceVersionCutoff.UtcDateTime })).Distinct().ToArray();
        if (captures.Length + exclusions.Count > options.Value.MaximumRowsPerArtifact)
            throw new WorkValidationException("Outage snapshot metadata exceeds the configured bound.");
        var catalog = loaded.Catalog with { Exclusions = exclusions.Distinct().ToArray(), Captures = captures };
        var path = await artifacts.WriteCatalogAsync(lease, catalog, cancellationToken);
        var end = DateOnly.FromDateTime(scope.EndExclusiveUtc.UtcDateTime.AddTicks(-1));
        if (end > lifecycle.ProcessableDate) end = lifecycle.ProcessableDate;
        var start = DateOnly.FromDateTime(scope.StartInclusiveUtc.UtcDateTime);
        var workId = OutageWorkKeys.Scope(work);
        var footprint = ranges.Union(effective.Select(interval =>
        {
            var first = DateOnly.FromDateTime(interval.StartInclusiveUtc);
            var last = DateOnly.FromDateTime(interval.EndExclusiveUtc.AddTicks(-1));
            return new OutageRange(interval.WtgId, work.Turbines.Single(turbine => turbine.WtgId == interval.WtgId).WtgName,
                first, last > lifecycle.ProcessableDate ? lifecycle.ProcessableDate : last);
        }).Where(range => range.StartInclusive <= range.EndInclusive));
        var plan = new OutageLifecyclePlan(workId, null, 1, footprint, footprint, work.Operation, "SnapshotReconciliation", work.Batch, work.Message.RawPaths, work.Message.OriginalMessageBody);
        var planPath = await artifacts.WritePlanAsync(lease, plan, cancellationToken);
        var update = new OutageLifecycleUpdate(new(workId, null, null, 0, planPath, false, end, Due(footprint.Count > 0)), plan);
        telemetry.Information("Outage snapshot scope planned after receipt barrier. ExpectedBatches={ExpectedBatches} PresentEvents={PresentEvents} ScopeWtgs={ScopeWtgs}",
            work.Message.ExpectedEventBatchIds.Length, present.Count, scope.WtgIds.Length);
        return new(catalog, path, [], [update], [workId]);
    }

    private DateTime Due(bool pending) => pending ? clock.GetUtcNow().UtcDateTime : clock.GetUtcNow().UtcDateTime.Date.AddDays(1);

    private static bool IsAtOrBeforeSnapshotFence(CanonicalOutage candidate, CanonicalOutage? previous,
        IReadOnlyList<OutageSnapshotCapture>? captures)
    {
        var change = candidate.State.LatestChange;
        var affectedEvents = new[] { previous?.State.CurrentEvent, candidate.State.CurrentEvent }
            .Where(item => item is not null).Cast<OutageEvent>().ToArray();
        return (captures ?? []).Any(capture => capture.SourceVersionCutoffUtc is { } cutoff &&
            change.LastUpdatedUtc <= cutoff &&
            (affectedEvents.Any(item => item.Turbine.WtgId == capture.WtgId &&
                 item.Overlaps(capture.StartInclusiveUtc, capture.EndExclusiveUtc)) ||
             affectedEvents.Length == 0 && change is OutageDeletion && change.Turbine.WtgId == capture.WtgId));
    }
}
