using System.Diagnostics;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Observability;
using Harmonizer.Core.Operations;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Microsoft.Extensions.Options;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

public sealed class OutageLifecycleProcessor(IOutageControlRepository state, IOutageArtifacts artifacts,
    OutageCanonicalReader canonicalReader, OutageRangePlanner ranges, OutageDayMaterializer materializer,
    OutageScopeProjector scopeProjector, TimeProvider clock, IOptions<HarmonizerOptions> options,
    ITelemetry<OutageLifecycleProcessor> telemetry, ITelemetry<AsyncHeartbeat> heartbeatTelemetry)
{
    public async Task ProcessFarmAsync(WindfarmIdentity farm, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var lease = await state.ClaimAsync(farm, cancellationToken);
        using var operation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await using var heartbeat = new AsyncHeartbeat(TimeSpan.FromSeconds(options.Value.HeartbeatSeconds),
            token => state.RenewAsync(lease, token), operation, heartbeatTelemetry);
        try
        {
            await ProcessClaimedAsync(lease, operation.Token);
            await heartbeat.StopAsync();
        }
        catch (Exception error)
        {
            if (error is not OperationCanceledException)
            {
                HarmonizerDiagnostics.LifecycleFailures.Add(1, new KeyValuePair<string, object?>("source.type", "Outage"));
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                try { await state.RecordFailureAsync(lease, error.GetType().Name, timeout.Token); }
                catch (Exception failure) { telemetry.Error(failure, "Could not record lifecycle failure."); }
                telemetry.Warning(error, "Outage lifecycle pass failed. WindfarmId={WindfarmId} ErrorCode={ErrorCode}", farm.WindfarmId, error.GetType().Name);
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
                HarmonizerDiagnostics.LifecycleDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds,
                    new KeyValuePair<string, object?>("source.type", "Outage"));
            }
        }
    }

    // Also called by the source processor while it owns the same farm/source fence.
    public async Task ProcessClaimedAsync(OutageCatalogLease lease, CancellationToken cancellationToken)
    {
        using var activity = HarmonizerDiagnostics.Activities.StartActivity("harmonizer.outage.lifecycle");
        using var scope = telemetry.BeginScope(new Dictionary<string, object?>
        { ["WindfarmId"] = lease.Windfarm.WindfarmId, ["WindfarmName"] = lease.Windfarm.WindfarmName,
          ["SourceType"] = "Outage", ["FenceToken"] = lease.Token, ["PipelineStage"] = "harmonizer" });
        var loaded = await canonicalReader.ReadAsync(lease, cancellationToken);
        var states = await state.ReadLifecycleAsync(lease.Windfarm.WindfarmId, cancellationToken);
        var remaining = options.Value.EventLifecycle.DaysPerPass;
        foreach (var current in states.Where(item => item.DueUtc <= clock.GetUtcNow().UtcDateTime))
        {
            if (remaining == 0) break;
            var update = await PrepareAsync(lease, current, loaded, cancellationToken);
            while (update.Plan.Pending.Count > 0 && remaining > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var range = update.Plan.Pending[0];
                var date = range.StartInclusive;
                var key = new WtgDayKey(lease.Windfarm.WindfarmId, range.WtgId, date, "Outage");
                using var dayScope = telemetry.BeginScope(new Dictionary<string, object?>
                { ["WtgId"] = range.WtgId, ["WtgName"] = range.WtgName, ["DataDate"] = date,
                  ["EventId"] = current.EventId, ["PlanningGeneration"] = update.Plan.Generation,
                  ["OperationId"] = update.Plan.Operation.OperationId, ["ExecutionId"] = update.Plan.Operation.ExecutionId,
                  ["TriggerKind"] = update.Plan.TriggerKind });
                using var dayActivity = HarmonizerDiagnostics.Activities.StartActivity("harmonizer.outage.day");
                var rows = BuildDay(key, loaded);
                var revision = await state.ReadDayRevisionAsync(key, cancellationToken);
                var trigger = update.Plan.EventId is { } eventId && loaded.Events.TryGetValue(eventId, out var triggeringEvent)
                    ? new OutageDayEvidence(triggeringEvent, loaded.References[eventId]) : null;
                var candidate = await artifacts.WriteDayAsync(lease, update.Plan, range, date, revision, rows, trigger, cancellationToken);
                var next = ranges.ConsumeFirst(update.Plan);
                var path = await artifacts.WritePlanAsync(lease, next, cancellationToken);
                update = new(update.State with { PlanPath = path, DueUtc = NextDue(next.Pending.Count > 0) }, next);
                await state.AdvanceAsync(lease, update, candidate, cancellationToken);
                HarmonizerDiagnostics.Committed.Add(1, new KeyValuePair<string, object?>("source.type", "Outage"));
                HarmonizerDiagnostics.LifecycleDays.Add(1, new KeyValuePair<string, object?>("source.type", "Outage"),
                    new KeyValuePair<string, object?>("trigger.kind", update.Plan.TriggerKind));
                dayActivity?.SetStatus(ActivityStatusCode.Ok);
                remaining--;
            }
            if (update.Plan.Pending.Count == 0)
                await state.AdvanceAsync(lease, update, null, cancellationToken);
        }
        await state.CompleteReadyBatchesAsync(lease.Windfarm.WindfarmId, cancellationToken);
        telemetry.Information("Outage lifecycle pass completed. DaysProcessed={DaysProcessed}", options.Value.EventLifecycle.DaysPerPass - remaining);
    }

    private async Task<OutageLifecycleUpdate> PrepareAsync(OutageCatalogLease lease, OutageLifecycleState current,
        LoadedOutageCatalog loaded, CancellationToken cancellationToken)
    {
        var plan = await artifacts.ReadPlanAsync(current.PlanPath, cancellationToken);
        if (plan.WorkId != current.WorkId || plan.EventId != current.EventId)
            throw new WorkValidationException("Lifecycle plan identity disagrees with its committed control pointer.");
        if (plan.Pending.Count > 0 || !current.NeedsContinuation) return new(current, plan);
        var through = ProcessableDate;
        if (current.ContinuedThrough >= through) return new(current with { DueUtc = NextDue(false) }, plan);
        if (current.EventId is null || !loaded.Events.TryGetValue(current.EventId, out var item))
            throw new WorkValidationException("Continuation lost its canonical event locator.");
        var footprint = ranges.Current(item.State.CurrentEvent, through);
        var firstNew = current.ContinuedThrough?.AddDays(1);
        var pending = footprint.Where(range => firstNew is null || range.EndInclusive >= firstNew)
            .Select(range => firstNew is { } first && first > range.StartInclusive ? range with { StartInclusive = first } : range).ToArray();
        var next = new OutageLifecyclePlan(plan.WorkId, plan.EventId, plan.Generation + 1, pending, footprint,
            OperationIdentity.Untracked, "ClockContinuation", null, item.Origins.Select(origin => origin.RawPath).Distinct().ToArray());
        var path = await artifacts.WritePlanAsync(lease, next, cancellationToken);
        var active = item.State.CurrentEvent;
        var needsContinuation = active is not null && (active.EndUtc is null || active.EndUtc > through.AddDays(1).ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc));
        var update = new OutageLifecycleUpdate(current with { PlanPath = path, ContinuedThrough = through,
            NeedsContinuation = needsContinuation, DueUtc = NextDue(pending.Length > 0) }, next);
        await state.AdvanceAsync(lease, update, null, cancellationToken);
        telemetry.Information("Outage clock continuation planned. EventId={EventId} ThroughDate={ThroughDate} Generation={Generation}",
            current.EventId, through, next.Generation);
        return update;
    }

    private IReadOnlyList<OutageDayReference> BuildDay(WtgDayKey key, LoadedOutageCatalog loaded)
    {
        var result = new List<OutageDayReference>();
        foreach (var segment in materializer.Build(key, loaded.Events.Values.Select(item => item.State).ToArray()))
        {
            var id = segment.Key.OutageClassificationId;
            var item = loaded.Events[id];
            foreach (var projected in scopeProjector.Apply(segment, item.EventRevision, loaded.Catalog.Exclusions))
                result.Add(new(projected, item.EventRevision, loaded.References[id], item.Origins));
        }
        return result;
    }

    public DateOnly ProcessableDate => DateOnly.FromDateTime(clock.GetUtcNow().UtcDateTime).AddDays(options.Value.EventLifecycle.ProcessableDayOffset);
    private DateTime NextDue(bool pending) => pending ? clock.GetUtcNow().UtcDateTime : clock.GetUtcNow().UtcDateTime.Date.AddDays(1);
}
