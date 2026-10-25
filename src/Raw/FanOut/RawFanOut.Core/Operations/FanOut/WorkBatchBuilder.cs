using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Serialization;

namespace CAT.RawFanOut.Operations.FanOut;

internal sealed class WorkBatchBuilder(FanOutPlanningContext context, FanOutOptions options)
{
    private readonly List<OutgoingMessage> messages = [];
    private readonly List<string> outageEventBatchIds = [];
    private WorkSelection? outageSnapshotBarrier;
    private readonly Dictionary<string, RawIdentity[]> outageRowsByEvent = context.Artifact.SourceType == "Outage"
        ? context.Rows.Where(row => row.EventId is not null)
            .GroupBy(row => row.EventId!, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.ToArray(), StringComparer.Ordinal)
        : new(StringComparer.Ordinal);
    private long plannedBytes;
    public IReadOnlyList<OutgoingMessage> Messages => messages;

    public void Add(WorkSelection selection)
    {
        if (context.Artifact.SourceType == "Outage" &&
            selection.WorkKind == HarmonizationWorkKinds.EventSnapshotReconciliation)
        {
            if (outageSnapshotBarrier is not null) throw new InvalidDataException("An Outage plan cannot contain multiple snapshot barriers.");
            outageSnapshotBarrier = selection;
            return;
        }

        if (selection.ItemIds.Length > 0)
        {
            foreach (var items in selection.ItemIds.Chunk(options.ItemBatchSize)) Append(selection with { ItemIds = items });
        }
        else if (selection.EventIds.Length > 0)
        {
            foreach (var events in selection.EventIds.Chunk(options.EventBatchSize)) Append(selection with { EventIds = events });
        }
        else Append(selection);
    }

    public void Complete()
    {
        if (outageSnapshotBarrier is not null) Append(outageSnapshotBarrier);
    }

    private void Append(WorkSelection selection)
    {
        var artifact = context.Artifact;
        var batchId = artifact.SourceType == "Outage"
            ? RawEnvelope.Hash(JsonSerializer.Serialize(new
            {
                Version = 3,
                artifact.ArtifactId,
                Selection = selection,
                SnapshotScope = artifact.EventSnapshotScope
            }))
            : RawEnvelope.Hash(JsonSerializer.Serialize(new { Version = 2, artifact.ArtifactId, Selection = selection }));
        var body = artifact.SourceType == "Production"
            ? ProductionWorkSerializer.Serialize(context, selection, batchId)
            : artifact.SourceType == "Outage"
                ? SerializeOutage(selection, batchId)
            : JsonSerializer.Serialize(new HarmonizationWorkV2
            {
                WorkKind = selection.WorkKind, ArtifactId = artifact.ArtifactId, BatchId = batchId,
                SourceType = artifact.SourceType, PayloadKind = artifact.PayloadKind,
                WindfarmId = context.Farm.WindfarmId, WindfarmName = context.Farm.WindfarmName,
                DataDate = selection.DataDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                ExtractionToken = artifact.ExtractionToken, RawPaths = artifact.Paths, RunId = artifact.RunId,
                TraceId = artifact.TraceId, OperationId = artifact.OperationId, ItemIds = selection.ItemIds, EventIds = selection.EventIds,
                Turbines = context.Farm.Turbines.Where(turbine => selection.ItemIds.Contains(turbine.WtgId)).OrderBy(turbine => turbine.WtgId).ToArray(),
                Snapshot = artifact.Snapshot, OriginalMessageBody = artifact.OriginalBody
            });
        var bytes = Encoding.UTF8.GetByteCount(body);
        if (bytes > options.MaxMessageBytes)
        {
            if (selection.ItemIds.Length > 1)
            {
                var middle = selection.ItemIds.Length / 2;
                Append(selection with { ItemIds = selection.ItemIds[..middle] });
                Append(selection with { ItemIds = selection.ItemIds[middle..] });
                return;
            }
            if (selection.EventIds.Length > 1)
            {
                var middle = selection.EventIds.Length / 2;
                Append(selection with { EventIds = selection.EventIds[..middle] });
                Append(selection with { EventIds = selection.EventIds[middle..] });
                return;
            }
            throw new InvalidDataException("Artifact metadata exceeds the output message byte limit.");
        }
        plannedBytes += bytes;
        if (plannedBytes > options.MaxPlanBytes) throw new InvalidDataException("Fan-out plan byte limit exceeded.");
        messages.Add(new OutgoingMessage(batchId, body));
        if (artifact.SourceType == "Outage" && selection.WorkKind == HarmonizationWorkKinds.CanonicalEvents)
            outageEventBatchIds.Add(batchId);
    }

    private string SerializeOutage(WorkSelection selection, string batchId)
    {
        var artifact = context.Artifact;
        var members = selection.WorkKind == HarmonizationWorkKinds.CanonicalEvents
            ? selection.EventIds.SelectMany(eventId => outageRowsByEvent.TryGetValue(eventId, out var rows)
                    ? rows
                    : throw new InvalidDataException("Outage event selection has no source rows."))
                .GroupBy(row => (EventId: row.EventId!, WtgId: row.WtgId
                    ?? throw new InvalidDataException("Outage member WTG identity is required.")))
                .Select(group =>
                {
                    var turbineNames = group.Select(row => row.WtgName).Distinct(StringComparer.Ordinal).ToArray();
                    if (turbineNames.Length != 1 || string.IsNullOrWhiteSpace(turbineNames[0]))
                        throw new InvalidDataException("Outage member turbine name is missing or inconsistent.");
                    var rawPaths = group.Select(row => row.RawPath
                            ?? throw new InvalidDataException("Outage member Raw path is required."))
                        .Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
                    var eventKey = RawEnvelope.Hash(JsonSerializer.Serialize(new
                    {
                        artifact.SourceType,
                        artifact.WindfarmId,
                        EventId = group.Key.EventId
                    }));
                    var memberKey = RawEnvelope.Hash(JsonSerializer.Serialize(new
                    {
                        EventKey = eventKey,
                        group.Key.WtgId
                    }));
                    return new EventWorkMemberV3(eventKey, memberKey, group.Key.EventId, group.Key.WtgId,
                        turbineNames[0], rawPaths);
                })
                .OrderBy(member => member.EventKey, StringComparer.Ordinal)
                .ThenBy(member => member.MemberKey, StringComparer.Ordinal)
                .ToArray()
            : [];
        var isSnapshot = artifact.PayloadKind == "OutageSnapshot";
        var isBarrier = selection.WorkKind == HarmonizationWorkKinds.EventSnapshotReconciliation;
        var reconciliationMode = artifact.CanonicalEnvelope is not null && isSnapshot && isBarrier
            ? EventReconciliationModes.ReceiptOnly
            : !isSnapshot || !isBarrier
            ? null
            : artifact.IsFullSnapshot && artifact.EventScopeWtgIdsSpecified &&
              artifact.ExpectedTotalRowCount is not null && artifact.SourceVersionCutoffUtc is not null
                ? EventReconciliationModes.Authoritative
                : EventReconciliationModes.ReceiptOnly;
        var scope = artifact.EventSnapshotScope;
        var rawPaths = selection.WorkKind == HarmonizationWorkKinds.CanonicalEvents
            ? members.SelectMany(member => member.RawPaths).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray()
            : artifact.Paths;

        return JsonSerializer.Serialize(new EventWorkV3
        {
            WorkKind = selection.WorkKind,
            ArtifactId = artifact.ArtifactId,
            BatchId = batchId,
            SourceType = artifact.SourceType,
            PayloadKind = artifact.PayloadKind,
            WindfarmId = context.Farm.WindfarmId,
            WindfarmName = context.Farm.WindfarmName,
            ExtractionToken = artifact.ExtractionToken,
            RunId = artifact.RunId,
            AttemptId = artifact.AttemptId ?? RawArtifactConventions.AttemptId(artifact.ExtractionToken),
            OperationId = artifact.OperationId,
            TraceId = artifact.TraceId,
            ExtractionMode = artifact.ExtractionMode,
            ManifestPath = artifact.ManifestPath,
            IsFullSnapshot = artifact.IsFullSnapshot,
            ReconciliationMode = reconciliationMode,
            SnapshotObservedAtUtc = artifact.SnapshotObservedAtUtc,
            SourceVersionCutoffUtc = artifact.SourceVersionCutoffUtc,
            SnapshotScope = scope is null ? null : new EventSnapshotScopeV3
            {
                Kind = scope.Kind,
                StartInclusiveUtc = scope.StartInclusiveUtc,
                EndExclusiveUtc = scope.EndExclusiveUtc,
                WtgIds = scope.WtgIds
            },
            Members = members,
            EventIds = selection.EventIds,
            RawPaths = rawPaths,
            ExpectedEventBatchIds = selection.WorkKind == HarmonizationWorkKinds.EventSnapshotReconciliation
                ? outageEventBatchIds.Order(StringComparer.Ordinal).ToArray()
                : [],
            OriginalMessageBody = artifact.OriginalBody
        });
    }
}
