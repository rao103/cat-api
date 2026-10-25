using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Serialization;

/// <summary>Publishes numeric V3 work for incremental/refresh inputs and retains the legacy daily fallback.</summary>
internal static class ProductionWorkSerializer
{
    public static string Serialize(FanOutPlanningContext context, WorkSelection selection, string batchId)
    {
        var artifact = context.Artifact;
        var byId = context.Farm.Turbines.ToDictionary(turbine => turbine.WtgId);
        var items = selection.ItemIds.Select(id => byId[id]).OrderBy(turbine => turbine.WtgName, StringComparer.Ordinal).ToArray();
        var scope = selection.ScopeItemIds.Select(id => byId[id]).OrderBy(turbine => turbine.WtgName, StringComparer.Ordinal).ToArray();
        var date = selection.DataDate!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (artifact.IsExtractionReady || artifact.IsIncrementalChanges)
        {
            return JsonSerializer.Serialize(new ProductionWtgDayWorkV3(
                SchemaVersion: 3, WorkKind: selection.WorkKind,
                ArtifactId: artifact.ArtifactId, BatchId: batchId, SourceType: "Production",
                WindfarmId: context.Farm.WindfarmId, WindfarmName: context.Farm.WindfarmName, DataDate: date,
                ExtractionMode: artifact.ExtractionMode!, PayloadKind: artifact.PayloadKind!,
                PayloadSchemaVersion: artifact.PayloadSchemaVersion ?? 1,
                RequestId: artifact.RequestId ?? artifact.RunId, AttemptId: artifact.AttemptId ?? artifact.ExtractionToken.ToString(CultureInfo.InvariantCulture),
                ExtractionToken: artifact.ExtractionToken,
                RawPaths: selection.RawPaths,
                ItemTurbines: items.Select(turbine => new ProductionTurbineIdentityV1(turbine.WtgId, turbine.WtgName)).ToArray(),
                ScopeTurbines: scope.Select(turbine => new ProductionTurbineIdentityV1(turbine.WtgId, turbine.WtgName)).ToArray(),
                IsExtractionReady: artifact.IsExtractionReady, HasNoSourceRows: selection.HasNoSourceRows,
                AllowRemovalReconciliation: artifact.CanonicalEnvelope is null && !string.IsNullOrWhiteSpace(artifact.SourceVersionEvidence),
                SourceVersionEvidence: artifact.CanonicalEnvelope is null ? artifact.SourceVersionEvidence : null,
                OriginalMessageBody: artifact.OriginalBody, TraceId: artifact.TraceId) { OperationId = artifact.OperationId });
        }
        return JsonSerializer.Serialize(new ProductionWtgDayWorkV2(
            SchemaVersion: 2, WorkKind: selection.WorkKind,
            ArtifactId: artifact.ArtifactId, BatchId: batchId, SourceType: "Production",
            WindfarmId: artifact.WindfarmId.ToString(CultureInfo.InvariantCulture), WindfarmName: context.Farm.WindfarmName, DataDate: date,
            ExtractionMode: artifact.ExtractionMode ?? "Full", PayloadKind: artifact.PayloadKind ?? "ProductionSnapshot",
            PayloadSchemaVersion: artifact.PayloadSchemaVersion ?? 1,
            RequestId: artifact.RequestId ?? artifact.RunId, AttemptId: artifact.AttemptId ?? artifact.ExtractionToken.ToString(CultureInfo.InvariantCulture),
            ExtractionToken: artifact.ExtractionToken,
            RawPaths: selection.RawPaths, WtgNames: items.Select(turbine => turbine.WtgName).ToArray(), ScopeWtgIds: scope.Select(turbine => turbine.WtgId).ToArray(),
            IsExtractionReady: false, HasNoSourceRows: selection.HasNoSourceRows,
            AllowRemovalReconciliation: artifact.CanonicalEnvelope is null && !string.IsNullOrWhiteSpace(artifact.SourceVersionEvidence),
            SourceVersionEvidence: artifact.CanonicalEnvelope is null ? artifact.SourceVersionEvidence : null, OriginalMessageBody: artifact.OriginalBody, TraceId: artifact.TraceId, OperationId: artifact.OperationId));
    }
}
