using System.Globalization;
using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Production.Models;
using Microsoft.Extensions.Options;
using Turbine = Harmonizer.Core.Models.TurbineIdentity;
namespace Harmonizer.Core.SourceTypes.Production.Serialization;

public sealed class ProductionWorkMessageReader(IOptions<HarmonizerOptions> options, ProductionWorkV2Reader v2Reader,
    ITelemetry<ProductionWorkMessageReader> telemetry, IRawObjectStore rawObjects)
{
    public async Task<ProductionWork> ReadAsync(string body, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var document = JsonDocument.Parse(body, new() { MaxDepth = 32 });
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new WorkValidationException("Work must be a JSON object.");
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in root.EnumerateObject())
        {
            if (!names.Add(property.Name))
            {
                throw new WorkValidationException("Duplicate JSON field.");
            }
        }

        var source = root.GetProperty("SourceType").GetString();
        var schema = root.GetProperty("SchemaVersion").GetInt32();
        if (source != "Production")
        {
            throw new UnsupportedWorkException($"{source} work requires its verified Raw schema and event adapter; message remains unacknowledged.");
        }

        var operation = OperationIdentityReader.Read(root, root.GetProperty("OriginalMessageBody").GetString()
            ?? throw new WorkValidationException("OriginalMessageBody is required."));
        using var operationScope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["OperationId"] = operation.OperationId,
            ["ExecutionId"] = operation.ExecutionId,
            ["PipelineStage"] = "harmonizer"
        });
        cancellationToken.ThrowIfCancellationRequested();
        var wire = schema switch
        {
            2 => await v2Reader.ReadAsync(body, cancellationToken),
            3 => JsonSerializer.Deserialize<ProductionWtgDayWorkV3>(body, HarmonizerJson.Options)
                ?? throw new WorkValidationException("Missing work."),
            _ => throw new UnsupportedWorkException($"Unsupported Production work schema {schema}; expected V2 or V3.")
        };

        if (wire.ItemTurbines is null || wire.ScopeTurbines is null || wire.RawPaths is null ||
            wire.ArtifactId is null || wire.BatchId is null || wire.OriginalMessageBody is null)
        {
            throw new WorkValidationException("Production work is missing required identities, paths or envelope fields.");
        }

        var snapshot = wire.PayloadKind == "ProductionSnapshot";
        if ((!snapshot && wire.PayloadKind != "ProductionChanges") || wire.PayloadSchemaVersion != 2 ||
            (wire.WorkKind != HarmonizationWorkKinds.ProductionWtgDays && !(snapshot && wire.WorkKind == HarmonizationWorkKinds.ProductionSnapshotReconciliation)) ||
            wire.ExtractionMode != (snapshot ? "Refresh" : "Incremental"))
        {
            throw new WorkValidationException("WorkKind, payload, mode and schema do not agree.");
        }

        if (wire.WindfarmId <= 0 || string.IsNullOrWhiteSpace(wire.WindfarmName) || wire.ExtractionToken < 0 ||
            string.IsNullOrWhiteSpace(wire.RequestId) || string.IsNullOrWhiteSpace(wire.AttemptId))
        {
            throw new WorkValidationException("Numeric farm ID, name and extraction identity are required.");
        }

        if (!IsHash(wire.ArtifactId) || !IsHash(wire.BatchId))
        {
            throw new WorkValidationException("ArtifactId and BatchId must be lowercase SHA-256 values.");
        }

        var reconciliation = wire.WorkKind == HarmonizationWorkKinds.ProductionSnapshotReconciliation;
        var members = reconciliation || (snapshot && wire.HasNoSourceRows) ? wire.ScopeTurbines : wire.ItemTurbines;
        ValidateMembers(members, allowEmpty: snapshot && wire.HasNoSourceRows);
        if (wire.ItemTurbines.Any(item => !members.Contains(item)))
        {
            throw new WorkValidationException("Actual members must be contained in the declared scope.");
        }

        if (wire.RawPaths.Length > options.Value.MaximumRawPaths ||
            wire.RawPaths.Distinct(StringComparer.Ordinal).Count() != wire.RawPaths.Length ||
            wire.RawPaths.Any(path => !ObjectPath.IsSafe(path)))
        {
            throw new WorkValidationException("Invalid, duplicate or excessive Raw paths.");
        }

        if (!snapshot && (wire.RawPaths.Length != 1 || wire.IsExtractionReady || wire.HasNoSourceRows || wire.AllowRemovalReconciliation))
        {
            throw new WorkValidationException("ProductionChanges must reference one exact nonempty Raw part without snapshot flags.");
        }

        if (snapshot && !wire.IsExtractionReady)
        {
            throw new WorkValidationException("Refresh requires extraction completeness evidence.");
        }

        if (snapshot && wire.AllowRemovalReconciliation)
        {
            throw new UnsupportedWorkException("Destructive refresh requires a documented SourceVersionEvidence format/as-of guarantee. Opaque evidence is not sufficient.");
        }

        var work = new ProductionWork(new(wire.ArtifactId, wire.BatchId), new(wire.WindfarmId, wire.WindfarmName),
            ParseDate(wire.DataDate), members.Select(member => new Turbine(member.WtgId, member.WtgName)).OrderBy(x => x.WtgId).ToArray(),
            Array.AsReadOnly(wire.RawPaths), wire.ExtractionToken, wire.RequestId, wire.AttemptId, snapshot,
            wire.HasNoSourceRows, wire.AllowRemovalReconciliation, wire.SourceVersionEvidence, wire.OriginalMessageBody, body, wire.TraceId)
        {
            Operation = operation
        };
        if (snapshot)
        {
            work = work with { ResolvedRefresh = await ProductionRefreshResolver.ResolveAsync(
                work.OriginalMessageBody, rawObjects, options.Value.MaximumArtifactBytes, cancellationToken) };
            RefreshManifestValidator.Validate(work);
        }

        telemetry.Debug("Production work validated. SchemaVersion={SchemaVersion} BatchId={BatchId} WindfarmId={WindfarmId} DataDate={DataDate} TurbineCount={TurbineCount} OperationId={OperationId} ExecutionId={ExecutionId}",
            schema, work.Batch.BatchId, work.Windfarm.WindfarmId, work.DataDate, work.Turbines.Count,
            work.Operation.OperationId, work.Operation.ExecutionId);
        return work;
    }

    private void ValidateMembers(ProductionTurbineIdentityV1[] members, bool allowEmpty)
    {
        if ((!allowEmpty && members.Length == 0) || members.Length > options.Value.MaximumTurbinesPerBatch ||
            members.Any(member => member is null || member.WtgId <= 0 || string.IsNullOrWhiteSpace(member.WtgName)) ||
            members.Select(member => member.WtgId).Distinct().Count() != members.Length ||
            members.Select(member => member.WtgName).Distinct(StringComparer.Ordinal).Count() != members.Length)
        {
            throw new WorkValidationException("Expected a bounded set of unique numeric turbine IDs and separate names.");
        }
    }

    public static DateOnly ParseDate(string value) =>
        DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date)
            ? date : throw new WorkValidationException("DataDate must be an ISO UTC calendar date.");
    private static bool IsHash(string value) => value.Length == 64 && value.All(character => character is >= '0' and <= '9' or >= 'a' and <= 'f');
}
