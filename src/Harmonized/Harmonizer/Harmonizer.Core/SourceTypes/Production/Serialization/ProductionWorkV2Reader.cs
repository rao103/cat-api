using System.Globalization;
using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Microsoft.Extensions.Options;

namespace Harmonizer.Core.SourceTypes.Production.Serialization;

/// <summary>Resolves V2's incomplete identities before shared Production validation and processing.</summary>
public sealed class ProductionWorkV2Reader(IIdentityCatalog catalog, IOptions<HarmonizerOptions> options,
    ITelemetry<ProductionWorkV2Reader> telemetry)
{
    public async Task<ProductionWtgDayWorkV3> ReadAsync(string body, CancellationToken cancellationToken)
    {
        var wire = JsonSerializer.Deserialize<ProductionWtgDayWorkV2>(body, HarmonizerJson.Options)
            ?? throw new WorkValidationException("Missing V2 work.");
        if (wire.SchemaVersion != 2 || wire.SourceType != "Production" ||
            !int.TryParse(wire.WindfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var windfarmId) ||
            windfarmId <= 0 || string.IsNullOrWhiteSpace(wire.WindfarmName))
        {
            throw new WorkValidationException("V2 requires a positive decimal farm ID and a separate farm name.");
        }

        var reconciliation = wire.WorkKind == HarmonizationWorkKinds.ProductionSnapshotReconciliation;
        if (wire.WtgNames is null || wire.ScopeWtgIds is null ||
            wire.WtgNames.Length > options.Value.MaximumTurbinesPerBatch ||
            wire.ScopeWtgIds.Length > options.Value.MaximumTurbinesPerBatch ||
            wire.WtgNames.Any(string.IsNullOrWhiteSpace) || wire.ScopeWtgIds.Any(id => id <= 0) ||
            wire.WtgNames.Distinct(StringComparer.Ordinal).Count() != wire.WtgNames.Length ||
            wire.ScopeWtgIds.Distinct().Count() != wire.ScopeWtgIds.Length ||
            (reconciliation ? wire.ScopeWtgIds.Length == 0 : wire.WtgNames.Length == 0))
        {
            throw new WorkValidationException("V2 requires bounded, unique turbine names and valid declared scope IDs.");
        }

        var windfarm = new WindfarmIdentity(windfarmId, wire.WindfarmName);
        var date = ProductionWorkMessageReader.ParseDate(wire.DataDate);
        var items = wire.WtgNames.Length == 0
            ? []
            : await catalog.ResolveAsync(windfarm, wire.WtgNames, date, cancellationToken);
        var scope = wire.ScopeWtgIds.Length == 0
            ? []
            : await catalog.ResolveIdsAsync(windfarm, wire.ScopeWtgIds, date, cancellationToken);

        // Scope IDs and actual names describe separate sets. They must never be paired by array position.
        if (scope.Count > 0 && items.Any(item => !scope.Contains(item)))
        {
            throw new WorkValidationException("V2 actual turbine identities are outside the declared scope.");
        }

        telemetry.Debug("Production V2 identities normalized. WindfarmId={WindfarmId} ItemCount={ItemCount} ScopeCount={ScopeCount}",
            windfarmId, items.Count, scope.Count);
        return new(3, wire.WorkKind, wire.ArtifactId, wire.BatchId, wire.SourceType,
            windfarmId, wire.WindfarmName, wire.DataDate, wire.ExtractionMode, wire.PayloadKind,
            wire.PayloadSchemaVersion, wire.RequestId, wire.AttemptId, wire.ExtractionToken, wire.RawPaths,
            items.Select(item => new ProductionTurbineIdentityV1(item.WtgId, item.WtgName)).ToArray(),
            scope.Select(item => new ProductionTurbineIdentityV1(item.WtgId, item.WtgName)).ToArray(),
            wire.IsExtractionReady, wire.HasNoSourceRows, wire.AllowRemovalReconciliation,
            wire.SourceVersionEvidence, wire.OriginalMessageBody, wire.TraceId);
    }
}
