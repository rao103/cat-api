using Harmonizer.Core.Models;
namespace Harmonizer.Core.SourceTypes.Production.Models;

// Reference records group related values without weakening numeric IDs or UTC timestamp types.
public sealed record ProductionValues(
    double? ActualProductionKWh,
    double? PossibleProductionKWh,
    double? LostProductionCapacitykWh,
    double? ContractualPoPEColumn,
    double? SiteAverageWindSpeedMps);

public sealed record ProductionRow(
    int WtgId,
    string WtgName,
    DateTime StartTimeInterval,
    DateTime EndTimeInterval,
    DateTime LastUpdatedDate,
    ProductionValues Values,
    ArtifactReference Origin);

public sealed record ProductionSnapshot(IReadOnlyList<ProductionRow> Rows, string TransformVersion = "production-v1");
public sealed record MergeResult(ProductionSnapshot Snapshot, string CanonicalHash, bool Changed, bool Complete);
