namespace ProductionIngestor.Models;

/// <summary>A single changed source row discovered during window-scoped change discovery.</summary>
public sealed record ChangedProductionRowModel(
    DateTime LastUpdatedUtc,
    string ItemId,
    DateTime ProductionTimestampUtc,
    DateOnly ProductionDate);

/// <summary>A single Production measurement row from a Windfarm-Day snapshot read-back.</summary>
public sealed record ProductionRowModel(
    string WindfarmName,
    string WtgName,
    DateTime ProductionTimestampUtc,
    decimal? ActualProduction,
    decimal? PossibleProduction,
    decimal? LostProductionCapacitykWh,
    decimal? SiteAverageWindSpeedMps,
    DateTime LastUpdatedUtc,
    string? SourceRowRevision,
    DateTime EndDateAndTimeUtc,
    string? ContractualPoPEColumn);
