namespace ProductionIngestor.Models;

/// <summary>
/// One changed source row with its full measurement payload, as returned by a window-scoped
/// <c>LastUpdatedUtc</c> query. Unlike <see cref="ChangedProductionRowModel"/> (identity only, used
/// for legacy day discovery), this carries the values persisted directly into a ProductionChanges
/// Raw part with no full-day read-back.
/// </summary>
public sealed record ProductionChangeRowModel(
    string WtgName,
    DateTime ProductionTimestampUtc,
    DateTime EndDateAndTimeUtc,
    DateTime LastUpdatedUtc,
    decimal? ActualProduction,
    decimal? PossibleProduction,
    string? ContractualPoPEColumn,
    decimal? LostProductionCapacitykWh,
    decimal? SiteAverageWindSpeedMps);
