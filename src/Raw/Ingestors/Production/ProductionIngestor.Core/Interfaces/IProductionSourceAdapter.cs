using ProductionIngestor.Adapters.Source;
using ProductionIngestor.Models;

namespace ProductionIngestor.Interfaces;

/// <summary>
/// Abstraction over the production source system. A fake/in-memory implementation backs
/// unit tests; <see cref="SqlProductionSourceAdapter"/> is the real SQL Server-backed adapter.
/// </summary>
public interface IProductionSourceAdapter
{
    /// <summary>
    /// Reads the latest visible source update timestamp for the wind farm.
    /// A null timestamp means no source rows; provider failures are not empty results.
    /// </summary>
    Task<LatestSourceUpdatedDateModel> GetLatestSourceUpdatedDateByWindfarmIdAsync(
        WindfarmSourceQueryModel query, CancellationToken cancellationToken);

    /// <summary>
    /// Keyset-paginated change discovery within a half-open [low, end) window,
    /// ordered by update timestamp, item identifier, and production timestamp.
    /// The adapter enumerates every page, including the lag-overlap window.
    /// </summary>
    IAsyncEnumerable<ChangedProductionRowModel> GetChangedRowsAsync(
        string windfarmName,
        DateTime low,
        DateTime end,
        CancellationToken cancellationToken);

    /// <summary>Reads the complete authoritative Windfarm-Day snapshot for one UTC production day.</summary>
    IAsyncEnumerable<ProductionRowModel> GetWindfarmDaySnapshotAsync(
        string windfarmName,
        DateTime dayStartUtc,
        DateTime dayEndUtc,
        CancellationToken cancellationToken);

    /// <summary>Streams one scoped refresh query. Empty turbine scope reads no rows.
    /// Scope and retention predicate match incremental eligibility; this is not a database-wide snapshot.</summary>
    IAsyncEnumerable<ProductionRowModel> GetRefreshSnapshotAsync(
        string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc,
        DateTime dayStartUtc, DateTime dayEndUtc, CancellationToken cancellationToken);

    /// <summary>
    /// Freezes this run's upper query boundary: the maximum <c>LastUpdatedUtc</c> currently visible
    /// for the given active turbine set (scoped by <paramref name="minRetrieveDateUtc"/>). Null means
    /// no eligible source row exists at all. This value, together with the committed checkpoint, is
    /// used to plan windows locally (see <c>ProductionChangeWindowPlanner</c>) — it is not itself a
    /// planning/grouping query.
    /// </summary>
    Task<DateTime?> GetLatestLastUpdatedUtcAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime minRetrieveDateUtc,
        CancellationToken cancellationToken);

    /// <summary>
    /// Reads full changed-row payloads (identity, production timestamp and measurement values)
    /// for the active turbine set whose <c>LastUpdatedUtc</c> falls within the inclusive
    /// <c>[windowMinInclusiveUtc, windowMaxInclusiveUtc]</c> range planned by
    /// <c>ProductionChangeWindowPlanner</c>, keyset-paginated and ordered by
    /// <c>(StartDateAndTimeUTC, WtgRDSPPID)</c> — the source's unique production key, not the
    /// update-time query axis. Every call opens its own connection/reader and starts from an empty
    /// page cursor. Rows are returned exactly as the source returns them, with no full-day
    /// read-back or aggregation.
    /// </summary>
    IAsyncEnumerable<ProductionChangeRowModel> GetProductionChangeRowsAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime minRetrieveDateUtc,
        DateTime windowMinInclusiveUtc,
        DateTime windowMaxInclusiveUtc,
        CancellationToken cancellationToken);
}
