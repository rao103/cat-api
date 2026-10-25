using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.Operations.Extraction;

/// <summary>
/// Reads back and deduplicates a complete Windfarm-Day snapshot for one affected date.
/// A legitimately empty authoritative day still yields an (empty) snapshot — never silently skipped.
/// </summary>
public sealed class WindfarmDayExtractor
{
    private readonly IProductionSourceAdapter _adapter;
    private readonly int _maxSnapshotRows;

    public WindfarmDayExtractor(IProductionSourceAdapter adapter, int maxSnapshotRows = 1_000_000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSnapshotRows);
        _adapter = adapter;
        _maxSnapshotRows = maxSnapshotRows;
    }

    /// <summary>
    /// Streams the deduplicated snapshot for <paramref name="productionDate"/> (UTC calendar day).
    /// Rows sharing the same natural key (ItemId + ProductionTimestampUtc) are deduplicated,
    /// keeping the most recently updated source row.
    /// </summary>
    public async IAsyncEnumerable<ProductionRowModel> GetDeduplicatedSnapshotAsync(
        string windfarmName,
        DateOnly productionDate,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var dayStartUtc = productionDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
        var dayEndUtc = dayStartUtc.AddDays(1);

        var latestByKey = new Dictionary<(string ItemId, DateTime TimestampUtc), ProductionRowModel>();

        await foreach (var row in _adapter.GetWindfarmDaySnapshotAsync(windfarmName, dayStartUtc, dayEndUtc, cancellationToken))
        {
            var key = (row.WtgName, row.ProductionTimestampUtc);
            latestByKey.TryGetValue(key, out var existing);

            if (existing is not null
                && row.LastUpdatedUtc == existing.LastUpdatedUtc
                && HasContradictoryMeasurements(row, existing))
            {
                throw new InvalidDataException("Contradictory rows share a source key and update timestamp.");
            }

            if (existing is null || row.LastUpdatedUtc > existing.LastUpdatedUtc)
            {
                latestByKey[key] = row;
            }

            if (latestByKey.Count > _maxSnapshotRows)
            {
                throw new InvalidDataException("Snapshot row limit exceeded; review the configured source bounds.");
            }
        }

        // Finish reading and validating the entire snapshot before yielding any rows.
        foreach (var row in latestByKey.Values)
        {
            yield return row;
        }
    }

    private static bool HasContradictoryMeasurements(ProductionRowModel row, ProductionRowModel existing)
    {
        return row.ActualProduction != existing.ActualProduction
            || row.PossibleProduction != existing.PossibleProduction
            || row.LostProductionCapacitykWh != existing.LostProductionCapacitykWh
            || row.SiteAverageWindSpeedMps != existing.SiteAverageWindSpeedMps;
    }
}
