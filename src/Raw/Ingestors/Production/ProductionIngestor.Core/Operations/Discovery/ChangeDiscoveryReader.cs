using ProductionIngestor.Interfaces;

namespace ProductionIngestor.Operations.Discovery;

/// <summary>
/// Affected UTC production dates, observed change count, and the maximum observed
/// <c>LastUpdatedUtc</c> (with its row identity as a tie-break) for a source window.
/// <see cref="MaxObservedLastUpdatedUtc"/> is null only when no rows were observed.
/// </summary>
public sealed record DiscoveryResult(
    IReadOnlyCollection<DateOnly> AffectedDates,
    long ChangedRowCount,
    DateTime? MaxObservedLastUpdatedUtc,
    string? MaxObservedItemId,
    DateTime? MaxObservedProductionTimestampUtc);

/// <summary>
/// Collects a bounded set of affected dates. The source adapter owns page iteration.
/// </summary>
public sealed class ChangeDiscoveryReader
{
    private readonly IProductionSourceAdapter _adapter;
    private readonly int _maxAffectedDates;

    public ChangeDiscoveryReader(IProductionSourceAdapter adapter, int maxAffectedDates = 5000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAffectedDates);
        _adapter = adapter;
        _maxAffectedDates = maxAffectedDates;
    }

    public async Task<DiscoveryResult> DiscoverAsync(
        string windfarmName, DateTime low, DateTime end, CancellationToken cancellationToken)
    {
        var affectedDates = new HashSet<DateOnly>();
        long changedRowCount = 0;
        DateTime? maxLastUpdatedUtc = null;
        string? maxItemId = null;
        DateTime? maxProductionTimestampUtc = null;

        await foreach (var row in _adapter.GetChangedRowsAsync(windfarmName, low, end, cancellationToken))
        {
            affectedDates.Add(row.ProductionDate);
            changedRowCount++;

            if (maxLastUpdatedUtc is null || row.LastUpdatedUtc > maxLastUpdatedUtc)
            {
                maxLastUpdatedUtc = row.LastUpdatedUtc;
                maxItemId = row.ItemId;
                maxProductionTimestampUtc = row.ProductionTimestampUtc;
            }

            if (affectedDates.Count > _maxAffectedDates)
            {
                throw new InvalidDataException("Affected-date limit exceeded; reduce the catch-up window size.");
            }
        }

        return new DiscoveryResult(affectedDates, changedRowCount, maxLastUpdatedUtc, maxItemId, maxProductionTimestampUtc);
    }
}
