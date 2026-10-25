using CurtailmentIngestor.Models;

namespace CurtailmentIngestor.Interfaces;

/// <summary>
/// Abstraction over the curtailment source system (a DB table). A fake/in-memory implementation
/// backs unit tests; the real adapter is added once the source table schema/indexing is
/// confirmed with the source-system owner.
/// </summary>
public interface ICurtailmentSourceAdapter
{
    /// <summary>
    /// Queries and freezes a finite high watermark for the windfarm, below any
    /// unsafe in-flight transaction horizon.
    /// </summary>
    Task<DateTime> GetHighWatermarkAsync(string windfarmId, CancellationToken cancellationToken);

    /// <summary>
    /// Keyset-paginated change discovery within a half-open [low, end) window, ordered by
    /// (LastUpdatedDate, Id). Each returned row already carries the full current state of the
    /// changed event (Section 4.3.1) — there is no separate read-back pass for events.
    /// </summary>
    IAsyncEnumerable<CurtailmentEventModel> GetChangedEventsAsync(
        string windfarmId,
        DateTime low,
        DateTime end,
        (DateTime LastUpdatedDate, long Id)? continuationKey,
        CancellationToken cancellationToken);

    /// <summary>Reads the full authoritative current event set for a windfarm (used by Refresh mode).</summary>
    IAsyncEnumerable<CurtailmentEventModel> GetAllCurrentEventsAsync(string windfarmId, CancellationToken cancellationToken);
}
