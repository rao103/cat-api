using OutageIngestor.Models;

namespace OutageIngestor.Interfaces;

/// <summary>
/// Abstraction over the outage source system (a DB table pair). A fake/in-memory implementation
/// backs unit tests; the real adapter is <c>SqlOutageSourceAdapter</c> against DART.
/// <para>
/// The live/changed table and the deletion table are two independent incremental feeds whose
/// <c>LastUpdatedUtc</c> progress is not synchronized (Incremental Refresh Spec, Section 2.2), so
/// each is discovered, checkpointed and published separately rather than merged into one stream.
/// </para>
/// </summary>
public interface IOutageSourceAdapter
{
    /// <summary>
    /// Queries and freezes a finite latest-updated-date bound for the windfarm, below any
    /// unsafe in-flight transaction horizon. Shared by both feeds' catch-up window planning.
    /// </summary>
    Task<DateTime> GetLatestSourceUpdatedDateByWindfarmIdAsync(string windfarmName, CancellationToken cancellationToken);

    /// <summary>
    /// Keyset-paginated change discovery of the live/changed-outage table within a half-open
    /// [low, end) window, ordered by (LastUpdatedDate, EventId) — the source's real stable
    /// identity (OutageClassificationID), never a fabricated numeric surrogate and never
    /// tie-broken by turbine/item id. Each returned row already carries the full current state of
    /// the changed event (Section 4.3.1) — there is no separate read-back pass for events.
    /// <paramref name="minRetrieveDateUtc"/> mirrors the legacy `OutageRepository` minRetriveDate
    /// predicate: Start > floor OR (Start < floor AND (End > floor OR End IS NULL)).
    /// Start == floor is excluded. Only the supplied active turbine names are included;
    /// an empty list returns no rows, never the entire site.
    /// </summary>
    IAsyncEnumerable<OutageEventModel> GetChangedOutagesAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime low,
        DateTime end,
        DateTime minRetrieveDateUtc,
        (DateTime LastUpdatedDate, string EventId)? continuationKey,
        CancellationToken cancellationToken);

    /// <summary>
    /// Keyset-paginated change discovery of the separate deletion-feed table within a half-open
    /// [low, end) window, ordered by (LastUpdatedDate, EventId). This feed's window and
    /// continuation key are independent of <see cref="GetChangedOutagesAsync"/>'s (Section 2.2,
    /// 5.1) — it is never bounded by a minimum-retrieve-date floor, matching the legacy
    /// deletion query, which carries no start/end columns to apply that floor against.
    /// </summary>
    IAsyncEnumerable<OutageEventModel> GetDeletedOutagesAsync(
        string windfarmName,
        IReadOnlyList<string> activeTurbineNames,
        DateTime low,
        DateTime end,
        (DateTime LastUpdatedDate, string EventId)? continuationKey,
        CancellationToken cancellationToken);

    /// <summary>Reads the authoritative current event set overlapping the half-open event-time
    /// range [<paramref name="scopeStartUtc"/>, <paramref name="scopeEndUtc"/>) for a windfarm
    /// (used by Refresh mode), restricted to the supplied active turbine names and the exact
    /// legacy retention predicate at <paramref name="minRetrieveDateUtc"/>. Empty lists return no rows.</summary>
    IAsyncEnumerable<OutageEventModel> GetAllCurrentEventsAsync(
        string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, DateTime scopeStartUtc, DateTime scopeEndUtc, CancellationToken cancellationToken);
}
