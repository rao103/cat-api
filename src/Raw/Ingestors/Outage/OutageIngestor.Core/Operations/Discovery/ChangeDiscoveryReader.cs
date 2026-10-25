using OutageIngestor.Interfaces;
using OutageIngestor.Models;

namespace OutageIngestor.Operations.Discovery;

/// <summary>
/// Result of discovering changed events within one catch-up window. Unlike Production, the
/// changed rows themselves are already the full-current-state Raw payload (Section 4.3.1) —
/// there is no separate "affected dates" set to re-read.
/// </summary>
public sealed record DiscoveryResult(
    IReadOnlyList<OutageEventModel> ChangedEvents,
    (DateTime LastUpdatedDate, string EventId)? LastObservedKey);

/// <summary>
/// Per-window change discovery reader for both independent Outage feeds. Keyset-paginates using
/// (LastUpdatedDate, EventId) as the continuation key (never OFFSET, never a turbine/item
/// tie-break). The live/changed and deletion feeds are discovered separately since they carry
/// independent checkpoints and windows (Section 2.2).
/// </summary>
public sealed class ChangeDiscoveryReader
{
    private readonly IOutageSourceAdapter _adapter;

    public ChangeDiscoveryReader(IOutageSourceAdapter adapter)
    {
        _adapter = adapter;
    }

    /// <summary>Discovers one catch-up window of the live/changed-outage feed.</summary>
    public async Task<DiscoveryResult> DiscoverChangedAsync(
        string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime low, DateTime end, DateTime minRetrieveDateUtc, CancellationToken cancellationToken)
    {
        var events = new List<OutageEventModel>();
        (DateTime LastUpdatedDate, string EventId)? lastObserved = null;

        await foreach (var evt in _adapter.GetChangedOutagesAsync(windfarmName, activeTurbineNames, low, end, minRetrieveDateUtc, lastObserved, cancellationToken))
        {
            events.Add(evt);
            lastObserved = (evt.LastUpdatedDate, evt.EventId);
        }

        return new DiscoveryResult(events, lastObserved);
    }

    /// <summary>Discovers one catch-up window of the deletion feed.</summary>
    public async Task<DiscoveryResult> DiscoverDeletedAsync(
        string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime low, DateTime end, CancellationToken cancellationToken)
    {
        var events = new List<OutageEventModel>();
        (DateTime LastUpdatedDate, string EventId)? lastObserved = null;

        await foreach (var evt in _adapter.GetDeletedOutagesAsync(windfarmName, activeTurbineNames, low, end, lastObserved, cancellationToken))
        {
            events.Add(evt);
            lastObserved = (evt.LastUpdatedDate, evt.EventId);
        }

        return new DiscoveryResult(events, lastObserved);
    }
}
