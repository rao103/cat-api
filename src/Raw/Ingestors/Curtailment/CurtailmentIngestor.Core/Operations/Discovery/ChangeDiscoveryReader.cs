using CurtailmentIngestor.Interfaces;
using CurtailmentIngestor.Models;

namespace CurtailmentIngestor.Operations.Discovery;

/// <summary>
/// Result of discovering changed events within one catch-up window. The changed rows
/// themselves are already the full-current-state Raw payload (Section 4.3.1) — there is no
/// separate "affected dates" set to re-read.
/// </summary>
public sealed record DiscoveryResult(
    IReadOnlyList<CurtailmentEventModel> ChangedEvents,
    (DateTime LastUpdatedDate, long Id)? LastObservedKey);

/// <summary>
/// Per-window change discovery reader. Keyset-paginates using (LastUpdatedDate, Id) as the
/// continuation key (never OFFSET).
/// </summary>
public sealed class ChangeDiscoveryReader
{
    private readonly ICurtailmentSourceAdapter _adapter;
    private readonly int _maxEventsPerWindow;

    public ChangeDiscoveryReader(ICurtailmentSourceAdapter adapter, int maxEventsPerWindow = 200_000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEventsPerWindow);
        _adapter = adapter;
        _maxEventsPerWindow = maxEventsPerWindow;
    }

    public async Task<DiscoveryResult> DiscoverAsync(
        string windfarmId, DateTime low, DateTime end, CancellationToken cancellationToken)
    {
        var events = new List<CurtailmentEventModel>();
        (DateTime LastUpdatedDate, long Id)? lastObserved = null;

        await foreach (var evt in _adapter.GetChangedEventsAsync(windfarmId, low, end, lastObserved, cancellationToken))
        {
            events.Add(evt);
            lastObserved = (evt.LastUpdatedDate, evt.Id);

            if (events.Count > _maxEventsPerWindow)
            {
                throw new InvalidDataException("Changed-event limit exceeded; reduce the catch-up window size.");
            }
        }

        return new DiscoveryResult(events, lastObserved);
    }
}
