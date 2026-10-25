using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Operations.Discovery;

namespace OutageIngestor.UnitTests;

/// <summary>
/// <see cref="ChangeDiscoveryReader"/> trusts the adapter's own emission order verbatim; these
/// tests exercise the case where the source reports two distinct events (e.g. different
/// turbines) at the exact same LastUpdatedUTC, which is possible because
/// OutageClassificationID alone (not paired with WtgRDSPPID, per README "Outage Source
/// Contract") is the only identity the shared cursor tracks. Neither event may be silently
/// dropped, and the reported LastObservedKey must reflect the true last-emitted row, not an
/// arbitrary/deduplicated one. The live/changed and deletion feeds are discovered independently
/// (Section 2.2), so each has its own coverage below.
/// </summary>
public sealed class ChangeDiscoveryReaderTests
{
    private static readonly DateTime SameInstant = new(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task TwoChangedEventsAtTheExactSameTimestampAreBothReturnedNotSkipped()
    {
        var first = MakeEvent("OC-1", "wtg-1");
        var second = MakeEvent("OC-2", "wtg-2");
        var adapter = new FakeAdapter(changed: [first, second]);
        var reader = new ChangeDiscoveryReader(adapter);

        var result = await reader.DiscoverChangedAsync("1", ["wtg-1", "wtg-2"], SameInstant.AddMinutes(-1), SameInstant.AddMinutes(1), DateTime.MinValue, CancellationToken.None);

        Assert.Equal(2, result.ChangedEvents.Count);
        Assert.Contains(result.ChangedEvents, e => e.EventId == "OC-1");
        Assert.Contains(result.ChangedEvents, e => e.EventId == "OC-2");
        Assert.Equal((SameInstant, "OC-2"), result.LastObservedKey);
    }

    [Fact]
    public async Task DuplicateClassificationIdAcrossDifferentTurbinesAtTheSameInstantIsNotCollapsed()
    {
        var first = MakeEvent("OC-1", "wtg-1");
        var duplicateIdDifferentTurbine = MakeEvent("OC-1", "wtg-2");
        var adapter = new FakeAdapter(changed: [first, duplicateIdDifferentTurbine]);
        var reader = new ChangeDiscoveryReader(adapter);

        var result = await reader.DiscoverChangedAsync("1", ["wtg-1", "wtg-2"], SameInstant.AddMinutes(-1), SameInstant.AddMinutes(1), DateTime.MinValue, CancellationToken.None);

        Assert.Equal(2, result.ChangedEvents.Count);
        Assert.Equal("wtg-1", result.ChangedEvents[0].ItemId);
        Assert.Equal("wtg-2", result.ChangedEvents[1].ItemId);
    }

    [Fact]
    public async Task DeletedFeedIsDiscoveredIndependentlyOfTheChangedFeed()
    {
        var tombstone = MakeEvent("OC-9", "wtg-3") with { IsTombstone = true };
        var adapter = new FakeAdapter(changed: [MakeEvent("OC-1", "wtg-1")], deleted: [tombstone]);
        var reader = new ChangeDiscoveryReader(adapter);

        var result = await reader.DiscoverDeletedAsync("1", ["wtg-3"], SameInstant.AddMinutes(-1), SameInstant.AddMinutes(1), CancellationToken.None);

        Assert.Single(result.ChangedEvents);
        Assert.Equal("OC-9", result.ChangedEvents[0].EventId);
        Assert.Equal((SameInstant, "OC-9"), result.LastObservedKey);
    }

    private static OutageEventModel MakeEvent(string eventId, string itemId) => new(
        EventId: eventId, WindfarmId: "1", ItemId: itemId, EventType: "Outage",
        OriginalStartUtc: SameInstant.AddHours(-1), OriginalEndUtc: null, Status: "Open",
        SourceRevision: null, SourceWatermarkUtc: SameInstant, IsTombstone: false,
        LastUpdatedDate: SameInstant, OutageId: $"OUT-{eventId}");

    private sealed class FakeAdapter : IOutageSourceAdapter
    {
        private readonly IReadOnlyList<OutageEventModel> _changed;
        private readonly IReadOnlyList<OutageEventModel> _deleted;

        public FakeAdapter(IReadOnlyList<OutageEventModel> changed, IReadOnlyList<OutageEventModel>? deleted = null)
        {
            _changed = changed;
            _deleted = deleted ?? [];
        }

        public async IAsyncEnumerable<OutageEventModel> GetChangedOutagesAsync(
            string windfarmId, IReadOnlyList<string> activeTurbineNames, DateTime low, DateTime end, DateTime minRetrieveDateUtc,
            (DateTime LastUpdatedDate, string EventId)? continuationKey,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Yield();
            foreach (var evt in _changed)
            {
                yield return evt;
            }
        }

        public async IAsyncEnumerable<OutageEventModel> GetDeletedOutagesAsync(
            string windfarmId, IReadOnlyList<string> activeTurbineNames, DateTime low, DateTime end,
            (DateTime LastUpdatedDate, string EventId)? continuationKey,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Yield();
            foreach (var evt in _deleted)
            {
                yield return evt;
            }
        }

        public Task<DateTime> GetLatestSourceUpdatedDateByWindfarmIdAsync(string windfarmId, CancellationToken cancellationToken)
            => Task.FromResult(SameInstant);

        public async IAsyncEnumerable<OutageEventModel> GetAllCurrentEventsAsync(
            string windfarmId, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, DateTime scopeStartUtc, DateTime scopeEndUtc,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.Yield();
            yield break;
        }
    }
}
