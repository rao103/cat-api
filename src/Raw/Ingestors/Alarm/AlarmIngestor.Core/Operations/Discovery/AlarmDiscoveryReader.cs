using System.Runtime.CompilerServices;
using AlarmIngestor.Configuration;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;

namespace AlarmIngestor.Operations.Discovery;

/// <summary>
/// Per-turbine ascending AlarmId discovery against ONE global cutoff. The caller (<see
/// cref="AlarmIngestor.Operations.Execution.AlarmIngestionRunner"/>) freezes a single upper bound
/// — the MAX of every configured turbine's own max AlarmId — before any turbine starts paging,
/// and passes that SAME bound to every turbine here. Because the bound may come from a different
/// turbine's data, a turbine legitimately running out of rows before reaching it is normal and
/// simply ends discovery for that turbine (an empty or short page is not a failure). Still fails
/// closed (throws) on a page exceeding the requested size, a non-advancing/repeated page, or an
/// AlarmId above the frozen bound. Yields one bounded page at a time rather than accumulating a
/// turbine's full discovery result in memory, so callers can publish and enroll each page before
/// fetching the next.
/// </summary>
public sealed class AlarmDiscoveryReader
{
    private readonly IAlarmSourceAdapter _adapter;

    public AlarmDiscoveryReader(IAlarmSourceAdapter adapter)
    {
        _adapter = adapter;
    }

    public async IAsyncEnumerable<IReadOnlyList<AlarmEventModel>> DiscoverTurbinePagesAsync(
        string windfarmId, TurbineIdentity turbine, long afterAlarmId, long? upperBound, DateTime minRetrievalDateUtc,
        IReadOnlyList<string> alarmCodes, int pageSize, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (upperBound is null || upperBound <= afterAlarmId)
        {
            yield break;
        }

        var cursor = afterAlarmId;

        while (cursor < upperBound)
        {
            var page = await _adapter.DiscoverPageAsync(
                windfarmId, turbine.TurbineRdspp, turbine.ItemId, cursor, upperBound.Value,
                minRetrievalDateUtc, alarmCodes, pageSize, cancellationToken);

            if (page.Count > pageSize)
            {
                throw new InvalidOperationException(
                    $"Alarm discovery page for turbine {turbine.TurbineRdspp} returned {page.Count} rows exceeding requested page size {pageSize}.");
            }

            if (page.Count == 0)
            {
                // The frozen bound is a GLOBAL cutoff (the max across all configured turbines),
                // not this turbine's own max, so this turbine having no more rows below it is
                // expected rather than a gap — stop cleanly instead of failing closed.
                break;
            }

            var previous = cursor;
            foreach (var alarm in page)
            {
                if (alarm.AlarmId <= previous)
                {
                    throw new InvalidOperationException(
                        $"Alarm discovery page for turbine {turbine.TurbineRdspp} returned AlarmId {alarm.AlarmId} not strictly greater than previous {previous}.");
                }

                if (alarm.AlarmId > upperBound)
                {
                    throw new InvalidOperationException(
                        $"Alarm discovery page for turbine {turbine.TurbineRdspp} returned AlarmId {alarm.AlarmId} above the frozen upper bound {upperBound}.");
                }

                previous = alarm.AlarmId;
            }

            cursor = previous;
            yield return page;

            if (page.Count < pageSize)
            {
                break;
            }
        }
    }
}
