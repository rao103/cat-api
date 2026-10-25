using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

/// <summary>Builds a complete day from all overlapping canonical classifications, not only changed rows.</summary>
public sealed class OutageDayMaterializer
{
    public IReadOnlyList<OutageDaySegment> Build(WtgDayKey key, IReadOnlyList<OutageState> canonicalStates)
    {
        if (key.WindfarmId <= 0 || key.WtgId <= 0 || key.InputType != "Outage" || key.DataDate == DateOnly.MaxValue)
        {
            throw new WorkValidationException("Expected an Outage WTG-Day with valid IDs and an exclusive next-day boundary.");
        }

        var keys = new HashSet<OutageKey>();
        foreach (var state in canonicalStates)
        {
            OutageRecordValidator.Validate(state.LatestChange);
            if (!keys.Add(state.Key))
            {
                throw new SourceConflictException("Canonical Outage state contains duplicate classification keys.");
            }
        }

        var start = key.DataDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
        var end = start.AddDays(1);
        return canonicalStates.Select(state => state.CurrentEvent)
            .OfType<OutageEvent>()
            .Where(outage => outage.Key.WindfarmId == key.WindfarmId && outage.Turbine.WtgId == key.WtgId && outage.Overlaps(start, end))
            .OrderBy(outage => outage.StartUtc).ThenBy(outage => outage.Key.OutageClassificationId, StringComparer.Ordinal)
            .Select(outage => new OutageDaySegment(outage.Key, outage.Turbine,
                outage.StartUtc > start ? outage.StartUtc : start,
                outage.EndUtc is { } closed && closed < end ? closed : end,
                outage.StartUtc, outage.EndUtc, outage.LastUpdatedUtc, outage.Attributes))
            .ToArray();
    }
}
