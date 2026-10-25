using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

/// <summary>Uses compact date ranges so a long-running event does not allocate a day per calendar date.</summary>
public sealed class OutageRangePlanner
{
    public IReadOnlyList<OutageRange> Current(OutageEvent? outage, DateOnly through)
    {
        if (outage is null) return [];
        var start = DateOnly.FromDateTime(outage.StartUtc);
        var end = outage.EndUtc is { } closed ? DateOnly.FromDateTime(closed.AddTicks(-1)) : through;
        if (end > through) end = through;
        return start > end ? [] : [new(outage.Turbine.WtgId, outage.Turbine.WtgName, start, end)];
    }

    public IReadOnlyList<OutageRange> Union(params IEnumerable<OutageRange>[] sets)
    {
        var result = new List<OutageRange>();
        foreach (var group in sets.SelectMany(set => set).GroupBy(range => range.WtgId).OrderBy(group => group.Key))
        {
            if (group.Select(range => range.WtgName).Distinct(StringComparer.Ordinal).Count() != 1)
                throw new WorkValidationException("A persisted WTG ID has conflicting names in lifecycle ranges.");
            foreach (var range in group.OrderBy(range => range.StartInclusive))
            {
                if (range.WtgId <= 0 || range.EndInclusive < range.StartInclusive || range.EndInclusive == DateOnly.MaxValue)
                    throw new WorkValidationException("Invalid bounded lifecycle date range.");
                var previous = result.LastOrDefault();
                if (previous is not null && previous.WtgId == range.WtgId && range.StartInclusive.DayNumber <= previous.EndInclusive.DayNumber + 1)
                    result[^1] = previous with { EndInclusive = range.EndInclusive > previous.EndInclusive ? range.EndInclusive : previous.EndInclusive };
                else result.Add(range);
            }
        }
        return result;
    }

    public OutageLifecyclePlan ConsumeFirst(OutageLifecyclePlan plan)
    {
        if (plan.Pending.Count == 0) throw new InvalidOperationException("No pending lifecycle date.");
        var first = plan.Pending[0];
        var remaining = plan.Pending.Skip(1).ToList();
        if (first.StartInclusive < first.EndInclusive)
            remaining.Insert(0, first with { StartInclusive = first.StartInclusive.AddDays(1) });
        return plan with { Pending = remaining };
    }
}
