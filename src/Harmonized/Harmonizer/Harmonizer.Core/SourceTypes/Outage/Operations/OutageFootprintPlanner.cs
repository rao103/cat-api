using Harmonizer.Core.Operations;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

/// <summary>Includes old materialized days even after shortening, turbine changes or deletion.</summary>
public sealed class OutageFootprintPlanner(EventFootprintPlanner planner)
{
    public IReadOnlyList<EventDay> Plan(OutageState state, IReadOnlyCollection<EventDay> previouslyMaterialized,
        DateOnly calculationProcessableDate, int maximumDays = 3660)
    {
        OutageRecordValidator.Validate(state.LatestChange);
        var current = state.CurrentEvent;
        var footprint = current is null ? null : new EventFootprint([current.Turbine.WtgId], current.StartUtc, current.EndUtc);
        return planner.Plan(previouslyMaterialized, footprint, calculationProcessableDate, maximumDays);
    }
}
