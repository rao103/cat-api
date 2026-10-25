namespace Harmonizer.Core.Operations;

public sealed record EventFootprint(IReadOnlyList<int> WtgIds, DateTime StartUtc, DateTime? EndUtc);
public sealed record EventDay(int WtgId, DateOnly DataDate);

/// <summary>Pure lifecycle policy; the verified source adapter supplies authoritative old/new revisions.</summary>
public sealed class EventFootprintPlanner
{
    public IReadOnlyList<EventDay> Plan(IReadOnlyCollection<EventDay> previouslyMaterialized,
        EventFootprint? current, DateOnly calculationProcessableDate, int maximumDays = 3660)
    {
        if (maximumDays < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumDays));
        }

        var affected = new HashSet<EventDay>(previouslyMaterialized);
        if (current is not null)
        {
            if (current.StartUtc.Kind != DateTimeKind.Utc || current.EndUtc is { Kind: not DateTimeKind.Utc } ||
                current.EndUtc <= current.StartUtc || current.WtgIds.Any(id => id <= 0))
            {
                throw new ArgumentException("Invalid event interval or turbine membership.", nameof(current));
            }

            var start = DateOnly.FromDateTime(current.StartUtc);
            var end = current.EndUtc is { } closed
                ? DateOnly.FromDateTime(closed.AddTicks(-1)) : calculationProcessableDate;
            if (end > calculationProcessableDate)
            {
                end = calculationProcessableDate;
            }

            if (end.DayNumber - start.DayNumber + 1 > maximumDays)
            {
                throw new ArgumentException("Event footprint requires bounded continuation work.", nameof(current));
            }

            foreach (var wtgId in current.WtgIds.Distinct())
            {
                for (var dayNumber = start.DayNumber; dayNumber <= end.DayNumber; dayNumber++)
                {
                    affected.Add(new(wtgId, DateOnly.FromDayNumber(dayNumber)));
                }
            }
        }
        return affected.OrderBy(day => day.WtgId).ThenBy(day => day.DataDate).ToArray();
    }

    public DateOnly? AdvanceContiguous(DateOnly firstDate, DateOnly targetDate, IReadOnlySet<DateOnly> completed)
    {
        DateOnly? through = null;
        for (var dayNumber = firstDate.DayNumber; dayNumber <= targetDate.DayNumber; dayNumber++)
        {
            var day = DateOnly.FromDayNumber(dayNumber);
            if (!completed.Contains(day))
            {
                break;
            }
            through = day;
        }

        return through;
    }
}
