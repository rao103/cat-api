namespace ProductionIngestor.Operations.Refresh;

public sealed record ProductionRefreshWindow(
    DateTime QueryStartUtc,
    DateTime QueryEndExclusiveUtc,
    IReadOnlyList<DateOnly> SelectedDates);

/// <summary>Groups only adjacent requested dates and never crosses a calendar-month boundary.</summary>
public static class ProductionRefreshWindowPlanner
{
    public static IReadOnlyList<ProductionRefreshWindow> Plan(IEnumerable<DateOnly> requestedDates)
    {
        ArgumentNullException.ThrowIfNull(requestedDates);
        var dates = requestedDates.Distinct().Order().ToArray();
        if (dates.Length == 0) throw new ArgumentException("At least one refresh date is required.", nameof(requestedDates));

        var windows = new List<ProductionRefreshWindow>();
        var first = dates[0];
        var previous = first;
        var selected = new List<DateOnly> { first };

        for (var index = 1; index < dates.Length; index++)
        {
            var current = dates[index];
            var sameMonth = current.Year == previous.Year && current.Month == previous.Month;
            if (sameMonth && current == previous.AddDays(1))
            {
                selected.Add(current);
            }
            else
            {
                windows.Add(CreateWindow(first, previous, selected));
                first = current;
                selected = [current];
            }
            previous = current;
        }

        windows.Add(CreateWindow(first, previous, selected));
        return windows;
    }

    private static ProductionRefreshWindow CreateWindow(DateOnly first, DateOnly last, List<DateOnly> selected)
    {
        var start = DateTime.SpecifyKind(first.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
        var end = DateTime.SpecifyKind(last.AddDays(1).ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
        return new ProductionRefreshWindow(start, end, selected.ToArray());
    }
}