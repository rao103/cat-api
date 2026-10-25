using ProductionIngestor.Operations.Refresh;

namespace ProductionIngestor.UnitTests;

public sealed class ProductionRefreshWindowPlannerTests
{
    [Fact]
    public void GroupsContiguousDatesAndClipsAtMonthBoundaries()
    {
        var windows = ProductionRefreshWindowPlanner.Plan(
        [
            new DateOnly(2026, 7, 15), new DateOnly(2026, 7, 16), new DateOnly(2026, 7, 31),
            new DateOnly(2026, 8, 1), new DateOnly(2026, 8, 2), new DateOnly(2026, 9, 10)
        ]);

        Assert.Equal(4, windows.Count);
        Assert.Equal(new DateTime(2026, 7, 15, 0, 0, 0, DateTimeKind.Utc), windows[0].QueryStartUtc);
        Assert.Equal(new DateTime(2026, 7, 17, 0, 0, 0, DateTimeKind.Utc), windows[0].QueryEndExclusiveUtc);
        Assert.Equal(new DateTime(2026, 7, 31, 0, 0, 0, DateTimeKind.Utc), windows[1].QueryStartUtc);
        Assert.Equal(new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc), windows[1].QueryEndExclusiveUtc);
        Assert.Equal(new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc), windows[2].QueryStartUtc);
        Assert.Equal(new DateTime(2026, 8, 3, 0, 0, 0, DateTimeKind.Utc), windows[2].QueryEndExclusiveUtc);
        Assert.Equal(new DateTime(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc), windows[3].QueryStartUtc);
        Assert.Equal(new DateTime(2026, 9, 11, 0, 0, 0, DateTimeKind.Utc), windows[3].QueryEndExclusiveUtc);
        Assert.Equal(new[] { new DateOnly(2026, 9, 10) }, windows[3].SelectedDates);
    }

    [Fact]
    public void PreservesGapsInsteadOfExpandingToDateExtent()
    {
        var windows = ProductionRefreshWindowPlanner.Plan(
            [new DateOnly(2026, 8, 1), new DateOnly(2026, 8, 3)]);

        Assert.Equal(2, windows.Count);
        Assert.All(windows, window => Assert.Single(window.SelectedDates));
    }

    [Fact]
    public void RejectsAnEmptyScope()
    {
        Assert.Throws<ArgumentException>(() => ProductionRefreshWindowPlanner.Plan([]));
    }
}