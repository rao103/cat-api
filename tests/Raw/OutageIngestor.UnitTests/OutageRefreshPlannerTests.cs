using OutageIngestor.Enums;
using OutageIngestor.Models;
using OutageIngestor.Operations.Refresh;

namespace OutageIngestor.UnitTests;

public sealed class OutageRefreshPlannerTests
{
    [Fact]
    public void FixedRangePlanPreservesIntegerIdNameRunIdAndExactScope()
    {
        var start = new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Utc).AddTicks(1234567);
        var end = new DateTime(2026, 9, 2, 6, 7, 8, DateTimeKind.Utc).AddTicks(7654321);
        var plan = new OutageRefreshPlanner().Plan(new(42, "CHW01", "caller", "reason", "run-42",
            start, end));
        Assert.Equal(42, plan.WindfarmId);
        Assert.Equal("CHW01", plan.WindfarmName);
        Assert.Equal("run-42", plan.RunId);
        Assert.Equal(IngestionMode.Refresh, plan.Mode);
        Assert.Equal("Outage", plan.SourceType);
        Assert.Equal(start, plan.RefreshScopeStartUtc);
        Assert.Equal(end, plan.RefreshScopeEndUtc);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void EmptyOrReversedScopeIsRejected(int endOffsetTicks)
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var request = new RefreshRequestModel(42, "CHW01", "caller", "reason", "run-42", start, start.AddTicks(endOffsetTicks));

        Assert.Throws<ArgumentOutOfRangeException>(() => new OutageRefreshPlanner().Plan(request));
    }

    [Fact]
    public void SubDayScopeIsNotExpandedToCalendarDays()
    {
        var start = new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Utc).AddTicks(1234567);
        var request = new RefreshRequestModel(42, "CHW01", "caller", "reason", "run-42", start, start.AddTicks(1));

        var plan = new OutageRefreshPlanner().Plan(request);

        Assert.Equal(start, plan.RefreshScopeStartUtc);
        Assert.Equal(start.AddTicks(1), plan.RefreshScopeEndUtc);
    }

    [Theory]
    [InlineData(0, "CHW01")]
    [InlineData(-42, "CHW01")]
    [InlineData(42, "")]
    [InlineData(42, " ")]
    public void InvalidIdentityOrNameIsRejected(int id, string name)
    {
        var request = new RefreshRequestModel(id, name, "caller", "reason", "run-42",
            new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc));
        Assert.ThrowsAny<ArgumentException>(() => new OutageRefreshPlanner().Plan(request));
    }
}