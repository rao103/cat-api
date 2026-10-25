using Harmonizer.Core.Operations;
using Xunit;
namespace Harmonizer.UnitTests;

public sealed class EventFootprintTests
{
    private readonly EventFootprintPlanner planner = new();
    [Fact]
    public void Shortening_event_keeps_obsolete_days_in_cleanup_work()
    {
        var previous = Enumerable.Range(0, 5).Select(offset => new EventDay(11, Fixtures.Day.AddDays(offset))).ToArray();
        var next = new EventFootprint([11], Fixtures.Start, Fixtures.Start.AddDays(2).AddHours(14).AddMinutes(20));
        Assert.Equal(previous, planner.Plan(previous, next, Fixtures.Day.AddDays(4)));
    }
    [Fact]
    public void Removed_turbine_membership_is_included_in_cleanup()
    {
        var previous = new[] { new EventDay(11, Fixtures.Day), new EventDay(12, Fixtures.Day) };
        Assert.Equal(previous, planner.Plan(previous, new([12], Fixtures.Start, Fixtures.Start.AddHours(1)), Fixtures.Day));
    }
    [Fact]
    public void Midnight_end_excludes_next_day() =>
        Assert.Single(planner.Plan([], new([11], Fixtures.Start, Fixtures.Start.AddDays(1)), Fixtures.Day.AddDays(2)));
    [Fact]
    public void Open_event_extends_only_through_processable_day() =>
        Assert.Equal(3, planner.Plan([], new([11], Fixtures.Start, null), Fixtures.Day.AddDays(2)).Count);
    [Fact]
    public void Tombstone_schedules_all_previously_materialized_days()
    {
        var previous = new[] { new EventDay(11, Fixtures.Day), new EventDay(11, Fixtures.Day.AddDays(1)) };
        Assert.Equal(previous, planner.Plan(previous, null, Fixtures.Day));
    }
    [Fact]
    public void Watermark_does_not_jump_over_a_gap() =>
        Assert.Equal(Fixtures.Day, planner.AdvanceContiguous(Fixtures.Day, Fixtures.Day.AddDays(2), new HashSet<DateOnly> { Fixtures.Day, Fixtures.Day.AddDays(2) }));
}
