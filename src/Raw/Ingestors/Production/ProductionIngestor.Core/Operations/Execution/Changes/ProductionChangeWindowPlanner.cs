namespace ProductionIngestor.Operations.Execution.Changes;

/// <summary>
/// Pure, DB-free planning of half-open-turned-inclusive UTC calendar-month <c>LastUpdatedUtc</c>
/// windows between a committed checkpoint and a frozen upper boundary. Windows are contiguous and
/// non-overlapping: each window's <c>MaxInclusiveUtc</c> is exactly one tick before the next
/// window's <c>MinInclusiveUtc</c>, and the very first window starts one tick past the checkpoint
/// (already-processed instant). The final window's <c>MaxInclusiveUtc</c> always equals
/// <paramref name="frozenMaxInclusiveUtc"/> exactly, so that exact instant is never skipped.
/// </summary>
public static class ProductionChangeWindowPlanner
{
    public static IReadOnlyList<(DateTime MinInclusiveUtc, DateTime MaxInclusiveUtc)> PlanCalendarMonthWindows(
        DateTime checkpointExclusiveUtc, DateTime frozenMaxInclusiveUtc)
    {
        var windows = new List<(DateTime, DateTime)>();
        if (checkpointExclusiveUtc.Kind != DateTimeKind.Utc || frozenMaxInclusiveUtc.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Change window bounds must be UTC.");
        if (frozenMaxInclusiveUtc <= checkpointExclusiveUtc) return windows;
        _ = CAT.Messaging.Contracts.RawArtifactConventions.ExclusiveEnd(frozenMaxInclusiveUtc);
        var start = checkpointExclusiveUtc.AddTicks(1);

        while (start <= frozenMaxInclusiveUtc)
        {
            var monthEndInclusive = start.Year == 9999 && start.Month == 12
                ? frozenMaxInclusiveUtc
                : new DateTime(start.Year, start.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(1).AddTicks(-1);
            var end = monthEndInclusive < frozenMaxInclusiveUtc ? monthEndInclusive : frozenMaxInclusiveUtc;
            windows.Add((start, end));
            start = end.AddTicks(1);
        }
        return windows;
    }
}
