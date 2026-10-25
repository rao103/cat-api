namespace OutageIngestor.Operations.Discovery;

/// <summary>A half-open [Start, End) LastUpdatedDate catch-up window.</summary>
public sealed record CatchUpWindow(DateTime Start, DateTime End);

/// <summary>
/// Computes the effective query low bound and splits [EffectiveQueryLow, LatestSourceUpdatedDate + 1 tick)
/// into bounded half-open windows.
/// </summary>
public sealed class CatchUpWindowPlanner
{
    /// <summary>
    /// Computes <c>committed.LastUpdatedDate − SafetyLagInterval</c> and splits the resulting
    /// span up to <paramref name="latestSourceUpdatedDate"/> into windows no larger than <paramref name="maxWindowSize"/>.
    /// Includes rows exactly at the frozen maximum, even when it equals the checkpoint.
    /// </summary>
    public IReadOnlyList<CatchUpWindow> PlanWindows(
        DateTime committedLastUpdatedDate,
        TimeSpan safetyLagInterval,
        DateTime latestSourceUpdatedDate,
        TimeSpan maxWindowSize)
    {
        if (maxWindowSize <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(maxWindowSize), "Max window size must be positive.");
        }

        if (safetyLagInterval < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(safetyLagInterval));
        if (committedLastUpdatedDate.Kind != DateTimeKind.Utc || latestSourceUpdatedDate.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Catch-up bounds must be UTC.");
        // MinValue is the adapter's no-source-data sentinel, not an observed row.
        if (latestSourceUpdatedDate == DateTime.MinValue) return Array.Empty<CatchUpWindow>();
        var exclusiveEnd = CAT.Messaging.Contracts.RawArtifactConventions.ExclusiveEnd(latestSourceUpdatedDate);
        var effectiveQueryLow = new DateTime(Math.Max(0, committedLastUpdatedDate.Ticks - safetyLagInterval.Ticks), DateTimeKind.Utc);
        var windows = new List<CatchUpWindow>();
        var cursor = effectiveQueryLow;
        while (cursor < exclusiveEnd)
        {
            var end = exclusiveEnd - cursor <= maxWindowSize ? exclusiveEnd : cursor + maxWindowSize;
            windows.Add(new CatchUpWindow(cursor, end));
            cursor = end;
        }
        return windows;
    }
}
