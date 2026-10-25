namespace CurtailmentIngestor.Operations.Discovery;

/// <summary>A half-open [Start, End) LastUpdatedDate catch-up window.</summary>
public sealed record CatchUpWindow(DateTime Start, DateTime End);

/// <summary>
/// Computes the effective query low bound and splits [EffectiveQueryLow, HighWatermark)
/// into bounded half-open windows.
/// </summary>
public sealed class CatchUpWindowPlanner
{
    /// <summary>
    /// Computes <c>committed.LastUpdatedDate − SafetyLagInterval</c> and splits the resulting
    /// span up to <paramref name="highWatermark"/> into windows no larger than <paramref name="maxWindowSize"/>.
    /// Returns an empty sequence if the effective low is not before the high watermark.
    /// </summary>
    public IEnumerable<CatchUpWindow> PlanWindows(
        DateTime committedLastUpdatedDate,
        TimeSpan safetyLagInterval,
        DateTime highWatermark,
        TimeSpan maxWindowSize)
    {
        if (maxWindowSize <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(maxWindowSize), "Max window size must be positive.");
        }

        if (safetyLagInterval < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(safetyLagInterval));
        if (committedLastUpdatedDate.Kind != DateTimeKind.Utc || highWatermark.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Cursor and watermark timestamps must be UTC.");
        var effectiveQueryLow = new DateTime(Math.Max(0, committedLastUpdatedDate.Ticks - safetyLagInterval.Ticks), DateTimeKind.Utc);
        var cursor = effectiveQueryLow;

        while (cursor < highWatermark)
        {
            var end = cursor.AddTicks(Math.Min(maxWindowSize.Ticks, (highWatermark - cursor).Ticks));
            yield return new CatchUpWindow(cursor, end);
            cursor = end;
        }
    }
}
