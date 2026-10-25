namespace ProductionIngestor.Raw;

using ProductionIngestor.Enums;

/// <summary>Builds deterministic Raw artifact paths per the `/raw/production/...` convention.</summary>
public static class RawPathBuilder
{
    public static string BuildBasePath(string windfarmName, DateOnly productionDate, long extractionToken)
        => BuildExtractionPrefix(windfarmName, IngestionMode.Incremental, extractionToken);

    public static string BuildSingleArtifactPath(string windfarmName, DateOnly productionDate, long extractionToken)
        => BuildPartPath(windfarmName, IngestionMode.Incremental, extractionToken,
            UtcStart(productionDate), UtcStart(productionDate.AddDays(1)), 1);

    public static string BuildShardPath(string windfarmName, DateOnly productionDate, long extractionToken, int shardIndex)
        => BuildPartPath(windfarmName, IngestionMode.Incremental, extractionToken,
            UtcStart(productionDate), UtcStart(productionDate.AddDays(1)), shardIndex);

    public static string BuildQuarantinePath(string windfarmName, DateOnly productionDate, long extractionToken)
        => BuildExtractionPrefix(windfarmName, IngestionMode.Incremental, extractionToken)
            + $"window-{FormatWindowBoundary(UtcStart(productionDate))}-to-{FormatWindowBoundary(UtcStart(productionDate.AddDays(1)))}-part-0001-quarantine.parquet";

    public static string BuildExtractionPrefix(string windfarmName, IngestionMode mode, long extractionToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(windfarmName);
        ArgumentOutOfRangeException.ThrowIfNegative(extractionToken);
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        var modeName = mode == IngestionMode.Refresh ? "refresh" : "incremental";
        return $"raw/production/windfarm={windfarmName}/mode={modeName}/extraction={extractionToken}/";
    }

    public static string BuildPartPath(
        string windfarmName, IngestionMode mode, long extractionToken,
        DateTime queryStartUtc, DateTime queryEndExclusiveUtc, int partNumber)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(partNumber);
        if (queryStartUtc.Kind != DateTimeKind.Utc || queryEndExclusiveUtc.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Production Raw query bounds must have UTC DateTimeKind.");
        if (queryEndExclusiveUtc <= queryStartUtc)
            throw new ArgumentException("The exclusive query end must be later than the query start.");

        return BuildExtractionPrefix(windfarmName, mode, extractionToken)
            + $"window-{FormatWindowBoundary(queryStartUtc)}-to-{FormatWindowBoundary(queryEndExclusiveUtc)}-part-{partNumber:D4}.parquet";
    }

    /// <summary>
    /// Base directory for the windowed ProductionChanges layout: no production-date subfolder,
    /// since one extraction can span many production dates. Also used as `extraction_run.output_path`
    /// for incremental attempts under this layout.
    /// </summary>
    public static string BuildWindowExtractionPrefix(string windfarmName, long extractionToken, IngestionMode mode = IngestionMode.Incremental)
        => BuildExtractionPrefix(windfarmName, mode, extractionToken);

    /// <summary>
    /// Deterministic part path for one bounded ProductionChanges part: the window's shared observed
    /// production-timestamp min/max (identical across every part of the same window) and a
    /// `part-NNNN` counter resetting per window (four-digit minimum padding).
    /// </summary>
    public static string BuildWindowPartPath(
        string windfarmName, long extractionToken, DateTime windowMinInclusiveUtc, DateTime windowMaxInclusiveUtc, int partNumber)
        => BuildPartPath(windfarmName, IngestionMode.Incremental, extractionToken,
            windowMinInclusiveUtc, windowMaxInclusiveUtc.AddTicks(1), partNumber);

    /// <summary>
    /// Invariant UTC timestamp without colons, e.g. <c>20260924T103000Z</c> or, when the instant has
    /// sub-second precision, <c>20260924T103000.1234567Z</c>. Fractional digits are included only
    /// when nonzero and trimmed of trailing zeros; two distinct instants never format identically.
    /// </summary>
    public static string FormatWindowBoundary(DateTime instantUtc)
    {
        var whole = instantUtc.ToString("yyyyMMdd\\THHmmss", System.Globalization.CultureInfo.InvariantCulture);
        var fractionTicks = instantUtc.Ticks % TimeSpan.TicksPerSecond;
        if (fractionTicks == 0)
        {
            return whole + "Z";
        }

        var fraction = fractionTicks.ToString("D7", System.Globalization.CultureInfo.InvariantCulture).TrimEnd('0');
        return $"{whole}.{fraction}Z";
    }

    private static DateTime UtcStart(DateOnly date) =>
        DateTime.SpecifyKind(date.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
}
