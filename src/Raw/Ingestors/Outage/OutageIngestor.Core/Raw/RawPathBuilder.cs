using OutageIngestor.Enums;

namespace OutageIngestor.Raw;

/// <summary>
/// Builds deterministic Raw artifact paths per the `/raw/outage/...` convention. Unlike
/// Production, there is no `date=` segment — Raw event batches are not Windfarm-Day shaped
/// (Section 4.3.1/4.9); identity is Windfarm + LeaseToken + window scope. Every incremental
/// artifact file name carries its own catch-up window's start/end timestamps, so a later
/// window in the same run never overwrites an earlier window's already-published batch (they
/// are different logical extractions, not retries of one) without needing a separate
/// `window={n}` path segment.
/// The live-event table and the deleted-event table are two physically separate source feeds
/// (Outage source design notes) and are kept as separate Raw artifacts under `events/` and
/// `deleted-events/` rather than merged into one file, so Raw stays close to what each source
/// table actually supplied instead of pre-transforming them into a common logical contract.
/// </summary>
public static class RawPathBuilder
{
    private const string ScopeTimestampFormat = "yyyyMMddTHHmmssZ";

    public static string BuildBasePath(string windfarmName, long extractionToken, IngestionMode mode = IngestionMode.Incremental)
    {
        if (!Enum.IsDefined(mode)) throw new ArgumentOutOfRangeException(nameof(mode));
        var modeName = mode == IngestionMode.Refresh ? "refresh" : "incremental";
        ArgumentException.ThrowIfNullOrWhiteSpace(windfarmName);
        ArgumentOutOfRangeException.ThrowIfNegative(extractionToken);
        return $"raw/outage/windfarm={windfarmName}/mode={modeName}/extraction={extractionToken}/";
    }

    private static string FormatScopeBound(DateTime utc)
        => utc.Ticks % TimeSpan.TicksPerSecond == 0
            ? utc.ToString(ScopeTimestampFormat, System.Globalization.CultureInfo.InvariantCulture)
            : utc.ToString("yyyyMMddTHHmmss.FFFFFFFZ", System.Globalization.CultureInfo.InvariantCulture);

    private static string FormatWindowScope(DateTime windowStartUtc, DateTime windowEndUtc)
        => $"window-{FormatScopeBound(windowStartUtc)}-to-{FormatScopeBound(windowEndUtc)}";

    public static string BuildEventsSingleArtifactPath(string windfarmName, long extractionToken, DateTime windowStartUtc, DateTime windowEndUtc)
        => BuildBasePath(windfarmName, extractionToken) + $"events/{FormatWindowScope(windowStartUtc, windowEndUtc)}-part-0001.parquet";

    public static string BuildEventsShardPath(string windfarmName, long extractionToken, DateTime windowStartUtc, DateTime windowEndUtc, int shardIndex)
        => BuildBasePath(windfarmName, extractionToken) + $"events/{FormatWindowScope(windowStartUtc, windowEndUtc)}-part-{shardIndex:0000}.parquet";

    public static string BuildDeletedEventsSingleArtifactPath(string windfarmName, long extractionToken, DateTime windowStartUtc, DateTime windowEndUtc)
        => BuildBasePath(windfarmName, extractionToken) + $"deleted-events/{FormatWindowScope(windowStartUtc, windowEndUtc)}-part-0001.parquet";

    public static string BuildDeletedEventsShardPath(string windfarmName, long extractionToken, DateTime windowStartUtc, DateTime windowEndUtc, int shardIndex)
        => BuildBasePath(windfarmName, extractionToken) + $"deleted-events/{FormatWindowScope(windowStartUtc, windowEndUtc)}-part-{shardIndex:0000}.parquet";

    public static string BuildQuarantinePath(string windfarmName, long extractionToken, DateTime windowStartUtc, DateTime windowEndUtc,
        IngestionMode mode = IngestionMode.Incremental)
        => BuildBasePath(windfarmName, extractionToken, mode) + $"{FormatWindowScope(windowStartUtc, windowEndUtc)}-outage-quarantine.parquet";

    /// <summary>Refresh snapshot Parquet part path, scoped by requested event-time range rather
    /// than a catch-up window (there is no `window={n}` segment for Refresh — see
    /// <see cref="BuildSnapshotManifestPath"/> for the sibling manifest under the same prefix).</summary>
    public static string BuildSnapshotPartPath(string windfarmName, long extractionToken, DateTime scopeStartUtc, DateTime scopeEndUtc, int partNumber)
        => BuildBasePath(windfarmName, extractionToken, IngestionMode.Refresh) + "snapshot/"
           + $"{FormatWindowScope(scopeStartUtc, scopeEndUtc)}-part-{partNumber:0000}.parquet";

    /// <summary>Durable manifest listing every snapshot part path once the read is complete
    /// (Incremental Refresh Spec Section 9-10) — written only after all parts are durable.</summary>
    public static string BuildSnapshotManifestPath(string windfarmName, long extractionToken)
        => BuildBasePath(windfarmName, extractionToken, IngestionMode.Refresh) + "snapshot/manifest.json";
}

