namespace AlarmIngestor.Raw;

/// <summary>
/// Deterministic Raw artifact paths. WindowSequence distinguishes the Discovery, Polling and
/// Historical batches written under one lease token within a single run so they never
/// overwrite each other; a retry of the same logical batch reuses the same path.
/// </summary>
public static class RawPathBuilder
{
    /// <summary>
    /// The exact common prefix of every path this run (one lease-token acquisition) can write —
    /// used as `extraction_run.output_path`. Must stay a true prefix of <see
    /// cref="BuildBasePath"/>; never widen this to a farm-wide (no-token) prefix and call it
    /// run-owned.
    /// </summary>
    public static string BuildRunPrefix(string windfarmId, long extractionToken)
        => $"/raw/alarm/windfarm={windfarmId}/extraction={extractionToken}/";

    public static string BuildBasePath(string windfarmId, long extractionToken, int windowSequence)
        => BuildRunPrefix(windfarmId, extractionToken) + $"window={windowSequence}/";

    public static string BuildSingleArtifactPath(string windfarmId, long extractionToken, int windowSequence)
        => BuildBasePath(windfarmId, extractionToken, windowSequence) + "alarm.parquet";

    public static string BuildShardPath(string windfarmId, long extractionToken, int windowSequence, int shardIndex)
        => BuildBasePath(windfarmId, extractionToken, windowSequence) + $"alarm-shard-{shardIndex:0000}.parquet";

    public static string BuildQuarantinePath(string windfarmId, long extractionToken, int windowSequence)
        => BuildBasePath(windfarmId, extractionToken, windowSequence) + "alarm-quarantine.parquet";
}
