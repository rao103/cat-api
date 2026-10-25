namespace CurtailmentIngestor.Raw;

/// <summary>
/// Builds deterministic Raw artifact paths per the `/raw/curtailment/...` convention. As with
/// Outage, there is no `date=` segment — Raw event batches are not Windfarm-Day shaped
/// (Section 4.3.1/4.9); identity is Windfarm + LeaseToken.
/// </summary>
public static class RawPathBuilder
{
    public static string BuildBasePath(string windfarmId, long extractionToken)
        => $"/raw/curtailment/windfarm={windfarmId}/extraction={extractionToken}/";

    public static string BuildSingleArtifactPath(string windfarmId, long extractionToken)
        => BuildBasePath(windfarmId, extractionToken) + "curtailment.parquet";

    public static string BuildShardPath(string windfarmId, long extractionToken, int shardIndex)
        => BuildBasePath(windfarmId, extractionToken) + $"curtailment-shard-{shardIndex:0000}.parquet";

    public static string BuildQuarantinePath(string windfarmId, long extractionToken)
        => BuildBasePath(windfarmId, extractionToken) + "curtailment-quarantine.parquet";
}
