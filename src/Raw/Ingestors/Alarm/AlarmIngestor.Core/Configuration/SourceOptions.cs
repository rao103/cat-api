namespace AlarmIngestor.Configuration;

/// <summary>
/// Per-source connection and identity configuration, bound from configuration/config file.
/// Independent of ProductionIngestor's and OutageIngestor's SourceOptions.
/// </summary>
public sealed class SourceOptions
{
    public const string SectionName = "Source";

    /// <summary>Base URI of the deployed Alarm API (e.g. https://alarm-api.../sdh/alarm/api).</summary>
    public required string ApiBaseUri { get; set; }

    public required string SourceSystemName { get; set; }

    public required string SqsQueueUrl { get; set; }

    /// <summary>Azure Blob connection string (config-file supplied); replaced by S3 later behind IRawObjectStore.</summary>
    public required string BlobConnectionString { get; set; }

    public required string BlobContainerName { get; set; }

    /// <summary>Postgres control-plane connection string for this ingestor's own extraction state.</summary>
    public required string ControlPlaneConnectionString { get; set; }
}
