namespace CurtailmentIngestor.Configuration;

/// <summary>
/// Per-source connection and identity configuration, bound from configuration/config file.
/// Independent of the other ingestors' SourceOptions.
/// </summary>
public sealed class SourceOptions
{
    public const string SectionName = "Source";

    /// <summary>Connection string for the curtailment source DB table.</summary>
    public required string ConnectionString { get; set; }

    public required string SourceSystemName { get; set; }

    public required string SqsQueueUrl { get; set; }

    /// <summary>Azure Blob connection string (config-file supplied); replaced by S3 later behind IRawObjectStore.</summary>
    public required string BlobConnectionString { get; set; }

    public required string BlobContainerName { get; set; }

    /// <summary>Postgres control-plane connection string for this ingestor's own extraction cursor table.</summary>
    public required string ControlPlaneConnectionString { get; set; }
}
