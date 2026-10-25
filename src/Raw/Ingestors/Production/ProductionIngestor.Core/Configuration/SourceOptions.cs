namespace ProductionIngestor.Configuration;

/// <summary>Per-source connection and identity configuration, bound from configuration/config file.</summary>
public sealed class SourceOptions
{
    public const string SectionName = "Source";

    public string ConnectionString { get; set; } = string.Empty;

    public required string SourceSystemName { get; set; }

    public string SqsQueueUrl { get; set; } = string.Empty;

    /// <summary>Azure Blob connection string (config-file supplied); replaced by S3 later behind IRawObjectStore.
    /// Takes precedence over <see cref="BlobServiceUri"/> when nonempty.</summary>
    public string BlobConnectionString { get; set; } = string.Empty;

    /// <summary>Absolute HTTPS Blob service endpoint used with DefaultAzureCredential when no connection string is supplied.</summary>
    public string? BlobServiceUri { get; set; }

    public required string BlobContainerName { get; set; }

    /// <summary>Postgres control-plane connection string (ExtractionCursor, availability tables).</summary>
    public required string ControlPlaneConnectionString { get; set; }
}
