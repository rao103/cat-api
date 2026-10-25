namespace OutageIngestor.Configuration;

/// <summary>
/// Per-source connection and identity configuration, bound from configuration/config file.
/// Independent of ProductionIngestor's SourceOptions.
/// </summary>
public sealed class SourceOptions
{
    public const string SectionName = "Source";

    /// <summary>Legacy fallback connection string, used only when SqlSourceOptions:ConnectionStrings
    /// is empty; the real path is the token-authenticated "OutageDatabase" entry shared with Production.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    public required string SourceSystemName { get; set; }

    public required string SqsQueueUrl { get; set; }

    /// <summary>Azure Blob connection string (config-file supplied); replaced by S3 later behind IRawObjectStore.
    /// Takes precedence over <see cref="BlobServiceUri"/> when nonempty.</summary>
    public string BlobConnectionString { get; set; } = string.Empty;

    /// <summary>Absolute HTTPS Blob service endpoint used with DefaultAzureCredential when no connection string is supplied.</summary>
    public string? BlobServiceUri { get; set; }

    public required string BlobContainerName { get; set; }

    /// <summary>Postgres control-plane connection string; used by OutageIngestor.Api's read-only
    /// status lookups. The Worker instead uses the IAM-authenticated <see cref="ControlPlaneOptions"/>
    /// and leaves this empty.</summary>
    public string ControlPlaneConnectionString { get; set; } = string.Empty;
}
