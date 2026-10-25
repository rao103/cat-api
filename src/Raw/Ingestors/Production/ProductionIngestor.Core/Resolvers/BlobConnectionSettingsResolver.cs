using ProductionIngestor.Configuration;

namespace ProductionIngestor.Resolvers;

/// <summary>Resolves Blob access as either a connection string or a service URI plus DefaultAzureCredential, without acquiring credentials.</summary>
public static class BlobConnectionSettingsResolver
{
    public static BlobConnectionSettings Resolve(SourceOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.BlobContainerName);

        if (!string.IsNullOrWhiteSpace(options.BlobConnectionString))
            return BlobConnectionSettings.ForConnectionString(options.BlobConnectionString, options.BlobContainerName);

        if (string.IsNullOrWhiteSpace(options.BlobServiceUri))
            throw new ArgumentException("Either Source:BlobConnectionString or Source:BlobServiceUri must be configured.");

        if (!Uri.TryCreate(options.BlobServiceUri, UriKind.Absolute, out var serviceUri) || serviceUri.Scheme != Uri.UriSchemeHttps)
            throw new ArgumentException("Source:BlobServiceUri must be an absolute HTTPS URI.");

        return BlobConnectionSettings.ForServiceUri(serviceUri, options.BlobContainerName);
    }
}

/// <summary>Resolved Blob access mode: a connection string or a service URI requiring token-credential authentication.</summary>
public sealed record BlobConnectionSettings
{
    public string? ConnectionString { get; private init; }

    public Uri? ServiceUri { get; private init; }

    public required string ContainerName { get; init; }

    public bool UsesConnectionString => ConnectionString is not null;

    public static BlobConnectionSettings ForConnectionString(string connectionString, string containerName)
        => new() { ConnectionString = connectionString, ContainerName = containerName };

    public static BlobConnectionSettings ForServiceUri(Uri serviceUri, string containerName)
        => new() { ServiceUri = serviceUri, ContainerName = containerName };
}
