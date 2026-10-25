using ProductionIngestor.Interfaces;

using Azure.Core;
using Azure.Storage.Blobs;

namespace ProductionIngestor.Adapters.Storage;

/// <summary>Azure Blob-backed <see cref="IRawObjectStore"/>. Replaced by an S3 implementation later.</summary>
public sealed class AzureBlobRawObjectStore : IRawObjectStore
{
    private readonly BlobContainerClient _container;

    public AzureBlobRawObjectStore(string connectionString, string containerName)
    {
        _container = new BlobContainerClient(connectionString, containerName);
    }

    /// <summary>Constructs the store against an authenticated Blob service endpoint, e.g. via DefaultAzureCredential.</summary>
    public AzureBlobRawObjectStore(Uri blobServiceUri, TokenCredential credential, string containerName)
    {
        _container = new BlobServiceClient(blobServiceUri, credential).GetBlobContainerClient(containerName);
    }

    public async Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
    {
        await _container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
        var blob = _container.GetBlobClient(NormalizePath(path));
        using var stream = new MemoryStream(content.ToArray());
        await blob.UploadAsync(stream, overwrite: true, cancellationToken: cancellationToken);
    }

    public async Task<bool> ExistsAsync(string path, CancellationToken cancellationToken)
    {
        var blob = _container.GetBlobClient(NormalizePath(path));
        var response = await blob.ExistsAsync(cancellationToken);
        return response.Value;
    }

    public async Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken)
    {
        var blob = _container.GetBlobClient(NormalizePath(path));
        var response = await blob.DownloadContentAsync(cancellationToken);
        return response.Value.Content.ToArray();
    }

    private static string NormalizePath(string path) => path.TrimStart('/');
}
