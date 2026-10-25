namespace OutageIngestor.Interfaces;

/// <summary>
/// Storage abstraction for Raw artifacts. An Azure Blob implementation exists now;
/// an S3 implementation can be added later without touching Raw path/shard logic.
/// </summary>
public interface IRawObjectStore
{
    /// <summary>Uploads the given bytes to <paramref name="path"/>, overwriting any existing content (idempotent retry).</summary>
    Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken);

    /// <summary>Returns true if an object already exists at the given path.</summary>
    Task<bool> ExistsAsync(string path, CancellationToken cancellationToken);

    /// <summary>Downloads the object at the given path.</summary>
    Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken);
}
