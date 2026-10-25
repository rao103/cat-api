namespace AlarmIngestor.Interfaces;

public interface IRawObjectStore
{
    Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken);

    Task<bool> ExistsAsync(string path, CancellationToken cancellationToken);

    Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken);
}
