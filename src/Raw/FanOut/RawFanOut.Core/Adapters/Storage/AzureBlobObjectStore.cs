using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.Adapters.Storage;

public sealed class AzureBlobObjectStore(BlobContainerClient container, FanOutOptions options,
    string pathPrefixToStrip = "") : IRawObjectStore
{
    public async IAsyncEnumerable<string> ListPathsAsync(string prefix,
        [EnumeratorCancellation] CancellationToken ct)
    {
        prefix = RawEnvelope.ValidatePath(prefix.TrimEnd('/')) + "/";
        var storagePrefix = prefix;
        if (!string.IsNullOrEmpty(pathPrefixToStrip))
        {
            if (!pathPrefixToStrip.EndsWith("/", StringComparison.Ordinal))
                throw new InvalidDataException("The configured Raw storage prefix must end with a slash.");
            if (!prefix.StartsWith(pathPrefixToStrip, StringComparison.Ordinal))
                throw new InvalidDataException("Sealed extraction prefix does not match the configured Raw storage prefix.");
            storagePrefix = prefix[pathPrefixToStrip.Length..];
        }

        await foreach (var blob in container.GetBlobsAsync(BlobTraits.None, BlobStates.None, storagePrefix, ct).WithCancellation(ct))
        {
            ct.ThrowIfCancellationRequested();
            var path = string.IsNullOrEmpty(pathPrefixToStrip) ? blob.Name : pathPrefixToStrip + blob.Name;
            yield return RawEnvelope.ValidatePath(path);
        }
    }

    public async Task<Stream> OpenSeekableAsync(string path, CancellationToken ct)
    {
        path = RawEnvelope.ValidatePath(path);
        if (!string.IsNullOrEmpty(pathPrefixToStrip))
        {
            if (!pathPrefixToStrip.EndsWith('/') || !path.StartsWith(pathPrefixToStrip, StringComparison.Ordinal))
                throw new InvalidDataException("Raw path does not match the configured prefix.");
            path = RawEnvelope.ValidatePath(path[pathPrefixToStrip.Length..]);
        }
        var blob = container.GetBlobClient(path);
        var properties = await blob.GetPropertiesAsync(cancellationToken: ct);
        if (properties.Value.ContentLength > options.MaxFileBytes)
            throw new InvalidDataException("Raw file exceeds configured byte limit.");
        var download = await blob.DownloadStreamingAsync(new BlobDownloadOptions
        {
            Conditions = new BlobRequestConditions { IfMatch = properties.Value.ETag }
        }, ct);
        await using var source = download.Value.Content;
        var local = new FileStream(Path.Combine(Path.GetTempPath(), $"raw-fanout-{Guid.NewGuid():N}.parquet"),
            FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 65536,
            FileOptions.Asynchronous | FileOptions.DeleteOnClose);
        try
        {
            var buffer = new byte[65536]; long copied = 0;
            int read;
            while ((read = await source.ReadAsync(buffer, ct)) != 0)
            {
                copied += read;
                if (copied > options.MaxFileBytes) throw new InvalidDataException("Raw file grew beyond configured limit.");
                await local.WriteAsync(buffer.AsMemory(0, read), ct);
            }
            if (copied != properties.Value.ContentLength) throw new IOException("Incomplete Raw download.");
            local.Position = 0;
            return local;
        }
        catch { await local.DisposeAsync(); throw; }
    }
}
