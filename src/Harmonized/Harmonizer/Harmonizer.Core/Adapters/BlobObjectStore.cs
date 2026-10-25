using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;

namespace Harmonizer.Core.Adapters;

public sealed class BlobObjectStore(BlobContainerClient container, ITelemetry<BlobObjectStore> telemetry, string storageRole)
    : IRawObjectStore, IHarmonizedArtifactStore
{
    public async Task<Stream> OpenReadAsync(string path, CancellationToken cancellationToken)
    {
        Validate(path);
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["StorageRole"] = storageRole });
        try
        {
            var stream = await container.GetBlobClient(path).OpenReadAsync(new BlobOpenReadOptions(allowModifications: false), cancellationToken);
            telemetry.Debug("Blob opened for reading. BlobPath={BlobPath}", path);
            return stream;
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            telemetry.Warning(error, "Blob read failed. BlobPath={BlobPath}", path);
            throw;
        }
    }

    public async Task WriteNewAsync(string path, Stream content, string contentType, CancellationToken cancellationToken)
    {
        Validate(path);
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["StorageRole"] = storageRole });
        try
        {
            await container.GetBlobClient(path).UploadAsync(content, new BlobUploadOptions
            {
                Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All },
                HttpHeaders = new BlobHttpHeaders { ContentType = contentType }
            }, cancellationToken);
            telemetry.Debug("Immutable Blob created. BlobPath={BlobPath}", path);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            telemetry.Warning(error, "Blob write failed. BlobPath={BlobPath}", path);
            throw;
        }
    }

    private static void Validate(string path)
    {
        if (!ObjectPath.IsSafe(path))
        {
            throw new WorkValidationException("Expected a container-relative object path.");
        }
    }
}
