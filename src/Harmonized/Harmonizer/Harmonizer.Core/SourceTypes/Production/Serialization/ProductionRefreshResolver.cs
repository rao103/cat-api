using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using ProductionIngestor.Contracts;

namespace Harmonizer.Core.SourceTypes.Production.Serialization;

/// <summary>Resolves the shared pointer into the existing internal completeness model.</summary>
public static class ProductionRefreshResolver
{
    public static async Task<ProductionExtractionReadyV1?> ResolveAsync(string body, IRawObjectStore objects,
        long maximumBytes, CancellationToken cancellationToken)
    {
        using var document = JsonDocument.Parse(body);
        if (!document.RootElement.TryGetProperty("MessageKind", out var kind) || kind.GetString() != "RawArtifactAvailable")
            return null; // Retain the legacy inline completion reader.
        var pointer = document.RootElement.Deserialize<RawArtifactAvailableV2>()
            ?? throw new WorkValidationException("Missing refresh pointer.");
        pointer.Validate();
        if (pointer.SourceType != "Production" || pointer.ExtractionMode != "Refresh")
            throw new WorkValidationException("Production refresh requires an immutable Raw manifest reader.");
        var prefix = $"raw/production/windfarm={pointer.WindfarmName}/mode=refresh/extraction={pointer.ExtractionToken.ToString(CultureInfo.InvariantCulture)}/";
        if (pointer.ManifestPath != prefix + "snapshot/manifest.json")
            throw new WorkValidationException("Refresh manifest path differs from its extraction identity.");
        await using var source = await objects.OpenReadAsync(pointer.ManifestPath, cancellationToken);
        // Bound non-seekable streams as well, before deserializing a complete manifest.
        using var buffer = new MemoryStream();
        var bytes = new byte[81920];
        var limit = Math.Min(maximumBytes, 64L * 1024 * 1024);
        int read;
        while ((read = await source.ReadAsync(bytes.AsMemory(), cancellationToken)) != 0)
        {
            if (buffer.Length + read > limit) throw new WorkValidationException("Refresh manifest exceeds its size limit.");
            await buffer.WriteAsync(bytes.AsMemory(0, read), cancellationToken);
        }
        buffer.Position = 0;
        var manifest = await JsonSerializer.DeserializeAsync<RawSnapshotManifestV1>(buffer, cancellationToken: cancellationToken)
            ?? throw new WorkValidationException("Missing Raw snapshot manifest.");
        manifest.Validate();
        if (Identity(manifest.Envelope) != Identity(pointer))
            throw new WorkValidationException("Refresh manifest does not match its pointer.");
        if (manifest.Parts.Any(part => !ObjectPath.IsSafe(part.BlobPath) ||
                !part.BlobPath.StartsWith(prefix, StringComparison.Ordinal) ||
                !part.BlobPath.EndsWith(".parquet", StringComparison.Ordinal) ||
                part.BlobPath.EndsWith("-quarantine.parquet", StringComparison.Ordinal)))
            throw new WorkValidationException("Refresh part is outside its immutable extraction directory.");
        if (manifest.RequestedIntervals.Any(interval => interval.StartInclusiveUtc.TimeOfDay != TimeSpan.Zero ||
                interval.EndExclusiveUtc.TimeOfDay != TimeSpan.Zero))
            throw new WorkValidationException("Production refresh requires whole UTC days.");
        return new(2, "ExtractionReady", "Production", pointer.WindfarmId, pointer.WindfarmName,
            "Refresh", "ProductionSnapshot", pointer.PayloadSchemaVersion, "ProductionTimestamp", pointer.RunId,
            pointer.AttemptId!, pointer.ExtractionToken,
            manifest.RequestedIntervals.Select(interval => Range(interval.StartInclusiveUtc, interval.EndExclusiveUtc)).ToArray(),
            manifest.CompletedWindows.Select(window => new ProductionQueryWindowV1(window.QueryWindowNumber,
                window.QueryStartInclusiveUtc, window.QueryEndExclusiveUtc,
                [Range(window.QueryStartInclusiveUtc, window.QueryEndExclusiveUtc)], window.RowCount, window.ArtifactCount, window.Completed)).ToArray(),
            manifest.Parts.Select(part => new ProductionArtifactReferenceV1(part.BlobPath, part.QueryWindowNumber, part.PartNumber, part.RowCount)).ToArray(),
            manifest.TotalRowCount, manifest.IsEmptyResult, manifest.ScopeWtgNames, null, pointer.TraceId,
            ExpectedArtifactCount: manifest.Parts.Count);
    }

    private static ProductionDateRangeV1 Range(DateTime start, DateTime end) => new(
        start.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), end.AddDays(-1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));

    private static string Identity(RawArtifactAvailableV2 envelope)
    {
        var node = JsonNode.Parse(JsonSerializer.Serialize(envelope))!.AsObject();
        node.Remove("TraceId");
        return node.ToJsonString();
    }
}
