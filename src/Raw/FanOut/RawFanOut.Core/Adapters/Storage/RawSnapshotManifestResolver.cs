using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using ProductionIngestor.Contracts;

namespace CAT.RawFanOut.Adapters.Storage;

/// <summary>Resolves the shared Production/Outage refresh manifest and validates its pointer.</summary>
internal sealed class RawSnapshotManifestResolver(IRawObjectStore objects, FanOutOptions options)
{
    public async Task<RawEnvelope> ResolveAsync(RawEnvelope artifact, CancellationToken cancellationToken)
    {
        if (artifact.ManifestPath is not { } manifestPath) return artifact;
        if (artifact.CanonicalEnvelope is null)
            return await new OutageSnapshotManifestResolver(objects, options).ResolveAsync(artifact, cancellationToken);
        await using var stream = await objects.OpenSeekableAsync(manifestPath, cancellationToken);
        if (stream.Length > options.MaxPlanBytes) throw new InvalidDataException("Raw snapshot manifest is too large.");
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        return ResolveCanonical(artifact, document.RootElement);
    }
    private RawEnvelope ResolveCanonical(RawEnvelope artifact, JsonElement root)
    {
        var manifest = root.Deserialize<RawSnapshotManifestV1>()
            ?? throw new InvalidDataException("Missing Raw snapshot manifest.");
        manifest.Validate(allowLegacyPayloadSchema: true);
        if (manifest.Parts.Count > options.MaxFilesPerExtraction || manifest.ScopeWtgNames.Count > options.MaxIdentities)
            throw new InvalidDataException("Snapshot exceeds configured file or scope limits.");
        // Compare every contract field except observability metadata. The pointer cannot widen scope.
        static string Identity(RawArtifactAvailableV2 envelope)
        {
            var node = System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(envelope))!.AsObject();
            node.Remove("TraceId");
            return node.ToJsonString();
        }
        if (Identity(manifest.Envelope) != Identity(artifact.CanonicalEnvelope!))
            throw new InvalidDataException("Manifest envelope does not match the received pointer.");
        var prefix = $"raw/{artifact.SourceType.ToLowerInvariant()}/windfarm={artifact.WindfarmName}/mode=refresh/extraction={artifact.ExtractionToken.ToString(CultureInfo.InvariantCulture)}/";
        if (RawEnvelope.ValidatePath(prefix.TrimEnd('/')) + "/snapshot/manifest.json" != artifact.ManifestPath)
            throw new InvalidDataException("Snapshot manifest path does not match the extraction identity.");
        var paths = manifest.Parts.Select(part => RawEnvelope.ValidatePath(part.BlobPath)).ToArray();
        if (paths.Any(path => !path.StartsWith(prefix, StringComparison.Ordinal) || !path.EndsWith(".parquet", StringComparison.Ordinal) ||
            path.EndsWith("-quarantine.parquet", StringComparison.Ordinal)) || paths.Distinct(StringComparer.Ordinal).Count() != paths.Length)
            throw new InvalidDataException("Snapshot part is outside its immutable extraction directory.");
        ProductionDateRangeV1[]? ranges = null;
        if (artifact.SourceType == "Production")
        {
            if (manifest.RequestedIntervals.Any(interval => interval.StartInclusiveUtc.TimeOfDay != TimeSpan.Zero || interval.EndExclusiveUtc.TimeOfDay != TimeSpan.Zero))
                throw new InvalidDataException("Production refresh scope must use whole UTC days.");
            var days = manifest.RequestedIntervals.Sum(interval => (interval.EndExclusiveUtc - interval.StartInclusiveUtc).TotalDays);
            if (days > options.MaxScopeDatesPerExtraction) throw new InvalidDataException("Production refresh date limit exceeded.");
            ranges = manifest.RequestedIntervals.OrderBy(interval => interval.StartInclusiveUtc).Select(interval => new ProductionDateRangeV1(
                interval.StartInclusiveUtc.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                interval.EndExclusiveUtc.AddDays(-1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))).ToArray();
        }
        return artifact with { CanonicalManifest = manifest, Paths = paths, ExpectedArtifactCount = paths.Length,
            ScopeWtgNames = manifest.ScopeWtgNames.ToArray(), RequestedScope = ranges,
            SnapshotObservedAtUtc = new DateTimeOffset(manifest.SnapshotAsOfUtc) };
    }

}
