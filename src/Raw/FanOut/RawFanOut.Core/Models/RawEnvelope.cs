using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Serialization;

namespace CAT.RawFanOut.Models;

// Consumer-local envelope: v2 Production dates come from rows, not query windows.
public sealed record RawEnvelope
{
    public required string SourceType { get; init; }
    public required int WindfarmId { get; init; }
    public DateOnly? DataDate { get; init; }
    public required long ExtractionToken { get; init; }
    public required string[] Paths { get; init; }
    public required string RunId { get; init; }
    public string? TraceId { get; init; }
    public bool IsFullSnapshot { get; init; }
    public required string OriginalBody { get; init; }
    public required string ArtifactId { get; init; }
    public string PayloadKind { get; init; } = "CanonicalEvents";
    public CAT.Messaging.Contracts.SnapshotScope? Snapshot { get; init; }
    public CAT.Messaging.Contracts.EventSnapshotScopeV3? EventSnapshotScope { get; init; }
    public bool EventScopeWtgIdsSpecified { get; init; }
    public CAT.Messaging.Contracts.RawArtifactAvailableV2? CanonicalEnvelope { get; init; }
    public CAT.Messaging.Contracts.RawSnapshotManifestV1? CanonicalManifest { get; init; }
    public string? ManifestPath { get; init; }
    public DateTimeOffset? SnapshotObservedAtUtc { get; init; }
    public DateTimeOffset? SourceVersionCutoffUtc { get; init; }
    public string? OperationId { get; init; }

    public bool IsExtractionReady { get; init; }
    public bool IsIncrementalChanges { get; init; }
    public string? ExtractionMode { get; init; }
    public int? PayloadSchemaVersion { get; init; }
    public string? RequestId { get; init; }
    public string? AttemptId { get; init; }
    public string? QueryAxis { get; init; }
    public ProductionIngestor.Contracts.ProductionDateRangeV1[]? RequestedScope { get; init; }
    public string[]? ScopeWtgNames { get; init; }
    public string? SourceVersionEvidence { get; init; }
    public string? ArtifactPrefix { get; init; }
    public int? ExpectedArtifactCount { get; init; }
    public string? ArtifactSelectionRule { get; init; }
    public long? ExpectedTotalRowCount { get; init; }
    public string? WindfarmName { get; init; }

    public static RawEnvelope Parse(string body, FanOutOptions options)
    {
        var parsed = ParseCore(body, options);
        using var document = JsonDocument.Parse(body);
        return parsed with { OperationId = CAT.Messaging.Contracts.OperationCorrelation.Read(document.RootElement) };
    }

    private static RawEnvelope ParseCore(string body, FanOutOptions options)
    {
        if (Encoding.UTF8.GetByteCount(body) > options.MaxMessageBytes)
            throw new InvalidDataException("Raw message exceeds configured byte limit.");
        using var document = JsonDocument.Parse(body);
        var value = document.RootElement;
        if (value.ValueKind != JsonValueKind.Object) throw new InvalidDataException("Expected JSON object.");
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in value.EnumerateObject())
            if (!names.Add(property.Name)) throw new InvalidDataException("Duplicate JSON property.");
        string Required(string name, int maxLength)
        {
            if (!value.TryGetProperty(name, out var item) || item.ValueKind != JsonValueKind.String ||
                string.IsNullOrWhiteSpace(item.GetString()) || item.GetString()!.Length > maxLength)
                throw new InvalidDataException($"Missing or invalid {name}.");
            return item.GetString()!;
        }
        if ((value.TryGetProperty("ManifestSchemaVersion", out var manifestVersion) && manifestVersion.ValueKind != JsonValueKind.Null) ||
            (value.TryGetProperty("QueryAxis", out var axis) && axis.ValueKind == JsonValueKind.String &&
                axis.GetString() == CAT.Messaging.Contracts.RawArtifactConventions.LastUpdatedUtc))
            return CanonicalRawEnvelopeParser.Parse(body);
        if ((value.TryGetProperty("MessageKind", out var messageKind) && messageKind.ValueKind == JsonValueKind.String && messageKind.GetString() == "ExtractionReady") ||
            (value.TryGetProperty("PayloadKind", out var productionKind) && productionKind.ValueKind == JsonValueKind.String && productionKind.GetString() == "ProductionChanges"))
            return ProductionEnvelopeParser.Parse(body, options);
        if (value.TryGetProperty("SourceType", out var outageSource) && outageSource.ValueKind == JsonValueKind.String &&
            string.Equals(outageSource.GetString(), "Outage", StringComparison.Ordinal))
            return OutageEnvelopeParser.Parse(body, options);
        var source = Required("SourceType", 32);
        if (source is not ("Production" or "Alarm" or "Outage" or "Curtailment" or "Adjustment"))
            throw new InvalidDataException("Unsupported SourceType.");
        if (!value.TryGetProperty("WindfarmId", out var farmValue))
            throw new InvalidDataException("WindfarmId is required.");
        var farmText = farmValue.ValueKind == JsonValueKind.String ? farmValue.GetString() : farmValue.GetRawText();
        if (!int.TryParse(farmText, NumberStyles.None, CultureInfo.InvariantCulture, out var farm) || farm <= 0)
            throw new InvalidDataException("WindfarmId must be a positive int32 ID, not a display name.");
        var run = Required("RunId", 128);
        if (!value.TryGetProperty("ExtractionToken", out var tokenValue) ||
            tokenValue.ValueKind != JsonValueKind.Number || !tokenValue.TryGetInt64(out var token) || token < 0)
            throw new InvalidDataException("ExtractionToken must be a nonnegative int64.");
        DateOnly? date = null;
        if (value.TryGetProperty("DataDate", out var dateValue) && dateValue.ValueKind != JsonValueKind.Null)
        {
            if (dateValue.ValueKind != JsonValueKind.String || !DateOnly.TryParseExact(dateValue.GetString(),
                "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
                throw new InvalidDataException("Invalid DataDate.");
            date = parsed;
        }
        var payloadKind = source == "Production" ? "ProductionSnapshot" : "CanonicalEvents";
        if (value.TryGetProperty("PayloadKind", out var kindValue))
        {
            if (kindValue.ValueKind != JsonValueKind.String) throw new InvalidDataException("Invalid PayloadKind.");
            payloadKind = kindValue.GetString()!;
        }
        if (source == "Production" && payloadKind is not ("ProductionChanges" or "ProductionSnapshot"))
            throw new InvalidDataException("Unsupported Production payload kind.");
        CAT.Messaging.Contracts.SnapshotScope? snapshot = null;
        if (value.TryGetProperty("Snapshot", out var snapshotValue) && snapshotValue.ValueKind != JsonValueKind.Null)
            snapshot = snapshotValue.Deserialize<CAT.Messaging.Contracts.SnapshotScope>();
        if (source == "Production" && payloadKind == "ProductionSnapshot" && (date is null || value.TryGetProperty("PayloadKind", out _)))
        {
            if (snapshot is null || string.IsNullOrWhiteSpace(snapshot.SnapshotId) ||
                snapshot.ScopeItemIds is null || snapshot.ScopeItemIds.Length == 0 ||
                snapshot.ScopeItemIds.Any(id => id <= 0) ||
                snapshot.ScopeItemIds.Distinct().Count() != snapshot.ScopeItemIds.Length ||
                !DateOnly.TryParseExact(snapshot.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var start) ||
                !DateOnly.TryParseExact(snapshot.EndDateExclusive, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var end) || end <= start)
                throw new InvalidDataException("ProductionSnapshot requires a complete explicit snapshot scope and all artifact paths.");
        }
        if (payloadKind == "ProductionChanges" && (HasFullSnapshotFlag(value) || snapshot is not null))
            throw new InvalidDataException("ProductionChanges cannot authorize snapshot replacement.");
        if (source != "Production" && date is not null) throw new InvalidDataException("Event DataDate must be absent or null.");
        string? path = null;
        if (value.TryGetProperty("BlobPath", out var single) && single.ValueKind != JsonValueKind.Null)
        {
            if (single.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(single.GetString()))
                throw new InvalidDataException("Invalid BlobPath.");
            path = single.GetString();
        }
        string[]? shards = null;
        if (value.TryGetProperty("ShardPaths", out var many) && many.ValueKind != JsonValueKind.Null)
        {
            if (many.ValueKind != JsonValueKind.Array || many.GetArrayLength() == 0 || many.GetArrayLength() > options.MaxShards)
                throw new InvalidDataException("Invalid ShardPaths.");
            shards = many.EnumerateArray().Select(x => x.ValueKind == JsonValueKind.String ?
                x.GetString() ?? "" : throw new InvalidDataException("Invalid shard path.")).ToArray();
        }
        if ((path is null) == (shards is null)) throw new InvalidDataException("Specify exactly one of BlobPath and ShardPaths.");
        var paths = (shards ?? new[] { path! }).Select(ValidatePath).Order(StringComparer.Ordinal).ToArray();
        if (paths.Distinct(StringComparer.Ordinal).Count() != paths.Length) throw new InvalidDataException("Duplicate shard.");
        string? trace = null;
        if (value.TryGetProperty("TraceId", out var traceValue) && traceValue.ValueKind != JsonValueKind.Null)
        {
            if (traceValue.ValueKind != JsonValueKind.String || traceValue.GetString()!.Length > 256)
                throw new InvalidDataException("Invalid TraceId.");
            trace = traceValue.GetString();
        }
        var full = false;
        if (value.TryGetProperty("IsFullSnapshot", out var fullValue))
        {
            if (fullValue.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                throw new InvalidDataException("Invalid IsFullSnapshot.");
            full = fullValue.GetBoolean();
        }
        // Metadata/body preserved for future consumers (WindowSequence, BatchKind, window bounds, etc.).
        var identity = CAT.Messaging.Contracts.RawArtifactIdentityV1.Compute(source,farm.ToString(CultureInfo.InvariantCulture),
            date?.ToString("yyyy-MM-dd",CultureInfo.InvariantCulture),token,run,paths);
        return new RawEnvelope
        {
            SourceType = source, WindfarmId = farm, DataDate = date, ExtractionToken = token,
            Paths = paths, RunId = run, TraceId = trace, IsFullSnapshot = full,
            OriginalBody = body, ArtifactId = identity, PayloadKind = payloadKind, Snapshot = snapshot
        };
    }

    private static bool HasFullSnapshotFlag(JsonElement value) =>
        value.TryGetProperty("IsFullSnapshot", out var flag) && flag.ValueKind == JsonValueKind.True;

    public static string ValidatePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || path.Length > 2048 || path.Contains('\\') ||
            path.Contains('?') || path.Contains('#') || path.Contains(':') || path.Any(char.IsControl))
            throw new InvalidDataException("Raw paths must be container-relative object names, not URLs or SAS tokens.");
        path = path.TrimStart('/');
        if (path.Split('/').Any(segment => segment is "" or "." or ".."))
            throw new InvalidDataException("Invalid path segment.");
        return path;
    }
    public static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
