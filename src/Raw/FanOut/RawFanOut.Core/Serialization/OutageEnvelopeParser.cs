using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Serialization;

internal static class OutageEnvelopeParser
{
    public static RawEnvelope Parse(string body, FanOutOptions options)
    {
        using var document = JsonDocument.Parse(body);
        var root = document.RootElement;
        string Required(string name, int maxLength = 256) => Optional(name) is { Length: > 0 } value &&
            !string.IsNullOrWhiteSpace(value) && value.Length <= maxLength
                ? value
                : throw new InvalidDataException($"Missing or invalid {name}.");

        string? Optional(string name)
        {
            if (!root.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null) return null;
            if (value.ValueKind != JsonValueKind.String) throw new InvalidDataException($"Invalid {name}.");
            return value.GetString();
        }

        if (root.ValueKind != JsonValueKind.Object) throw new InvalidDataException("Expected Outage JSON object.");
        if (Encoding.UTF8.GetByteCount(body) > options.MaxMessageBytes) throw new InvalidDataException("Raw message exceeds configured byte limit.");
        ValidateOptionalVersion(root, "EnvelopeVersion", 2);
        ValidateOptionalVersion(root, "PayloadSchemaVersion", 1);
        if (Optional("MessageKind") is { } messageKind && messageKind != "RawArtifactAvailable")
            throw new InvalidDataException("Unsupported Outage message kind.");

        var farmText = root.TryGetProperty("WindfarmId", out var farmValue)
            ? farmValue.ValueKind == JsonValueKind.String ? farmValue.GetString() : farmValue.GetRawText()
            : null;
        if (!int.TryParse(farmText, NumberStyles.None, CultureInfo.InvariantCulture, out var windfarmId) || windfarmId <= 0)
            throw new InvalidDataException("WindfarmId must be a positive numeric ID.");
        var runId = Required("RunId", 128);
        if (!root.TryGetProperty("ExtractionToken", out var tokenValue) || !tokenValue.TryGetInt64(out var extractionToken) || extractionToken < 0)
            throw new InvalidDataException("Invalid ExtractionToken.");
        if (root.TryGetProperty("DataDate", out var dataDate) && dataDate.ValueKind != JsonValueKind.Null)
            throw new InvalidDataException("Outage work has no DataDate.");

        var payloadKind = Required("PayloadKind", 64);
        var isSnapshot = payloadKind == "OutageSnapshot";
        if (payloadKind is not ("OutageEvents" or "OutageDeletedEvents" or "OutageSnapshot"))
            throw new InvalidDataException("Unsupported Outage payload kind.");
        var extractionMode = Optional("ExtractionMode") ?? (isSnapshot ? "Refresh" : "Incremental");
        if (extractionMode != (isSnapshot ? "Refresh" : "Incremental"))
            throw new InvalidDataException("Outage payload kind and extraction mode disagree.");

        var isFullSnapshot = false;
        if (root.TryGetProperty("IsFullSnapshot", out var fullSnapshot))
        {
            if (fullSnapshot.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
                throw new InvalidDataException("Invalid IsFullSnapshot.");
            isFullSnapshot = fullSnapshot.GetBoolean();
        }
        if (isFullSnapshot && !isSnapshot)
            throw new InvalidDataException("Incremental Outage cannot declare full-snapshot authority.");

        var blobPath = Optional("BlobPath");
        var manifestPath = Optional("ManifestPath");
        var hasShardArray = root.TryGetProperty("ShardPaths", out var shardValue) && shardValue.ValueKind != JsonValueKind.Null;
        var paths = new List<string>();
        if (blobPath is not null) paths.Add(RawEnvelope.ValidatePath(blobPath));
        if (hasShardArray)
        {
            if (shardValue.ValueKind != JsonValueKind.Array || blobPath is not null)
                throw new InvalidDataException("Specify BlobPath or ShardPaths, not both.");
            foreach (var shard in shardValue.EnumerateArray())
            {
                if (shard.ValueKind != JsonValueKind.String)
                    throw new InvalidDataException("Invalid Outage shard path.");
                paths.Add(RawEnvelope.ValidatePath(shard.GetString() ?? string.Empty));
            }
        }
        if (manifestPath is not null)
        {
            if (!isSnapshot || blobPath is not null || hasShardArray)
                throw new InvalidDataException("ManifestPath is only valid for a snapshot and cannot be combined with direct paths.");
            manifestPath = RawEnvelope.ValidatePath(manifestPath);
        }
        else if (paths.Count == 0 && !(isSnapshot && hasShardArray))
        {
            throw new InvalidDataException("Outage artifact requires a Raw path reference.");
        }
        if (paths.Count > options.MaxShards || paths.Distinct(StringComparer.Ordinal).Count() != paths.Count)
            throw new InvalidDataException("Invalid or duplicate Outage shard paths.");

        long? rowCount = null;
        if (root.TryGetProperty("RowCount", out var rowCountValue) && rowCountValue.ValueKind != JsonValueKind.Null)
        {
            if (!rowCountValue.TryGetInt64(out var parsedCount) || parsedCount < 0 || parsedCount > options.MaxRowsPerArtifact)
                throw new InvalidDataException("Invalid Outage RowCount.");
            rowCount = parsedCount;
        }
        if (isSnapshot && manifestPath is null)
        {
            if (rowCount is null) throw new InvalidDataException("Direct Outage snapshots require RowCount.");
            if (paths.Count == 0 && rowCount != 0)
                throw new InvalidDataException("An empty direct snapshot must declare RowCount=0.");
        }
        if (!isSnapshot && paths.Count == 0)
            throw new InvalidDataException("Incremental Outage requires a nonempty Raw path reference, even for zero rows.");

        var eventScope = isSnapshot ? ParseSnapshotScope(root) : null;
        if (!isSnapshot && (root.TryGetProperty("SnapshotScope", out var suppliedScope) && suppliedScope.ValueKind != JsonValueKind.Null))
            throw new InvalidDataException("Incremental Outage cannot carry snapshot scope.");
        var sourceVersionCutoff = ReadOptionalRootUtc(root, "SourceVersionCutoffUtc");
        if (!isSnapshot && sourceVersionCutoff is not null)
            throw new InvalidDataException("Incremental Outage cannot carry a snapshot version cutoff.");
        var snapshotObservedAt = ReadOptionalRootUtc(root, "SnapshotAsOfUtc");
        if (!isSnapshot && snapshotObservedAt is not null)
            throw new InvalidDataException("Incremental Outage cannot carry snapshot observation time.");

        var windfarmName = Optional("WindfarmName");
        if (windfarmName is { Length: > 256 }) throw new InvalidDataException("WindfarmName is too long.");
        var traceId = Optional("TraceId");
        if (traceId is { Length: > 256 }) throw new InvalidDataException("TraceId is too long.");
        var attemptId = Optional("AttemptId");
        if (attemptId is { Length: > 128 }) throw new InvalidDataException("AttemptId is too long.");
        var operationId = Optional("OperationId");
        OperationCorrelation.Validate(operationId);

        var sortedPaths = paths.Order(StringComparer.Ordinal).ToArray();
        var artifactId = !isSnapshot
            ? RawArtifactIdentityV1.Compute("Outage", windfarmId.ToString(CultureInfo.InvariantCulture), null,
                extractionToken, runId, sortedPaths)
            : RawEnvelope.Hash(JsonSerializer.Serialize(new
            {
                Version = 3,
                SourceType = "Outage",
                WindfarmId = windfarmId,
                ExtractionToken = extractionToken,
                RunId = runId,
                PayloadKind = payloadKind,
                Paths = sortedPaths,
                ManifestPath = manifestPath,
                SnapshotScope = eventScope,
                IsFullSnapshot = isFullSnapshot,
                SourceVersionCutoffUtc = sourceVersionCutoff,
                RowCount = rowCount
            }));

        return new RawEnvelope
        {
            SourceType = "Outage",
            WindfarmId = windfarmId,
            ExtractionToken = extractionToken,
            Paths = sortedPaths,
            RunId = runId,
            TraceId = traceId,
            AttemptId = attemptId,
            OperationId = operationId,
            OriginalBody = body,
            ArtifactId = artifactId,
            PayloadKind = payloadKind,
            IsFullSnapshot = isFullSnapshot,
            ExtractionMode = extractionMode,
            WindfarmName = windfarmName,
            ExpectedTotalRowCount = rowCount,
            EventSnapshotScope = eventScope,
            EventScopeWtgIdsSpecified = IsWtgScopeExplicit(root),
            ManifestPath = manifestPath,
            SnapshotObservedAtUtc = snapshotObservedAt,
            SourceVersionCutoffUtc = sourceVersionCutoff
        };
    }

    private static EventSnapshotScopeV3 ParseSnapshotScope(JsonElement root)
    {
        EventSnapshotScopeV3? declared = null;
        if (root.TryGetProperty("SnapshotScope", out var scopeValue) && scopeValue.ValueKind != JsonValueKind.Null)
        {
            if (scopeValue.ValueKind != JsonValueKind.Object ||
                !scopeValue.TryGetProperty("Kind", out var kindValue) || kindValue.ValueKind != JsonValueKind.String ||
                kindValue.GetString() != "EventsOverlappingInterval")
                throw new InvalidDataException("Unsupported Outage snapshot scope.");
            var start = ReadUtc(scopeValue, "StartInclusiveUtc");
            var end = ReadUtc(scopeValue, "EndExclusiveUtc");
            var wtgIds = ReadWtgIds(scopeValue);
            declared = new EventSnapshotScopeV3 { Kind = "EventsOverlappingInterval", StartInclusiveUtc = start, EndExclusiveUtc = end, WtgIds = wtgIds };
        }

        var windowStart = ReadOptionalUtcPair(root, "WindowStartUtc", "WindowEndUtc");
        var queryStart = ReadOptionalUtcPair(root, "QueryStartInclusiveUtc", "QueryEndExclusiveUtc");
        var fallback = windowStart ?? queryStart;
        if (declared is null && fallback is null)
            throw new InvalidDataException("Outage snapshot requires explicit interval scope.");
        if (declared is not null && windowStart is { } legacy &&
            (declared.StartInclusiveUtc != legacy.Start || declared.EndExclusiveUtc != legacy.End))
            throw new InvalidDataException("Window bounds and SnapshotScope disagree.");
        if (declared is not null && queryStart is { } query &&
            (declared.StartInclusiveUtc != query.Start || declared.EndExclusiveUtc != query.End))
            throw new InvalidDataException("Query bounds and SnapshotScope disagree.");
        if (windowStart is { } window && queryStart is { } queryPair &&
            (window.Start != queryPair.Start || window.End != queryPair.End))
            throw new InvalidDataException("Window bounds and query bounds disagree.");

        var selected = declared ?? new EventSnapshotScopeV3
        {
            Kind = "EventsOverlappingInterval",
            StartInclusiveUtc = fallback!.Value.Start,
            EndExclusiveUtc = fallback.Value.End,
            WtgIds = []
        };
        if (selected.EndExclusiveUtc <= selected.StartInclusiveUtc ||
            selected.WtgIds.Any(id => id <= 0) || selected.WtgIds.Distinct().Count() != selected.WtgIds.Length)
            throw new InvalidDataException("Invalid Outage snapshot interval or WTG scope.");
        return selected;
    }

    private static (DateTimeOffset Start, DateTimeOffset End)? ReadOptionalUtcPair(JsonElement root, string startName, string endName)
    {
        var hasStart = root.TryGetProperty(startName, out var startValue) && startValue.ValueKind != JsonValueKind.Null;
        var hasEnd = root.TryGetProperty(endName, out var endValue) && endValue.ValueKind != JsonValueKind.Null;
        if (hasStart != hasEnd) throw new InvalidDataException($"{startName} and {endName} must be supplied together.");
        return hasStart ? (ReadUtcValue(startValue, startName), ReadUtcValue(endValue, endName)) : null;
    }

    private static DateTimeOffset ReadUtc(JsonElement owner, string name)
    {
        if (!owner.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String ||
            value.ValueKind == JsonValueKind.Null)
            throw new InvalidDataException($"{name} must be an explicit UTC timestamp.");
        return ReadUtcValue(value, name);
    }

    private static DateTimeOffset? ReadOptionalRootUtc(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null) return null;
        return ReadUtcValue(value, name);
    }

    private static DateTimeOffset ReadUtcValue(JsonElement value, string name)
    {
        if (value.ValueKind != JsonValueKind.String || !value.TryGetDateTimeOffset(out var parsed) ||
            parsed.Offset != TimeSpan.Zero)
            throw new InvalidDataException($"{name} must be an explicit UTC timestamp.");
        return parsed;
    }

    private static int[] ReadWtgIds(JsonElement scope)
    {
        if (!scope.TryGetProperty("WtgIds", out var value)) return [];
        if (value.ValueKind != JsonValueKind.Array || value.GetArrayLength() == 0)
            throw new InvalidDataException("An explicit Outage WTG scope must not be empty.");
        var ids = value.EnumerateArray().Select(item => item.TryGetInt32(out var id) ? id : 0).ToArray();
        if (ids.Any(id => id <= 0) || ids.Distinct().Count() != ids.Length)
            throw new InvalidDataException("Outage WTG scope IDs must be positive and unique.");
        return ids;
    }

    private static bool IsWtgScopeExplicit(JsonElement root) =>
        root.TryGetProperty("SnapshotScope", out var scope) && scope.ValueKind == JsonValueKind.Object &&
        scope.TryGetProperty("WtgIds", out _);

    private static void ValidateOptionalVersion(JsonElement root, string name, int expected)
    {
        if (root.TryGetProperty(name, out var value) &&
            (!value.TryGetInt32(out var actual) || actual != expected))
            throw new InvalidDataException($"Unsupported {name}.");
    }
}