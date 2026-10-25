using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Adapters.Storage;

internal sealed class OutageSnapshotManifestResolver(IRawObjectStore objects, FanOutOptions options)
{
    public async Task<RawEnvelope> ResolveAsync(RawEnvelope artifact, CancellationToken cancellationToken)
    {
        if (artifact.ManifestPath is not { } manifestPath) return artifact;
        await using var stream = await objects.OpenSeekableAsync(manifestPath, cancellationToken);
        if (stream.Length > options.MaxPlanBytes) throw new InvalidDataException("Outage snapshot manifest is too large.");
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty("SchemaVersion", out var schemaValue) || !schemaValue.TryGetInt32(out var schemaVersion) || schemaVersion != 1 ||
            !root.TryGetProperty("Parts", out var partsValue) || partsValue.ValueKind != JsonValueKind.Array ||
            partsValue.GetArrayLength() > options.MaxShards)
            throw new InvalidDataException("Unsupported Outage snapshot manifest schema.");

        var manifestFarm = RequiredString(root, "WindfarmId", 64);
        if (!int.TryParse(manifestFarm, NumberStyles.None, CultureInfo.InvariantCulture, out var farmId) || farmId != artifact.WindfarmId)
            throw new InvalidDataException("Outage manifest farm ID does not match its envelope.");
        var windfarmName = RequiredString(root, "WindfarmName", 256);
        if (artifact.WindfarmName is { } envelopeFarmName && !string.Equals(envelopeFarmName, windfarmName, StringComparison.Ordinal))
            throw new InvalidDataException("Outage manifest farm name does not match its envelope.");
        if (!string.Equals(RequiredString(root, "RunId", 128), artifact.RunId, StringComparison.Ordinal) ||
            !root.TryGetProperty("ExtractionToken", out var tokenValue) || !tokenValue.TryGetInt64(out var token) || token != artifact.ExtractionToken)
            throw new InvalidDataException("Outage manifest run identity does not match its envelope.");

        var scopeStart = ReadUtc(root, "ScopeStartUtc");
        var scopeEnd = ReadUtc(root, "ScopeEndUtc");
        var observedAt = ReadUtc(root, "SnapshotAsOfUtc");
        if (artifact.SnapshotObservedAtUtc is { } declaredObservedAt && declaredObservedAt != observedAt)
            throw new InvalidDataException("Outage manifest observation time does not match its envelope.");
        if (artifact.EventSnapshotScope is not { } scope ||
            scope.StartInclusiveUtc != scopeStart || scope.EndExclusiveUtc != scopeEnd)
            throw new InvalidDataException("Outage manifest scope does not match its envelope.");

        if (!root.TryGetProperty("RecordCount", out var countValue) || !countValue.TryGetInt32(out var recordCount) ||
            recordCount < 0 || recordCount > options.MaxRowsPerArtifact ||
            artifact.ExpectedTotalRowCount is { } declaredCount && declaredCount != recordCount)
            throw new InvalidDataException("Outage manifest record count is invalid or disagrees with the envelope.");

        var directoryEnd = manifestPath.LastIndexOf('/');
        if (directoryEnd < 0) throw new InvalidDataException("Outage manifest must be stored under a snapshot directory.");
        var directory = manifestPath[..(directoryEnd + 1)];
        if (!directory.Contains("/snapshot/", StringComparison.Ordinal))
            throw new InvalidDataException("Outage manifest is outside its snapshot directory.");
        var paths = new List<string>(partsValue.GetArrayLength());
        foreach (var part in partsValue.EnumerateArray())
        {
            if (part.ValueKind != JsonValueKind.String)
                throw new InvalidDataException("Outage manifest Parts must contain path strings.");
            var path = RawEnvelope.ValidatePath(part.GetString() ?? string.Empty);
            if (!path.StartsWith(directory, StringComparison.Ordinal) || !path.EndsWith(".parquet", StringComparison.Ordinal))
                throw new InvalidDataException("Outage manifest part is outside its snapshot directory.");
            paths.Add(path);
        }
        if (paths.Distinct(StringComparer.Ordinal).Count() != paths.Count || (paths.Count == 0) != (recordCount == 0))
            throw new InvalidDataException("Outage manifest parts are duplicated or inconsistent with its record count.");

        return artifact with
        {
            Paths = paths.Order(StringComparer.Ordinal).ToArray(),
            WindfarmName = windfarmName,
            ExpectedTotalRowCount = recordCount,
            SnapshotObservedAtUtc = observedAt
        };
    }

    private static string RequiredString(JsonElement root, string name, int maxLength)
    {
        if (!root.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(value.GetString()) || value.GetString()!.Length > maxLength)
            throw new InvalidDataException($"Invalid Outage manifest {name}.");
        return value.GetString()!;
    }

    private static DateTimeOffset ReadUtc(JsonElement root, string name)
    {
        if (!root.TryGetProperty(name, out var value) || value.ValueKind != JsonValueKind.String ||
            !value.TryGetDateTimeOffset(out var parsed) || parsed.Offset != TimeSpan.Zero)
            throw new InvalidDataException($"Outage manifest {name} must be UTC.");
        return parsed;
    }
}