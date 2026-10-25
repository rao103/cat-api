using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Models;
using ProductionIngestor.Contracts;
namespace CAT.RawFanOut.Serialization;

internal static class ProductionEnvelopeParser
{
    public static RawEnvelope Parse(string body, FanOutOptions options)
    {
        using var document = JsonDocument.Parse(body);
        var value = document.RootElement;
        return StringValue(value, "MessageKind") == "ExtractionReady"
            ? ParseExtractionReady(body, value, options)
            : ParseChanges(body, value);
    }

    private static RawEnvelope ParseChanges(string body, JsonElement value)
    {
        if (IntegerValue(value, "EnvelopeVersion") != 2 || StringValue(value, "MessageKind") != "RawArtifactAvailable" ||
            StringValue(value, "SourceType") != "Production" || StringValue(value, "PayloadKind") != "ProductionChanges" ||
            StringValue(value, "ExtractionMode") != "Incremental" || StringValue(value, "QueryAxis") != "LastUpdatedDate")
            throw new InvalidDataException("Unsupported ProductionChanges contract.");
        var schema = IntegerValue(value, "PayloadSchemaVersion");
        if (schema < 1 || schema > ProductionParquetSchemaV2.Version)
            throw new InvalidDataException("Unsupported Production payload schema version.");
        if (HasValue(value, "DataDate") || HasValue(value, "ShardPaths") || HasValue(value, "Snapshot"))
            throw new InvalidDataException("ProductionChanges requires a single part without business-date or snapshot scope.");
        if (value.TryGetProperty("IsFullSnapshot", out var full) && full.ValueKind != JsonValueKind.False)
            throw new InvalidDataException("ProductionChanges cannot authorize snapshot replacement.");
        var start = UtcValue(value, "QueryStartInclusiveUtc");
        var end = UtcValue(value, "QueryEndExclusiveUtc");
        if (end <= start || IntegerValue(value, "QueryWindowNumber") < 1 || IntegerValue(value, "PartNumber") < 1)
            throw new InvalidDataException("ProductionChanges query bounds and part identity are invalid.");
        var rows = LongValue(value, "RowCount");
        var token = LongValue(value, "ExtractionToken");
        if (rows < 1 || token < 0) throw new InvalidDataException("RowCount must be positive and ExtractionToken nonnegative.");
        if (!value.TryGetProperty("WindfarmId", out var farmValue)) throw new InvalidDataException("WindfarmId is required.");
        var farmText = farmValue.ValueKind == JsonValueKind.String ? farmValue.GetString() : farmValue.GetRawText();
        if (!int.TryParse(farmText, NumberStyles.None, CultureInfo.InvariantCulture, out var farm) || farm <= 0)
            throw new InvalidDataException("WindfarmId must be a positive numeric ID.");
        var run = StringValue(value, "RunId");
        var attempt = StringValue(value, "AttemptId");
        var path = ValidatePath(StringValue(value, "BlobPath", 2048));
        var trace = HasValue(value, "TraceId") ? StringValue(value, "TraceId", 256) : null;
        return new RawEnvelope
        {
            SourceType = "Production", WindfarmId = farm, ExtractionToken = token, Paths = [path], RunId = run,
            TraceId = trace, OriginalBody = body,
            ArtifactId = CAT.Messaging.Contracts.RawArtifactIdentityV1.Compute("Production", farm.ToString(CultureInfo.InvariantCulture), null, token, run, [path]),
            IsIncrementalChanges = true, ExtractionMode = "Incremental", PayloadKind = "ProductionChanges",
            PayloadSchemaVersion = schema, RequestId = run, AttemptId = attempt, QueryAxis = "LastUpdatedDate",
            ExpectedTotalRowCount = rows
        };
    }

    private static bool HasValue(JsonElement value, string name) => value.TryGetProperty(name, out var property) && property.ValueKind != JsonValueKind.Null;
    private static string StringValue(JsonElement value, string name, int maxLength = 128)
    {
        if (!value.TryGetProperty(name, out var property) || property.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(property.GetString()) || property.GetString()!.Length > maxLength)
            throw new InvalidDataException($"Missing or invalid {name}.");
        return property.GetString()!;
    }
    private static long LongValue(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out var property) || property.ValueKind != JsonValueKind.Number || !property.TryGetInt64(out var result))
            throw new InvalidDataException($"Missing or invalid {name}.");
        return result;
    }
    private static int IntegerValue(JsonElement value, string name)
    {
        var result = LongValue(value, name);
        if (result < int.MinValue || result > int.MaxValue) throw new InvalidDataException($"Invalid {name}.");
        return (int)result;
    }
    private static DateTime UtcValue(JsonElement value, string name)
    {
        if (!value.TryGetProperty(name, out var property) || property.ValueKind != JsonValueKind.String ||
            !property.TryGetDateTime(out var result) || result.Kind != DateTimeKind.Utc)
            throw new InvalidDataException($"{name} must be a UTC timestamp.");
        return result;
    }

    private static RawEnvelope ParseExtractionReady(string body, JsonElement value, FanOutOptions options)
    {
        var ready = JsonSerializer.Deserialize<ProductionExtractionReadyV1>(body)
            ?? throw new InvalidDataException("Invalid Production extraction-ready envelope.");
        if (ready.RequestedScope is null || ready.CompletedWindows is null || ready.Artifacts is null || ready.ScopeWtgIds is null ||
            ready.RequestedScope.Any(range => range is null) || ready.CompletedWindows.Any(window => window is null || window.SelectedScope is null) ||
            ready.Artifacts.Any(artifact => artifact is null))
            throw new InvalidDataException("Extraction-ready collections must be present and non-null.");
        if (ready.CompletedWindows.Select(window => window.WindowNumber).Distinct().Count() != ready.CompletedWindows.Count ||
            !ready.CompletedWindows.Select(window => window.WindowNumber).Order().SequenceEqual(Enumerable.Range(1, ready.CompletedWindows.Count)) ||
            ready.Artifacts.Select(artifact => (artifact.QueryWindowNumber, artifact.PartNumber)).Distinct().Count() != ready.Artifacts.Count ||
            ready.Artifacts.Count > options.MaxFilesPerExtraction)
            throw new InvalidDataException("Extraction-ready windows and parts must have unique bounded identities.");
        if (ready.EnvelopeVersion != 2 || ready.MessageKind != "ExtractionReady" || ready.SourceType != "Production" ||
            ready.WindfarmId <= 0 || ready.ExtractionMode != "Refresh" || ready.PayloadKind != "ProductionSnapshot" ||
            ready.PayloadSchemaVersion < 1 || ready.PayloadSchemaVersion > ProductionParquetSchemaV2.Version ||
            ready.QueryAxis != "ProductionTimestamp" || ready.ExtractionToken < 0 ||
            string.IsNullOrWhiteSpace(ready.RequestId) || ready.RequestId.Length > 128 ||
            string.IsNullOrWhiteSpace(ready.AttemptId) || ready.AttemptId.Length > 128 ||
            string.IsNullOrWhiteSpace(ready.WindfarmName) || ready.RequestedScope.Count == 0 ||
            ready.CompletedWindows.Count == 0 || ready.ScopeWtgIds.Count > 100_000 ||
            ready.ScopeWtgIds.Any(string.IsNullOrWhiteSpace) ||
            ready.ScopeWtgIds.Distinct(StringComparer.Ordinal).Count() != ready.ScopeWtgIds.Count)
            throw new InvalidDataException("Production extraction-ready identity or scope is invalid.");

        ValidateRanges(ready.RequestedScope);
        foreach (var window in ready.CompletedWindows)
        {
            if (!window.Completed || window.WindowNumber < 1 || window.RowCount < 0 || window.ArtifactCount < 0 ||
                window.QueryStartInclusiveUtc.Kind != DateTimeKind.Utc ||
                window.QueryEndExclusiveUtc.Kind != DateTimeKind.Utc ||
                window.QueryEndExclusiveUtc <= window.QueryStartInclusiveUtc)
                throw new InvalidDataException("Extraction-ready contains an incomplete or invalid query window.");
            ValidateRanges(window.SelectedScope);
        }

        var scopeRanges = ready.CompletedWindows.SelectMany(window => window.SelectedScope).ToArray();
        if (!scopeRanges.SequenceEqual(ready.RequestedScope))
            throw new InvalidDataException("Completed query windows do not enumerate the exact requested scope.");

        string? artifactPrefix = null;
        if (ready.ArtifactPrefix is not null)
        {
            if (!ready.ArtifactPrefix.EndsWith("/", StringComparison.Ordinal) ||
                !ready.ExpectedArtifactCount.HasValue || ready.ExpectedArtifactCount.Value < 1 ||
                ready.ExpectedArtifactCount.Value > options.MaxFilesPerExtraction ||
                ready.ArtifactSelectionRule != ProductionArtifactSelectionRules.DirectParquetFilesV1 ||
                ready.Artifacts.Count != 0)
                throw new InvalidDataException("Extraction-ready sealed-prefix declaration is invalid.");
            artifactPrefix = ValidatePath(ready.ArtifactPrefix.TrimEnd('/')) + "/";
            var expectedPrefix = $"raw/production/windfarm={ready.WindfarmName}/mode=refresh/extraction={ready.ExtractionToken}/";
            if (!string.Equals(artifactPrefix, expectedPrefix, StringComparison.Ordinal))
                throw new InvalidDataException("Extraction-ready sealed prefix does not match its farm and extraction identity.");
        }
        else if (ready.ExpectedArtifactCount is not null || ready.ArtifactSelectionRule is not null)
        {
            throw new InvalidDataException("Extraction-ready prefix metadata requires an artifact prefix.");
        }

        var paths = artifactPrefix is not null ? Array.Empty<string>() : ready.Artifacts.Select(artifact =>
        {
            if (artifact.QueryWindowNumber < 1 || artifact.PartNumber < 1 || artifact.RowCount < 1)
                throw new InvalidDataException("Extraction-ready contains an invalid artifact reference.");
            return ValidatePath(artifact.BlobPath);
        }).ToArray();
        if (paths.Distinct(StringComparer.Ordinal).Count() != paths.Length)
            throw new InvalidDataException("Extraction-ready contains duplicate artifact paths.");

        foreach (var window in ready.CompletedWindows)
        {
            if (artifactPrefix is not null) continue;
            var windowArtifacts = ready.Artifacts.Where(artifact => artifact.QueryWindowNumber == window.WindowNumber).ToArray();
            if (windowArtifacts.Length != window.ArtifactCount || windowArtifacts.Sum(artifact => artifact.RowCount) != window.RowCount)
                throw new InvalidDataException("Extraction-ready window counts do not match its artifact enumeration.");
        }
        var declaredArtifactCount = artifactPrefix is null ? ready.Artifacts.Count : ready.ExpectedArtifactCount!.Value;
        var declaredRowCount = artifactPrefix is null ? ready.Artifacts.Sum(artifact => artifact.RowCount) : ready.TotalRowCount;
        if (ready.Artifacts.Any(artifact => artifact.QueryWindowNumber > ready.CompletedWindows.Count) ||
            (artifactPrefix is not null && ready.CompletedWindows.Sum(window => window.ArtifactCount) != declaredArtifactCount) ||
            declaredRowCount != ready.TotalRowCount ||
            ready.CompletedWindows.Sum(window => window.RowCount) != ready.TotalRowCount ||
            ready.IsEmptyResult != (ready.TotalRowCount == 0) ||
            ready.IsEmptyResult != (declaredArtifactCount == 0))
            throw new InvalidDataException("Extraction-ready total counts or empty-result declaration are inconsistent.");

        return new RawEnvelope
        {
            SourceType = "Production", WindfarmId = ready.WindfarmId, DataDate = null, ExtractionToken = ready.ExtractionToken,
            Paths = paths, RunId = ready.RequestId, TraceId = ready.TraceId, IsFullSnapshot = true,
            OriginalBody = body, ArtifactId = Hash(body), IsExtractionReady = true,
            ExtractionMode = ready.ExtractionMode, PayloadKind = ready.PayloadKind,
            PayloadSchemaVersion = ready.PayloadSchemaVersion, RequestId = ready.RequestId, AttemptId = ready.AttemptId,
            QueryAxis = ready.QueryAxis, RequestedScope = ready.RequestedScope.ToArray(), ScopeWtgNames = ready.ScopeWtgIds.ToArray(),
            SourceVersionEvidence = ready.SourceVersionEvidence, ArtifactPrefix = artifactPrefix,
            ExpectedArtifactCount = ready.ExpectedArtifactCount, ArtifactSelectionRule = ready.ArtifactSelectionRule,
            ExpectedTotalRowCount = ready.TotalRowCount, WindfarmName = ready.WindfarmName
        };
    }

    private static void ValidateRanges(IReadOnlyList<ProductionDateRangeV1> ranges)
    {
        if (ranges.Count == 0 || ranges.Any(range => range is null)) throw new InvalidDataException("A production scope range is required.");
        DateOnly? previousEnd = null;
        foreach (var range in ranges)
        {
            if (!DateOnly.TryParseExact(range.StartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out var start) ||
                !DateOnly.TryParseExact(range.EndDateInclusive, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out var end) || end < start ||
                (previousEnd.HasValue && start <= previousEnd.Value))
                throw new InvalidDataException("Production scope ranges must be exact, ascending, and non-overlapping.");
            previousEnd = end;
        }
    }

    private static string ValidatePath(string path) => RawEnvelope.ValidatePath(path);
    private static string Hash(string value) => RawEnvelope.Hash(value);
}
