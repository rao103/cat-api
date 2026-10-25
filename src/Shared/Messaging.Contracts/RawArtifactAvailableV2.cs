using System;
using System.IO;
using System.Text.Json.Serialization;

namespace CAT.Messaging.Contracts;

/// <summary>Transport metadata for one immutable Raw artifact; canonical event placement is downstream work.</summary>
public sealed class RawArtifactAvailableV2
{
    public int EnvelopeVersion { get; init; } = 2;
    public string MessageKind { get; init; } = "RawArtifactAvailable";
    public required int PayloadSchemaVersion { get; init; }
    public required string ExtractionMode { get; init; }
    public required string PayloadKind { get; init; }
    public required string SourceType { get; init; }
    // Read older numeric strings, but always publish a JSON number.
    [JsonNumberHandling(JsonNumberHandling.AllowReadingFromString)]
    public required int WindfarmId { get; init; }
    public required string WindfarmName { get; init; }
    public required string RunId { get; init; }
    public required long ExtractionToken { get; init; }
    public string? AttemptId { get; init; }
    public required string QueryAxis { get; init; }
    public required DateTime QueryStartInclusiveUtc { get; init; }
    public required DateTime QueryEndExclusiveUtc { get; init; }
    public int? QueryWindowNumber { get; init; }
    public int? PartNumber { get; init; }
    public string? BlobPath { get; init; }
    public string? ManifestPath { get; init; }
    public int? ManifestSchemaVersion { get; init; }
    public long? RowCount { get; init; }
    public RawSnapshotScopeV2? SnapshotScope { get; init; }
    public string? OperationId { get; init; }
    public string? TraceId { get; init; }

    /// <summary>Validate the canonical producer contract before publication. Legacy readers may accept older aliases.</summary>
    public void Validate(bool allowLegacyPayloadSchema = false)
    {
        OperationCorrelation.Validate(OperationId);
        if (EnvelopeVersion != RawArtifactConventions.EnvelopeVersion || MessageKind != RawArtifactConventions.MessageKind ||
            WindfarmId <= 0 || string.IsNullOrWhiteSpace(WindfarmName) ||
            string.IsNullOrWhiteSpace(RunId) || ExtractionToken < 0 ||
            AttemptId != RawArtifactConventions.AttemptId(ExtractionToken) ||
            (BlobPath is null) == (ManifestPath is null) ||
            (BlobPath is not null && string.IsNullOrWhiteSpace(BlobPath)) ||
            (ManifestPath is not null && string.IsNullOrWhiteSpace(ManifestPath)) ||
            QueryStartInclusiveUtc.Kind != DateTimeKind.Utc || QueryEndExclusiveUtc.Kind != DateTimeKind.Utc ||
            QueryEndExclusiveUtc <= QueryStartInclusiveUtc || RowCount is null or < 0)
            throw new InvalidDataException("Invalid Raw artifact envelope.");

        RawArtifactConventions.ValidatePath((BlobPath ?? ManifestPath)!);
        var production = SourceType == "Production";
        if ((!production && SourceType != "Outage") || (!(allowLegacyPayloadSchema && SourceType == "Outage" && PayloadSchemaVersion == 1) && PayloadSchemaVersion != (production
                ? RawArtifactConventions.ProductionPayloadSchemaVersion : RawArtifactConventions.OutagePayloadSchemaVersion)))
            throw new InvalidDataException("Unsupported source or payload schema version.");

        if (ExtractionMode == "Incremental")
        {
            if ((production ? PayloadKind != "ProductionChanges" : PayloadKind is not ("OutageEvents" or "OutageDeletedEvents")) ||
                QueryAxis != RawArtifactConventions.LastUpdatedUtc || BlobPath is null || SnapshotScope is not null ||
                ManifestSchemaVersion is not null || QueryWindowNumber is null or < 1 || PartNumber is null or < 1 || RowCount < 1)
                throw new InvalidDataException("Invalid incremental artifact metadata.");
        }
        else if (ExtractionMode == "Refresh")
        {
            if (PayloadKind != (production ? "ProductionSnapshot" : "OutageSnapshot") ||
                QueryAxis != (production ? RawArtifactConventions.ProductionTimestampUtc : RawArtifactConventions.EventOverlapUtc) ||
                ManifestPath is null || ManifestSchemaVersion != RawSnapshotManifestV1.CurrentSchemaVersion ||
                QueryWindowNumber is not null || PartNumber is not null || SnapshotScope is null ||
                SnapshotScope.Kind != (production ? RawArtifactConventions.SelectedIntervals : RawArtifactConventions.EventsOverlappingInterval) ||
                SnapshotScope.StartInclusiveUtc.Kind != DateTimeKind.Utc || SnapshotScope.EndExclusiveUtc.Kind != DateTimeKind.Utc ||
                SnapshotScope.StartInclusiveUtc != QueryStartInclusiveUtc || SnapshotScope.EndExclusiveUtc != QueryEndExclusiveUtc)
                throw new InvalidDataException("Invalid refresh scope or manifest metadata.");
        }
        else throw new InvalidDataException("Unsupported extraction mode.");
    }

    // Kept for source compatibility; new publishers use the common validator.
    public void ValidateOutage()
    {
        Validate();
        if (SourceType != "Outage") throw new InvalidDataException("Expected Outage source.");
    }
}

/// <summary>Bounding interval only for SelectedIntervals; the manifest contains the exact, possibly disjoint scope.</summary>
public sealed record RawSnapshotScopeV2(string Kind, DateTime StartInclusiveUtc, DateTime EndExclusiveUtc);
