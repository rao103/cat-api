namespace ProductionIngestor.Contracts;

public sealed record ProductionDateRangeV1(string StartDate, string EndDateInclusive);

public sealed record ProductionArtifactReferenceV1(
    string BlobPath,
    int QueryWindowNumber,
    int PartNumber,
    long RowCount);

public sealed record ProductionQueryWindowV1(
    int WindowNumber,
    DateTime QueryStartInclusiveUtc,
    DateTime QueryEndExclusiveUtc,
    IReadOnlyList<ProductionDateRangeV1> SelectedScope,
    long RowCount,
    int ArtifactCount,
    bool Completed);

/// <summary>Attempt-scoped completeness declaration emitted only after every refresh query and upload succeeds.</summary>
public sealed record ProductionExtractionReadyV1(
    int EnvelopeVersion,
    string MessageKind,
    string SourceType,
    int WindfarmId,
    string WindfarmName,
    string ExtractionMode,
    string PayloadKind,
    int PayloadSchemaVersion,
    string QueryAxis,
    string RequestId,
    string AttemptId,
    long ExtractionToken,
    IReadOnlyList<ProductionDateRangeV1> RequestedScope,
    IReadOnlyList<ProductionQueryWindowV1> CompletedWindows,
    IReadOnlyList<ProductionArtifactReferenceV1> Artifacts,
    long TotalRowCount,
    bool IsEmptyResult,
    IReadOnlyList<string> ScopeWtgIds,
    string? SourceVersionEvidence,
    string? TraceId = null,
    string? ArtifactPrefix = null,
    int? ExpectedArtifactCount = null,
    string? ArtifactSelectionRule = null);

public static class ProductionArtifactSelectionRules
{
    public const string DirectParquetFilesV1 = "DirectParquetFilesV1";
}