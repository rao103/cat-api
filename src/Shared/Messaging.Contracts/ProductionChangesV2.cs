namespace ProductionIngestor.Contracts;

/// <summary>
/// Versioned changed-row Production Raw notification. Unlike <see cref="RawArtifactAvailableV1"/>,
/// one part may contain rows for many production dates, so no per-message business date is carried.
/// Fan-out and Harmonizer must be upgraded to consume this contract before any producer publishes
/// it; see <c>IngestionOptions.ProductionChangesEnabled</c>.
/// </summary>
public sealed class ProductionChangesV2
{
    /// <summary>Version of this SQS JSON contract; independent of the referenced Parquet schema.</summary>
    public int EnvelopeVersion { get; init; } = 2;

    public string MessageKind { get; init; } = "RawArtifactAvailable";

    /// <summary>Version of the Production Parquet row schema referenced by this message.</summary>
    public int PayloadSchemaVersion { get; init; } = ProductionParquetSchemaV2.Version;

    public string ExtractionMode { get; init; } = "Incremental";

    public string PayloadKind { get; init; } = "ProductionChanges";

    public required string SourceType { get; init; }

    public required string WindfarmId { get; init; }

    public required string WindfarmName { get; init; }

    public required string RunId { get; init; }

    public required long ExtractionToken { get; init; }

    public required string AttemptId { get; init; }

    /// <summary>Axis the query bounds below are expressed against.</summary>
    public required string QueryAxis { get; init; }

    public required DateTime QueryStartInclusiveUtc { get; init; }

    public required DateTime QueryEndExclusiveUtc { get; init; }

    /// <summary>1-based index of the bounded update-time window this part belongs to.</summary>
    public required int QueryWindowNumber { get; init; }

    public required int PartNumber { get; init; }

    public required string BlobPath { get; init; }

    public required long RowCount { get; init; }

    public string? TraceId { get; init; }
}
