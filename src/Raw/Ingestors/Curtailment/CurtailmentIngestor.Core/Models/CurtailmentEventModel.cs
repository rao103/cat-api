namespace CurtailmentIngestor.Models;

/// <summary>
/// A single curtailment event row carrying full current state (Section 4.3.1) — not a delta.
/// EventId is the stable business identity; IsTombstone marks explicit deletion/invalidation.
/// CurtailmentTypeId is the source curtailment classification (Section 6.8); the ingestor
/// extracts it verbatim and does not merge adjacent intervals — that aggregation belongs to
/// the Harmonized zone.
/// </summary>
public sealed record CurtailmentEventModel(
    string EventId,
    string WindfarmId,
    string ItemId,
    string EventType,
    DateTime OriginalStartUtc,
    DateTime? OriginalEndUtc,
    string Status,
    string? CurtailmentTypeId,
    string? SourceRevision,
    DateTime SourceWatermarkUtc,
    bool IsTombstone,
    DateTime LastUpdatedDate,
    long Id);
