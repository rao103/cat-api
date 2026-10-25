namespace OutageIngestor.Models;

/// <summary>
/// A single outage event row carrying full current state (Section 4.3.1) — not a delta.
/// EventId is the source OutageClassificationID. WindfarmName and WtgName are RDSPP names.
/// Numeric farm/WTG IDs come from the control-plane catalog, not from parsing those names.
/// IsTombstone marks explicit deletion/invalidation (a row from the deletion feed).
/// </summary>
public sealed record OutageEventModel(
    string EventId,
    string WindfarmName,
    string WtgName,
    string EventType,
    DateTime OriginalStartUtc,
    DateTime? OriginalEndUtc,
    string Status,
    string? SourceRevision,
    DateTime SourceWatermarkUtc,
    bool IsTombstone,
    DateTime LastUpdatedDate,
    string OutageId = "",
    string? Comment = null,
    string? AlarmCode = null,
    string? AlarmText = null,
    string? Category = null,
    string? SubCategory = null)
{
    // Source supplies RDSPP names; resolve control-plane IDs before writing Raw.
    public int WindfarmId { get; init; }
    public int WtgId { get; init; }
}
