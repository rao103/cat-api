namespace OutageIngestor.Models;

/// <summary>An event rejected at write time, with an auditable reason (never silently dropped).</summary>
public sealed record QuarantinedEventModel(string EventId, string Reason);

/// <summary>
/// Result of writing one extraction run's Raw batch. The live-event and deleted-event source
/// feeds are written as separate physical artifacts (see <c>RawPathBuilder</c>): the
/// <c>BlobPath</c>/<c>ShardPaths</c>/<c>RowCount</c> triple describes the live "events" artifact,
/// the <c>Deleted*</c> triple describes the "deleted-events" artifact. Either triple is empty
/// (null paths, zero count) when that feed had no valid rows in this batch.
/// </summary>
public sealed record RawWriteResultModel(
    string WindfarmName,
    long ExtractionToken,
    string? BlobPath,
    IReadOnlyList<string>? ShardPaths,
    int RowCount,
    IReadOnlyList<QuarantinedEventModel> QuarantinedEvents,
    string? DeletedBlobPath = null,
    IReadOnlyList<string>? DeletedShardPaths = null,
    int DeletedRowCount = 0,
    IReadOnlyList<int>? PartRowCounts = null,
    IReadOnlyList<int>? DeletedPartRowCounts = null)
{
    /// <summary>Total rows durably written across both feeds; used for run/telemetry accounting.</summary>
    public int TotalRowCount => RowCount + DeletedRowCount;
}
