namespace OutageIngestor.Contracts;

/// <summary>
/// External "raw-fanout" SQS message schema published after one immutable, append-only Outage
/// Raw artifact (or shard-set) has been durably written. Unlike Production, there is no per-day
/// DataDate — event batches are not Windfarm-Day shaped (Section 4.3.1). The live-event feed and
/// the deleted-event feed are two physically separate Raw artifacts with independent checkpoints
/// (Incremental Refresh Spec, Section 2.2); each artifact is announced by its own message rather
/// than one message describing both, so a fan-out consumer never needs to know about a second,
/// unrelated blob path to process this one.
/// </summary>
public sealed class RawOutageBatchAvailableV1
{
    public required string SourceType { get; init; }

    public required string WindfarmId { get; init; }

    public required long ExtractionToken { get; init; }

    /// <summary>
    /// Identifies which feed/shape this artifact carries: <c>OutageEvents</c> (incremental
    /// new/changed full event state), <c>OutageDeletedEvents</c> (incremental deletion/tombstone
    /// feed), or <c>OutageSnapshot</c> (authoritative Refresh snapshot). See Section 25.
    /// </summary>
    public required string PayloadKind { get; init; }

    /// <summary>Populated when the batch is a single Parquet file.</summary>
    public string? BlobPath { get; init; }

    /// <summary>Populated (instead of/alongside <see cref="BlobPath"/>) when the batch is sharded.</summary>
    public string[]? ShardPaths { get; init; }

    /// <summary>
    /// Populated only for <c>PayloadKind = OutageSnapshot</c>: the durable
    /// <c>snapshot/manifest.json</c> path listing every part written for this refresh. A
    /// consumer resolves the snapshot's actual Parquet parts from the manifest, not from
    /// <see cref="BlobPath"/>/<see cref="ShardPaths"/>, which stay unset for this payload kind.
    /// </summary>
    public string? ManifestPath { get; init; }

    public required string RunId { get; init; }

    public string? TraceId { get; init; }

    /// <summary>
    /// True only when this batch is the full authoritative current event set for the windfarm
    /// (Refresh mode, <see cref="PayloadKind"/> = <c>OutageSnapshot</c>). Downstream removal
    /// reconciliation must require this flag before treating any event's absence as an
    /// authoritative deletion; an Incremental batch (false) only ever carries explicit tombstones
    /// and never proves completeness on its own.
    /// </summary>
    public required bool IsFullSnapshot { get; init; }

    /// <summary>Half-open discovery window [WindowStartUtc, WindowEndUtc) this batch covers. For
    /// an incremental batch this is the catch-up window queried by LastUpdatedUtc; for a
    /// <c>OutageSnapshot</c> batch this is the requested refresh event-time scope instead.</summary>
    public DateTime? WindowStartUtc { get; init; }

    public DateTime? WindowEndUtc { get; init; }
}
