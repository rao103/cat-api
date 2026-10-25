namespace CurtailmentIngestor.Contracts;

/// <summary>
/// External "raw-fanout" SQS message schema published after an immutable, append-only Curtailment
/// Raw batch (or shard-set) has been durably written. Like Outage, there is no per-day
/// DataDate — event batches are not Windfarm-Day shaped (Section 4.3.1).
/// </summary>
public sealed class RawCurtailmentBatchAvailableV1
{
    public required string SourceType { get; init; }

    public required string WindfarmId { get; init; }

    public required long ExtractionToken { get; init; }

    /// <summary>Populated when the batch is a single Parquet file.</summary>
    public string? BlobPath { get; init; }

    /// <summary>Populated (instead of/alongside <see cref="BlobPath"/>) when the batch is sharded.</summary>
    public string[]? ShardPaths { get; init; }

    public required string RunId { get; init; }

    public string? TraceId { get; init; }

    public string? OperationId { get; init; }
}
