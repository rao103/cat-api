namespace ProductionIngestor.Contracts;

/// <summary>
/// External "raw-fanout" SQS message schema published after a Raw artifact
/// (or shard-set) for a single Windfarm-Day has been durably written.
/// </summary>
public sealed class RawArtifactAvailableV1
{
    public required string SourceType { get; init; }

    public required int WindfarmId { get; init; }

    public string WindfarmName { get; set; }

    /// <summary>UTC production date, e.g. "2026-06-03".</summary>
    public required string DataDate { get; init; }

    public required long ExtractionToken { get; init; }

    /// <summary>Populated when the day's artifact is a single Parquet file.</summary>
    public string? BlobPath { get; init; }

    /// <summary>Populated (instead of/alongside <see cref="BlobPath"/>) when the day is sharded.</summary>
    public string[]? ShardPaths { get; init; }

    public required string RunId { get; init; }

    public string? OperationId { get; init; }
    public string? TraceId { get; init; }
}
