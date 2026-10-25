namespace AlarmIngestor.Contracts;

/// <summary>
/// External "raw-fanout" SQS message schema published after an immutable, append-only Alarm
/// Raw batch (or shard-set) has been durably written. There is no source watermark carried
/// here (the deployed API has none) — ExtractionToken plus WindowSequence are the only ordering
/// identity; downstream consumers must not treat this as a change-watermark contract.
/// </summary>
public sealed class RawAlarmBatchAvailableV1
{
    public required string SourceType { get; init; }

    public required string WindfarmId { get; init; }

    public required long ExtractionToken { get; init; }

    public required int WindowSequence { get; init; }

    public string? BlobPath { get; init; }

    public string[]? ShardPaths { get; init; }

    public required string RunId { get; init; }

    public string? TraceId { get; init; }

    public string? OperationId { get; init; }

    /// <summary>"Discovery" (new alarms), "Polling" (known-open resolution) or "Historical".</summary>
    public required string BatchKind { get; init; }
}
