using OutageIngestor.Enums;

namespace OutageIngestor.Models;

/// <summary>One `extraction_run` audit row: the from/to cursor snapshot bracketing a bounded run.</summary>
public sealed record ExtractionRunModel(
    long RunId,
    int WindfarmId,
    string SourceType,
    ExtractionRunStatus Status,
    DateTime StartedAtUtc,
    DateTime? CompletedAtUtc,
    DateTime FromUpdatedAtUtc,
    string FromCommittedId,
    DateTime? ToUpdatedAtUtc,
    string? ToCommittedId,
    string? OutputPath,
    string? ErrorMessage,
    string? OperationId);
