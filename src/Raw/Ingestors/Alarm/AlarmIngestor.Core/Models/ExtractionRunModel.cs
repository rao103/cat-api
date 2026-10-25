using AlarmIngestor.Enums;

namespace AlarmIngestor.Models;

/// <summary>
/// One `extraction_run` audit row: created Running at lease acquisition, and terminal
/// (Completed/Failed) exactly once thereafter — a terminal run is never updated again.
/// FromCommittedId/ToCommittedId are the shared global AlarmId checkpoint text, not per-turbine.
/// </summary>
public sealed record ExtractionRunModel(
    long RunId,
    string WindfarmId,
    string SourceType,
    ExtractionRunStatus Status,
    DateTime StartedAtUtc,
    DateTime? CompletedAtUtc,
    string FromCommittedId,
    string? ToCommittedId,
    string? OutputPath,
    string? ErrorMessage,
    string? OperationId = null);
