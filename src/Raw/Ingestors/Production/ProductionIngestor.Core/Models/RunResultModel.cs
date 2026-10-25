using ProductionIngestor.Enums;
using ProductionIngestor.Operations.Execution;

namespace ProductionIngestor.Models;

/// <summary>Outcome of one bounded <see cref="ProductionIngestionRunner"/> pass.</summary>
public sealed record RunResultModel(
    RunOutcome Outcome,
    IReadOnlyList<DateOnly> DaysWritten,
    string? Error = null,
    DateTime? QueryStartInclusiveUtc = null,
    DateTime? QueryEndExclusiveUtc = null,
    DateTime? CompletedCheckpointUpdatedUtc = null,
    string? CompletedCheckpointId = null);
