using OutageIngestor.Enums;
using OutageIngestor.Operations.Execution;

namespace OutageIngestor.Models;

/// <summary>Outcome of one bounded <see cref="OutageIngestionRunner"/> pass. The checkpoint fields
/// are populated only for a claimed (scheduled/refresh) attempt, whose finalization is owned by
/// <see cref="Interfaces.IExtractionSchedulingRepository"/> rather than by the runner itself.
/// The live/changed and deletion feeds carry independent checkpoints (Section 2.2) that must be
/// committed together.</summary>
public sealed record RunResultModel(
    RunOutcome Outcome,
    int EventsWritten,
    string? Error = null,
    DateTime? CompletedCheckpointUpdatedUtc = null,
    string? CompletedCheckpointId = null,
    DateTime? CompletedDeletedCheckpointUpdatedUtc = null,
    string? CompletedDeletedCheckpointId = null);
