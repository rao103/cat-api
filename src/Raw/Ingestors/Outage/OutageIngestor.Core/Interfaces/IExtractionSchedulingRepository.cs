using OutageIngestor.Models;

namespace OutageIngestor.Interfaces;

/// <summary>
/// Atomic due-cursor claiming plus its `extraction_run` audit history, layered on top of
/// <see cref="IExtractionCursorRepository"/>'s per-lease primitives. Mirrors
/// ProductionIngestor's scheduling repository so one Kubernetes invocation can claim and
/// process every due, enabled Outage cursor in sequence (architecture v12, Section 4.2).
/// </summary>
public interface IExtractionSchedulingRepository
{
    Task<bool> IsRegisteredAsync(ExtractionLookupModel lookup, CancellationToken cancellationToken);
    Task<ExtractionClaimModel?> TryClaimCursorAndStartExtractionRunAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken);
    Task<ExtractionSelectionStatusModel> GetSelectionStatusAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken);
    Task<bool> TryFinalizeAsync(ExtractionAttemptResultModel attempt, CancellationToken cancellationToken);
}
