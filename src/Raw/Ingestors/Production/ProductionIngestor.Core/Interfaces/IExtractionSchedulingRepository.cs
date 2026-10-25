using ProductionIngestor.Models;

namespace ProductionIngestor.Interfaces;

public interface IExtractionSchedulingRepository
{
    Task<bool> IsRegisteredAsync(ExtractionLookupModel lookup, CancellationToken cancellationToken);
    Task<ExtractionClaimModel?> TryClaimCursorAndStartExtractionRunAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken);
    Task<ExtractionSelectionStatusModel> GetSelectionStatusAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken);
    Task<bool> TryFinalizeAsync(ExtractionAttemptResultModel attempt, CancellationToken cancellationToken);
}