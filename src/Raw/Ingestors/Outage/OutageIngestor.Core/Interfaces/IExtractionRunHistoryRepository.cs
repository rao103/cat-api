using OutageIngestor.Models;

namespace OutageIngestor.Interfaces;

public interface IExtractionRunHistoryRepository
{
    Task<IReadOnlyList<ExtractionRunHistoryModel>> GetAttemptsAsync(ExtractionHistoryQueryModel query, CancellationToken cancellationToken);
}
