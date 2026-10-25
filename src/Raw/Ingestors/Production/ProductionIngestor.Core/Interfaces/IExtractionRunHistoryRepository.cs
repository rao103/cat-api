using ProductionIngestor.Models;

namespace ProductionIngestor.Interfaces;

public interface IExtractionRunHistoryRepository
{
    Task<IReadOnlyList<ExtractionRunHistoryModel>> GetAttemptsAsync(ExtractionHistoryQueryModel query, CancellationToken cancellationToken);
}