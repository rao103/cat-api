using ProductionIngestor.Models;

namespace ProductionIngestor.Interfaces;

public interface IProductionAssignmentExecutor
{
    Task<RunResultModel> ExecuteAsync(ExtractionClaimModel claim, CancellationToken cancellationToken);
}