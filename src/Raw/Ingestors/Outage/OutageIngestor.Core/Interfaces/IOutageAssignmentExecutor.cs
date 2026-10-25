using OutageIngestor.Models;

namespace OutageIngestor.Interfaces;

/// <summary>Executes one already-claimed extraction attempt. Implemented in the Worker so each
/// attempt runs inside its own DI scope (mirrors ProductionIngestor's IProductionAssignmentExecutor).</summary>
public interface IOutageAssignmentExecutor
{
    Task<RunResultModel> ExecuteAsync(ExtractionClaimModel claim, CancellationToken cancellationToken);
}
