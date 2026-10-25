using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Execution;

namespace ProductionIngestor.Worker.Operations;

public sealed class ScopedProductionAssignmentExecutor(IServiceScopeFactory scopes) : IProductionAssignmentExecutor
{
    public async Task<RunResultModel> ExecuteAsync(ExtractionClaimModel claim, CancellationToken cancellationToken)
    {
        await using var scope = scopes.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<ProductionIngestionRunner>().RunClaimedAsync(claim, cancellationToken);
    }
}