using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Operations.Execution;

namespace OutageIngestor.Worker.Operations;

public sealed class ScopedOutageAssignmentExecutor(IServiceScopeFactory scopes) : IOutageAssignmentExecutor
{
    public async Task<RunResultModel> ExecuteAsync(ExtractionClaimModel claim, CancellationToken cancellationToken)
    {
        await using var scope = scopes.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<OutageIngestionRunner>().RunClaimedAsync(claim, cancellationToken);
    }
}
