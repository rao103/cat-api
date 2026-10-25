using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Production.Models;
namespace Harmonizer.Core.SourceTypes.Production.Interfaces;

public interface IProductionControlStateRepository
{
    Task RegisterBatchAsync(ProductionWork work, CancellationToken cancellationToken);
    Task<bool> IsItemAppliedAsync(BatchIdentity batch, int wtgId, CancellationToken cancellationToken);
    Task<ComponentLease> ClaimAsync(WtgDayKey key, BatchIdentity batch, Guid ownerId, CancellationToken cancellationToken);
    Task RenewAsync(ComponentLease lease, CancellationToken cancellationToken);
    Task CommitAsync(ComponentCommit commit, CancellationToken cancellationToken);
    Task FailAsync(ComponentLease lease, ProductionWork work, string errorCode, CancellationToken cancellationToken);
}
