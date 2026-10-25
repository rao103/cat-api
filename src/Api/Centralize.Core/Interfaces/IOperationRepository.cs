using Centralize.Core.Models;

namespace Centralize.Core.Interfaces;

public interface IOperationRepository
{
    Task<IReadOnlyList<RefreshSourceModel>> GetRefreshSourcesAsync(OperationRequestModel request, CancellationToken cancellationToken);
    Task<OperationModel> CreateAsync(string requestedBy, CancellationToken cancellationToken);
    Task<OperationModel?> GetAsync(Guid operationId, string requestedBy, CancellationToken cancellationToken);
    Task<IReadOnlyList<RefreshAttempt>> GetRefreshAttemptsAsync(string requestId, CancellationToken cancellationToken);
}
