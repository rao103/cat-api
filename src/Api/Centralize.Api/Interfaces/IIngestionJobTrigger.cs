using Centralize.Core.Models;

namespace Centralize.Api.Interfaces;

public interface IIngestionJobTrigger
{
    void ValidateSources(IReadOnlyList<RefreshSourceModel> sources);
    Task<string> TriggerAsync(OperationRequestModel request, RefreshSourceModel source,
        Guid operationId, string runId, CancellationToken cancellationToken);
}
