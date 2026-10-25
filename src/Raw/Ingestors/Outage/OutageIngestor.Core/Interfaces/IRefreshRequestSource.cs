using OutageIngestor.Models;

namespace OutageIngestor.Interfaces;

/// <summary>Abstraction the worker consumes refresh requests from (API call, internal queue, CLI — TBD).</summary>
public interface IRefreshRequestSource
{
    IAsyncEnumerable<RefreshRequestModel> GetPendingRequestsAsync(CancellationToken cancellationToken);
}
