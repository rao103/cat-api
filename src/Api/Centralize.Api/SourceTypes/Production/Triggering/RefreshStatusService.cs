using Centralize.Core.Interfaces;
using Centralize.Core.Models;
using ProductionIngestor.Models;

namespace Centralize.Api.SourceTypes.Production.Triggering;

public sealed record RefreshStatusResponse(
    string RunId, string? JobName, string Status,
    IReadOnlyList<RefreshAttempt> Attempts,
    bool EndToEndCompletionVerified = false);

public sealed class RefreshStatusService(IOperationRepository repository)
{
    public async Task<RefreshStatusResponse> GetAsync(ExtractionHistoryQueryModel query, CancellationToken cancellationToken)
    {
        var attempts = await repository.GetRefreshAttemptsAsync(query.RequestId, cancellationToken);
        var latest = attempts.FirstOrDefault();
        var status = latest?.Status == "Completed" ? "IngestionSucceeded" :
            latest?.Status == "Running" && latest.LeaseExpiresAt <= DateTimeOffset.UtcNow ? "LeaseExpired" :
            latest?.Status ?? "Unknown";
        return new(query.RequestId, latest?.JobName, status, attempts);
    }
}
