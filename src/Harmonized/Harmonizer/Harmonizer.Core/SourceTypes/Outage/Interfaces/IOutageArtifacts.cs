using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Interfaces;

public interface IOutageArtifacts
{
    Task<IReadOnlyList<CanonicalOutage>> ReadRawAsync(OutageWork work, CancellationToken cancellationToken);
    Task<OutageCatalog> ReadCatalogAsync(string? path, CancellationToken cancellationToken);
    Task<EventMonthSnapshot> ReadMonthAsync(EventMonthReference reference, CancellationToken cancellationToken);
    Task<OutageLifecyclePlan> ReadPlanAsync(string path, CancellationToken cancellationToken);
    Task<EventMonthReference> WriteMonthAsync(OutageCatalogLease lease, string partitionKey,
        long revision, IReadOnlyList<CanonicalOutage> events, CancellationToken cancellationToken);
    Task<string> WriteCatalogAsync(OutageCatalogLease lease, OutageCatalog catalog, CancellationToken cancellationToken);
    Task<string> WritePlanAsync(OutageCatalogLease lease, OutageLifecyclePlan plan, CancellationToken cancellationToken);
    Task<OutageDailyCandidate> WriteDayAsync(OutageCatalogLease lease, OutageLifecyclePlan plan,
        OutageRange range, DateOnly date, long previousRevision, IReadOnlyList<OutageDayReference> rows, OutageDayEvidence? trigger,
        CancellationToken cancellationToken);
}
