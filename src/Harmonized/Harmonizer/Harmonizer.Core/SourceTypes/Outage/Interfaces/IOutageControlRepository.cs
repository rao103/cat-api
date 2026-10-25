using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Interfaces;

/// <summary>Atomic publication and progress boundaries; provider transactions stay in the adapter.</summary>
public interface IOutageControlRepository
{
    Task<bool> RegisterAsync(OutageWork work, CancellationToken cancellationToken);
    Task RecordSourceFailureAsync(OutageWork work, string errorCode, CancellationToken cancellationToken);
    Task RecordObservationAsync(OutageWork work, CancellationToken cancellationToken);
    Task<bool> HasCanonicalReceiptAsync(BatchIdentity batch, CancellationToken cancellationToken);
    Task<bool> HasExpectedReceiptsAsync(OutageWork work, CancellationToken cancellationToken);
    Task<OutageCatalogLease> ClaimAsync(WindfarmIdentity farm, CancellationToken cancellationToken);
    Task RenewAsync(OutageCatalogLease lease, CancellationToken cancellationToken);
    Task ReleaseAsync(OutageCatalogLease lease, CancellationToken cancellationToken);
    Task<IReadOnlyList<WindfarmIdentity>> GetDueFarmsAsync(int limit, CancellationToken cancellationToken);
    Task<IReadOnlyList<OutageLifecycleState>> ReadLifecycleAsync(int farmId, CancellationToken cancellationToken);
    Task PublishAsync(OutageCatalogLease lease, OutageWork work, OutagePublication publication, CancellationToken cancellationToken);
    Task<long> ReadDayRevisionAsync(WtgDayKey key, CancellationToken cancellationToken);
    Task AdvanceAsync(OutageCatalogLease lease, OutageLifecycleUpdate update, OutageDailyCandidate? candidate,
        CancellationToken cancellationToken);
    Task RecordFailureAsync(OutageCatalogLease lease, string errorCode, CancellationToken cancellationToken);
    Task CompleteReadyBatchesAsync(int farmId, CancellationToken cancellationToken);
    Task<bool> CompleteBatchAsync(BatchIdentity batch, CancellationToken cancellationToken);
}
