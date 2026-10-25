using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Production.Models;
namespace Harmonizer.Core.SourceTypes.Production.Interfaces;

public interface IProductionArtifactStore
{
    Task<IReadOnlyList<ProductionRow>> ReadRawAsync(ProductionWork work, CancellationToken cancellationToken);
    Task<ProductionSnapshot> ReadSnapshotAsync(string path, CancellationToken cancellationToken);
    Task<ComponentCandidate> WriteCandidateAsync(ComponentLease lease, ProductionWork work,
        MergeResult result, CancellationToken cancellationToken);
}
