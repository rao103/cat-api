using CAT.RawFanOut.Models;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.Interfaces;
public interface IArtifactDispatchStore
{
    Task<IReadOnlyList<OutgoingMessage>?> LoadPlanAsync(RawEnvelope artifact, CancellationToken cancellationToken);
    Task<IReadOnlyList<OutgoingMessage>> SavePlanAsync(RawEnvelope artifact,IReadOnlyList<OutgoingMessage> proposed,CancellationToken ct);
    Task MarkDispatchedAsync(string artifactId,CancellationToken ct);
}
