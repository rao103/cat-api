using System;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.Interfaces;

public interface IDeadLetterRetentionStore
{
    // Deletes at most maxIncidents RESOLVED incidents whose resolution is older than the cutoff.
    // Unresolved incidents are never eligible, however old. Returns 0 when nothing is eligible.
    Task<int> DeleteResolvedBatchAsync(DateTimeOffset cutoffUtc, int maxIncidents, CancellationToken cancellationToken);
}
