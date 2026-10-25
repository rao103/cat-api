using System;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Models;
namespace CAT.RawFanOut.Interfaces;

public interface IArtifactRetentionCleanupStore
{
    // Deletes at most maxParentsPerBatch eligible artifacts (and their children) in one bounded
    // transaction. Returns (0,0) when nothing is currently eligible.
    Task<RetentionCleanupBatchResult> DeleteEligibleBatchAsync(
        DateTimeOffset cutoffUtc, int maxParentsPerBatch, CancellationToken cancellationToken);
}
