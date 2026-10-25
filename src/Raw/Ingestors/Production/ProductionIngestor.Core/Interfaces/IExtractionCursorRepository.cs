using ProductionIngestor.Models;

namespace ProductionIngestor.Interfaces;

/// <summary>Fenced access to the ExtractionCursor control-plane table.</summary>
public interface IExtractionCursorRepository
{
    /// <summary>Read-only snapshot of the committed cursor position; does not acquire a lease.</summary>
    Task<ExtractionLeaseModel?> GetCurrentAsync(int windfarmId, string sourceType, CancellationToken cancellationToken);

    /// <summary>
    /// Atomically acquires (or renews) a lease for the given windfarm/source.
    /// Returns null if a live lease is already held by another owner.
    /// </summary>
    Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(
        int windfarmId, string sourceType, string owner, TimeSpan leaseDuration, CancellationToken cancellationToken);

    /// <summary>Extends an already-held lease's expiry. Returns false if the token no longer matches (zombie).</summary>
    Task<bool> TryRenewLeaseAsync(
        int windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken);

    /// <summary>
    /// Fenced checkpoint: advances the committed position only if the given lease token
    /// still matches the row. Returns false ("zombie") when zero rows were affected. Does not
    /// touch the run's status; used for interim progress within a still-Running run.
    /// </summary>
    Task<bool> TryCommitAsync(
        int windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedUtc, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedUtc = null);

    /// <summary>Releases a held lease (best-effort; safe no-op if the token no longer matches).</summary>
    Task ReleaseLeaseAsync(int windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken);

    /// <summary>
    /// Creates the `extraction_run` audit row for a newly acquired lease (status Running),
    /// capturing the committed position at acquisition time as the run's `from_*` snapshot.
    /// Fenced on the cursor's current lease token/expiry; returns null if the lease is no longer
    /// held by this token (stale owner) so the caller never proceeds with an orphaned run row.
    /// </summary>
    Task<ExtractionRunModel?> StartRunAsync(
        int windfarmId, string sourceType, long leaseToken,
        DateTime fromUpdatedAtUtc, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null);

    /// <summary>
    /// Atomically, in one DB transaction: marks the run Completed with its final `to_*` snapshot
    /// AND fences/advances the cursor to that same position. Returns false ("zombie") if the
    /// lease token/owner no longer matches or the run is not still Running (stale owner cannot
    /// complete a run; terminal runs are immutable). <paramref name="queryStartInclusiveUtc"/>
    /// and <paramref name="queryEndExclusiveUtc"/> are the exact executed query bounds for the
    /// windowed ProductionChanges path; left null for the legacy day-snapshot path.
    /// </summary>
    Task<bool> TryCompleteRunAsync(
        long runId, int windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedUtc, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedUtc = null,
        DateTime? queryStartInclusiveUtc = null,
        DateTime? queryEndExclusiveUtc = null);

    /// <summary>
    /// Best-effort terminal audit write: marks the run Failed with whatever partial progress was
    /// actually checkpointed (via <see cref="TryCommitAsync"/>) before the failure, honestly
    /// reflecting real progress rather than the run's original `from_*` snapshot. Does not itself
    /// advance the cursor. A no-op (does not throw) if the run is no longer Running.
    /// <paramref name="queryStartInclusiveUtc"/>/<paramref name="queryEndExclusiveUtc"/> persist the
    /// exact executed query bounds for the windowed ProductionChanges path.
    /// </summary>
    Task FailRunAsync(
        long runId, int windfarmId, string sourceType, long leaseToken,
        DateTime? partialUpdatedAtUtc, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken,
        DateTime? queryStartInclusiveUtc = null,
        DateTime? queryEndExclusiveUtc = null);
}
