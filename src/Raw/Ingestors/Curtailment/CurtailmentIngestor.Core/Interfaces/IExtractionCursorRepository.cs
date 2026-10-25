using CurtailmentIngestor.Models;

namespace CurtailmentIngestor.Interfaces;

/// <summary>Fenced access to this ingestor's own `curtailment_extraction_cursor` control-plane table.</summary>
public interface IExtractionCursorRepository
{
    /// <summary>Read-only snapshot of the committed cursor position; does not acquire a lease.</summary>
    Task<ExtractionLeaseModel?> GetCurrentAsync(string windfarmId, string sourceType, CancellationToken cancellationToken);

    /// <summary>
    /// Atomically acquires (or renews) a lease for the given windfarm/source.
    /// Returns null if a live lease is already held by another owner.
    /// </summary>
    Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(
        string windfarmId, string sourceType, string owner, TimeSpan leaseDuration, CancellationToken cancellationToken);

    /// <summary>Extends an already-held lease's expiry. Returns false if the token no longer matches (zombie).</summary>
    Task<bool> TryRenewLeaseAsync(
        string windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken);

    /// <summary>
    /// Fenced checkpoint: advances the committed position only if the given lease token still
    /// matches the row and the new position does not move the cursor backwards. Returns false
    /// ("zombie") when zero rows were affected. Does not touch the run's status; used for
    /// interim progress within a still-Running run.
    /// </summary>
    Task<bool> TryCommitAsync(
        string windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedDate, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedDate = null);

    /// <summary>Releases a held lease (best-effort; safe no-op if the token no longer matches).</summary>
    Task ReleaseLeaseAsync(string windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken);

    /// <summary>
    /// Creates the `extraction_run` audit row for a newly acquired lease (status Running),
    /// capturing the committed position at acquisition time as the run's `from_*` snapshot.
    /// </summary>
    Task<ExtractionRunModel> StartRunAsync(
        string windfarmId, string sourceType, long leaseToken,
        DateTime fromUpdatedAt, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null);

    /// <summary>
    /// Atomically, in one DB transaction: marks the run Completed with its final `to_*` snapshot
    /// AND fences/advances the cursor to that same position. Returns false ("zombie") if the
    /// lease token no longer matches or the run is not still Running (terminal runs are immutable).
    /// </summary>
    Task<bool> TryCompleteRunAsync(
        long runId, string windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedDate, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedDate = null);

    /// <summary>
    /// Best-effort terminal audit write: marks the run Failed with whatever partial progress was
    /// actually checkpointed (via <see cref="TryCommitAsync"/>) before the failure. Does not
    /// itself advance the cursor. A no-op (does not throw) if the run is no longer Running.
    /// </summary>
    Task FailRunAsync(
        long runId, string windfarmId, string sourceType, long leaseToken,
        DateTime? partialUpdatedAt, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken);
}
