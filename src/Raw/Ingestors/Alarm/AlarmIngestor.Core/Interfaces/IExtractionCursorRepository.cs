using AlarmIngestor.Models;

namespace AlarmIngestor.Interfaces;

/// <summary>
/// Control-plane persistence against the shared `extraction_cursor`/`extraction_run` schema
/// (Section "Global incremental AlarmId checkpoint per windfarm") plus this ingestor's own
/// `alarm_known_open_alarm` operational tracking table. There is exactly ONE committed AlarmId
/// checkpoint per windfarm — turbine identity never affects it. Known-open enrollment/removal is
/// fenced against the shared cursor's lease row but never itself advances the checkpoint id;
/// only <see cref="TryCompleteRunAsync"/> does that, once per run.
/// </summary>
public interface IExtractionCursorRepository
{
    /// <summary>Read-only snapshot of the committed cursor position; does not acquire a lease.</summary>
    Task<ExtractionLeaseModel?> GetCurrentAsync(string windfarmId, string sourceType, CancellationToken cancellationToken);

    Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(
        string windfarmId, string sourceType, string owner, TimeSpan leaseDuration, CancellationToken cancellationToken);

    Task<bool> TryRenewLeaseAsync(
        string windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken);

    Task ReleaseLeaseAsync(string windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken);

    /// <summary>
    /// Creates the `extraction_run` audit row for a newly acquired lease (status Running),
    /// capturing the committed global AlarmId at acquisition time as the run's from-snapshot.
    /// </summary>
    Task<ExtractionRunModel> StartRunAsync(
        string windfarmId, string sourceType, long leaseToken, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null);

    /// <summary>
    /// Atomically, in one DB transaction: marks the run Completed with its final committed
    /// AlarmId AND fences/advances the shared cursor to that same value. Returns false
    /// ("zombie") if the lease token no longer matches or the run is not still Running.
    /// </summary>
    Task<bool> TryCompleteRunAsync(
        long runId, string windfarmId, string sourceType, long leaseToken, string newCommittedId, CancellationToken cancellationToken);

    /// <summary>
    /// Best-effort terminal audit write: marks the run Failed with whatever position was
    /// actually committed (i.e. the run's original from-snapshot, since Alarm only ever advances
    /// the checkpoint once at the very end of a successful run). Never advances the cursor.
    /// A no-op if the run is no longer Running.
    /// </summary>
    Task FailRunAsync(
        long runId, string windfarmId, string sourceType, long leaseToken, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken);

    /// <summary>
    /// Enumerates known-open alarm IDs bounded by <paramref name="maxIdsPerTurbine"/> per
    /// turbine. Fails closed (throws) if any turbine has more open IDs than the cap rather than
    /// silently truncating and abandoning the excess IDs.
    /// </summary>
    Task<IReadOnlyDictionary<string, IReadOnlyList<long>>> GetKnownOpenAlarmsAsync(
        string windfarmId, IReadOnlyList<string> turbineRdspps, int maxIdsPerTurbine, CancellationToken cancellationToken);

    /// <summary>
    /// Fenced by <paramref name="leaseToken"/> against the shared cursor row (not the checkpoint
    /// value itself): enrolls newly discovered open alarms. Safe to call once per published
    /// discovery page, independent of the eventual global checkpoint advance (Section "existing
    /// per-page enrollment may happen fenced but no global advancement until complete").
    /// </summary>
    Task<bool> TryEnrollOpenAlarmsAsync(
        string windfarmId, long leaseToken, IReadOnlyList<(string TurbineRdspp, long AlarmId)> newOpenAlarms, CancellationToken cancellationToken);

    /// <summary>
    /// Fenced by <paramref name="leaseToken"/>: removes known-open entries whose closure or
    /// deletion has already been durably published. Ids not present in
    /// <paramref name="resolvedAlarmIds"/> remain tracked — omission never means closed.
    /// </summary>
    Task<bool> TryRemoveResolvedOpenAlarmsAsync(
        string windfarmId, long leaseToken, IReadOnlyList<(string TurbineRdspp, long AlarmId)> resolvedAlarmIds, CancellationToken cancellationToken);
}
