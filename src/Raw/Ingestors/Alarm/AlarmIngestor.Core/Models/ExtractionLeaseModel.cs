namespace AlarmIngestor.Models;

/// <summary>
/// Fencing token granted by a successful Windfarm + Alarm lease acquisition, plus the shared,
/// global (not per-turbine) committed AlarmId checkpoint at acquisition time. LastCommittedId is
/// the highest AlarmId already durably checkpointed across ALL configured turbines for this
/// windfarm (default "0"); turbine identity does not affect this value (Section "global
/// incremental AlarmId checkpoint per windfarm").
/// </summary>
public sealed record ExtractionLeaseModel(
    string WindfarmId,
    string SourceType,
    long LeaseToken,
    string LastCommittedId,
    DateTime? LeaseExpiresAtUtc);
