namespace AlarmIngestor.Models;

/// <summary>
/// Retired: the discovery checkpoint is now a single global (per-windfarm, not per-turbine)
/// AlarmId carried on <see cref="ExtractionLeaseModel.LastCommittedId"/>. Kept only so an older
/// reference to this type fails to compile loudly rather than silently reintroducing per-turbine
/// checkpoint storage.
/// </summary>
[Obsolete("Per-turbine cursor storage was replaced by the shared global extraction_cursor checkpoint.", error: true)]
public sealed record TurbineCursorModel(string TurbineRdspp, long LastCommittedAlarmId);
