namespace AlarmIngestor.Models;

/// <summary>
/// A single source alarm/warning record carrying full source event facts (Section "Persist
/// full source records"). AlarmId is the source's stable numeric identity used for keyset
/// discovery paging and known-open tracking — there is no fabricated surrogate. EndTimeUtc is
/// preserved exactly as returned (raw ends are never modified, no duration cap is applied —
/// that is a downstream calculation concern per the legacy AlarmAdaptor, not a Raw transformation).
/// ObservedAtUtc/SequenceInRun represent explicit extraction observation ordering — this is not
/// a source revision or a change watermark; the deployed API rejects LastUpdatedUtc.
/// </summary>
public sealed record AlarmEventModel(
    string WindfarmId,
    string TurbineRdspp,
    string ItemId,
    long AlarmId,
    string AlarmCode,
    string? AlarmText,
    DateTime StartTimeUtc,
    DateTime? EndTimeUtc,
    string AlarmType,
    string AlarmState,
    bool IsTombstone,
    DateTime ObservedAtUtc,
    int SequenceInRun);
