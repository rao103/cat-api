namespace OutageIngestor.Models;

/// <summary>
/// A requested Outage refresh: re-pull the authoritative current event set overlapping
/// [<see cref="ScopeStartUtc"/>, <see cref="ScopeEndUtc"/>) for a windfarm, regardless of cursor
/// position (Incremental Refresh Spec, Section 9). The scope is required — there is no
/// unscoped/full-table refresh mode.
/// </summary>
public sealed record RefreshRequestModel(
    int WindfarmId,
    string WindfarmName,
    string RequestedBy,
    string Reason,
    string RunId,
    DateTime ScopeStartUtc,
    DateTime ScopeEndUtc);
