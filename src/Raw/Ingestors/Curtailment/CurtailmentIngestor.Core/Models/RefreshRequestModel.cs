namespace CurtailmentIngestor.Models;

/// <summary>
/// A requested full Curtailment refresh: re-pull the authoritative current event set for a
/// windfarm, regardless of cursor position.
/// </summary>
public sealed record RefreshRequestModel(
    string WindfarmId,
    string RequestedBy,
    string Reason,
    string RunId);
