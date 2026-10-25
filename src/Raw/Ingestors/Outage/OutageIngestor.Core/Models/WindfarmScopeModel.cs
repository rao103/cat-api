namespace OutageIngestor.Models;

/// <summary>
/// Immutable planning scope for one windfarm execution: internal integer identity and separate
/// DART name (used to query the source and build Raw paths), minimum retrievable event
/// date, and the `wtg_name` of every turbine currently eligible (`is_active = true` and
/// `turbine_toc` already in the past). Loaded once per run and shared across every window.
/// </summary>
public sealed record WindfarmScopeModel(
    int WindfarmId,
    string WindfarmName,
    DateTime MinRetrieveDateUtc,
    IReadOnlyList<string> ActiveTurbineNames)
{
    public IReadOnlyList<CAT.Messaging.Contracts.TurbineIdentity> ActiveTurbines { get; init; } = [];
}
