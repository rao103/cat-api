namespace ProductionIngestor.Models;

/// <summary>
/// Immutable planning scope for one windfarm execution: PostgreSQL identity/name, minimum
/// retrievable production date, and the `wtg_name` of every turbine currently eligible
/// (`is_active = true` and `turbine_toc` already in the past). Loaded once per run and shared
/// across every window; never reloaded per batch.
/// </summary>
public sealed record WindfarmScopeModel(
    int WindfarmId,
    string WindfarmName,
    DateTime MinRetrieveDateUtc,
    IReadOnlyList<string> ActiveTurbineNames);
