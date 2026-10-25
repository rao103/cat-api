namespace AlarmIngestor.Configuration;

/// <summary>
/// One selected turbine's source (RDSPP) and platform item identity, matching the same input
/// the legacy AlarmIntegrator receives from CAT windfarm/windturbine configuration.
/// </summary>
public sealed record TurbineIdentity(string TurbineRdspp, string ItemId);
