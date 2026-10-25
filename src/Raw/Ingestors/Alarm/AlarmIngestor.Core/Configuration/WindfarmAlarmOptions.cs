namespace AlarmIngestor.Configuration;

/// <summary>
/// The configured turbine/code selection for one windfarm — the same inputs the legacy
/// caller receives from CAT windfarm/other-loss-definition (EventTypeId = 2) configuration.
/// Never silently replaced by a hardcoded legacy static code list, and never defaulted to
/// "all codes" when empty (Section "Configuration and Source Contract"). Adding codes or
/// turbines requires an explicit Historical backfill — an existing ID cursor cannot recover
/// previously filtered records.
/// </summary>
public sealed class WindfarmAlarmOptions
{
    public required string WindfarmId { get; set; }

    /// <summary>Minimum retrieval date used only for a turbine's first-ever discovery pass (no saved cursor yet).</summary>
    public required DateTime MinRetrievalDateUtc { get; set; }

    public required List<string> AlarmCodes { get; set; }

    public required List<TurbineIdentity> Turbines { get; set; }
}
