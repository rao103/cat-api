namespace AlarmIngestor.Configuration;

/// <summary>
/// Top-level options controlling Alarm ingestion behavior, bound from configuration.
/// Independent of ProductionIngestor's/OutageIngestor's options.
/// </summary>
public sealed class IngestionOptions
{
    public const string SectionName = "Ingestion";

    /// <summary>Lease duration granted on acquisition/renewal for Windfarm + Alarm.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Max rows requested per discovery/poll page (legacy default cap was a single 3000-row page).</summary>
    public int SourceQueryPageSize { get; set; } = 3000;

    /// <summary>Bounded per-turbine batch size for known-open alarm-ID polling.</summary>
    public int PollBatchSize { get; set; } = 500;

    /// <summary>Timeout applied to a single HTTP search request.</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Max events held in memory per Raw batch shard before splitting.</summary>
    public int MaxEventsPerShard { get; set; } = 50_000;

    /// <summary>
    /// Fail-closed cap on known-open alarm IDs enumerated per turbine per run. The database
    /// query itself is bounded by this value; a turbine with more open IDs than the cap fails
    /// the run rather than silently abandoning the excess IDs (Section "Poll open-ID enumeration").
    /// </summary>
    public int MaxOpenAlarmsPerTurbine { get; set; } = 20_000;

    /// <summary>Per-windfarm turbine/code selection. Keyed by WindfarmId at lookup time.</summary>
    public List<WindfarmAlarmOptions> Windfarms { get; set; } = [];
}
