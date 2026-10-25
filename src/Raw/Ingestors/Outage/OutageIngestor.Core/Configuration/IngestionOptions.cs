namespace OutageIngestor.Configuration;

/// <summary>
/// Top-level options controlling Outage ingestion behavior, bound from configuration.
/// Independent of ProductionIngestor's options — each ingestor owns its own configuration shape.
/// </summary>
public sealed class IngestionOptions
{
    public const string SectionName = "Ingestion";

    /// <summary>Minimum time between the end of one completed incremental attempt and the next claim.</summary>
    public TimeSpan MinimumIngestionInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Delay before a failed cursor becomes claimable again.</summary>
    public TimeSpan FailureRetryBackoff { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Bounds one invocation's due-cursor claim loop so it cannot run forever.</summary>
    public int MaxClaimsPerInvocation { get; set; } = 100;

    /// <summary>Wall-clock budget for one scheduled invocation across all claimed attempts.</summary>
    public TimeSpan MaxInvocationDuration { get; set; } = TimeSpan.FromMinutes(50);

    /// <summary>Duration of a lag re-read window behind the source's committed change stamp.</summary>
    public TimeSpan SafetyLagInterval { get; set; } = TimeSpan.Zero;

    /// <summary>Maximum size of a single LastUpdated catch-up window (default 1 month).</summary>
    public TimeSpan MaxCatchUpWindowSize { get; set; } = TimeSpan.FromDays(30);

    /// <summary>Lease duration granted on acquisition/renewal.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Max events held in memory per Raw batch shard before splitting.</summary>
    public int MaxEventsPerShard { get; set; } = 50_000;

    /// <summary>Bounded per-source concurrent windfarm workers for normal cadence.</summary>
    public int NormalCadenceConcurrencyLimit { get; set; } = 4;

    /// <summary>Bounded per-source concurrent windfarm workers for historical refresh.</summary>
    public int BackfillConcurrencyLimit { get; set; } = 2;

    /// <summary>Max rows requested per SQL page (TOP N); bounds a single round-trip, not the total window/snapshot.</summary>
    public int SourceQueryBatchSize { get; set; } = 10_000;
}
