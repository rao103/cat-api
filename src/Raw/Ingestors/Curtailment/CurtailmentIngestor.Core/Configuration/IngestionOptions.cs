namespace CurtailmentIngestor.Configuration;

/// <summary>
/// Top-level options controlling Curtailment ingestion behavior, bound from configuration.
/// Independent of the other ingestors' options — each ingestor owns its own configuration shape.
/// </summary>
public sealed class IngestionOptions
{
    public const string SectionName = "Ingestion";

    /// <summary>Duration of a lag re-read window behind the source's committed change stamp.</summary>
    public TimeSpan SafetyLagInterval { get; set; } = TimeSpan.Zero;

    /// <summary>Maximum size of a single LastUpdated catch-up window (default 1 month).</summary>
    public TimeSpan MaxCatchUpWindowSize { get; set; } = TimeSpan.FromDays(30);

    /// <summary>Lease duration granted on acquisition/renewal.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Max events held in memory per Raw batch shard before splitting.</summary>
    public int MaxEventsPerShard { get; set; } = 50_000;

    /// <summary>Maximum changed events discovered per source window; excess fails without committing.</summary>
    public int MaxEventsPerWindow { get; set; } = 200_000;

    /// <summary>Maximum events buffered for one full-refresh authoritative pull.</summary>
    public int MaxSnapshotEvents { get; set; } = 1_000_000;

    /// <summary>Max rows held in memory per Parquet row group before flushing.</summary>
    public int ParquetRowGroupSize { get; set; } = 50_000;

    /// <summary>Bounded per-source concurrent windfarm workers for normal cadence.</summary>
    public int NormalCadenceConcurrencyLimit { get; set; } = 4;

    /// <summary>Bounded per-source concurrent windfarm workers for historical refresh.</summary>
    public int BackfillConcurrencyLimit { get; set; } = 2;
}
