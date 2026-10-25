namespace ProductionIngestor.Configuration;

/// <summary>Top-level options controlling ingestion behavior, bound from configuration.</summary>
public sealed class IngestionOptions
{
    public const string SectionName = "Ingestion";

    public TimeSpan MinimumIngestionInterval { get; set; } = TimeSpan.FromMinutes(2);
    public TimeSpan FailureRetryBackoff { get; set; } = TimeSpan.FromMinutes(5);
    public int MaxClaimsPerInvocation { get; set; } = 100;
    public TimeSpan MaxInvocationDuration { get; set; } = TimeSpan.FromMinutes(50);

    /// <summary>Duration of a lag re-read window behind the source's committed change stamp.</summary>
    public TimeSpan SafetyLagInterval { get; set; } = TimeSpan.Zero;

    /// <summary>Maximum size of a single LastUpdated catch-up window (default 1 month).</summary>
    public TimeSpan MaxCatchUpWindowSize { get; set; } = TimeSpan.FromDays(30);

    /// <summary>Lease duration granted on acquisition/renewal.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Maximum distinct dates per source window; excess fails without committing.</summary>
    public int MaxAffectedDatesPerWindow { get; set; } = 5000;

    public int MaxSnapshotRows { get; set; } = 1_000_000;

    /// <summary>Max rows held in memory per Parquet row group before flushing.</summary>
    public int ParquetRowGroupSize { get; set; } = 50_000;

    public int MaxRowsPerShard { get; set; } = 200_000;

    /// <summary>Max approximate bytes for a single day's artifact before it is sharded.</summary>
    public long MaxArtifactShardSizeBytes { get; set; } = 128L * 1024 * 1024;

    /// <summary>Number of Item-Days per downstream fan-out batch (informational for this pod).</summary>
    public int FanOutBatchPageSize { get; set; } = 50;

    /// <summary>Bounded per-source concurrent day-extraction workers for normal cadence.</summary>
    public int NormalCadenceConcurrencyLimit { get; set; } = 4;

    /// <summary>Bounded per-source concurrent day-extraction workers for historical refresh.</summary>
    public int BackfillConcurrencyLimit { get; set; } = 2;

    /// <summary>Expected row count per WTG-day for a 10-minute cadence source.</summary>
    public int ExpectedRowsPerWtgDay { get; set; } = 144;

    /// <summary>Max rows fetched per source query (TOP clause), mirroring the legacy integrator's batching.</summary>
    public int SourceQueryBatchSize { get; set; } = 200_000;

    /// <summary>
    /// Gates the versioned changed-row <c>ProductionChanges</c> incremental path (schemaVersion 2).
    /// Defaults to true: this is now the default scheduled-incremental path. Set to false to fall
    /// back to the legacy day-snapshot path (e.g. while Raw Fan-out/Harmonizer verification is
    /// still pending in a given environment).
    /// </summary>
    public bool ProductionChangesEnabled { get; set; } = true;

    /// <summary>Bounded in-process concurrency across monthly extraction windows within one run.</summary>
    public int MaxParallelWindows { get; set; } = 2;
}
