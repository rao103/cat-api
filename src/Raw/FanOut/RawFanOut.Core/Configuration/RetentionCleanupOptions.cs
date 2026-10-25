using System;
namespace CAT.RawFanOut.Configuration;

public sealed class RetentionCleanupOptions
{
    // Measured from harmonization_completed_at_utc; applies to completed artifact ledgers only.
    public int RetentionMonths { get; set; } = 6;
    public int MaxParentsPerBatch { get; set; } = 500;
    // Measured from resolved_at_utc; unresolved incidents are never deleted. The 6-month default mirrors the
    // artifact rule and is an assumption to confirm.
    public int ResolvedIncidentRetentionMonths { get; set; } = 6;

    public void Validate()
    {
        if (RetentionMonths < 1 || RetentionMonths > 120)
            throw new ArgumentException("RetentionMonths must be a small positive number of calendar months.");
        if (ResolvedIncidentRetentionMonths < 1 || ResolvedIncidentRetentionMonths > 120)
            throw new ArgumentException("ResolvedIncidentRetentionMonths must be a small positive number of calendar months.");
        if (MaxParentsPerBatch < 1 || MaxParentsPerBatch > 10_000)
            throw new ArgumentException("MaxParentsPerBatch must be a bounded positive batch size.");
    }
}
