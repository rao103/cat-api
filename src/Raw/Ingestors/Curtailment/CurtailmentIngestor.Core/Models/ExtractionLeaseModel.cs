namespace CurtailmentIngestor.Models;

/// <summary>Committed cursor state plus the fencing token granted by a successful lease acquisition.</summary>
public sealed class ExtractionLeaseModel
{
    public required string WindfarmId { get; init; }

    public required string SourceType { get; init; }

    /// <summary>The committed change-timestamp position at lease acquisition time.</summary>
    public required DateTime LastUpdatedDate { get; init; }

    /// <summary>Committed tie-break identity as TEXT (the shared schema's `last_committed_id` is TEXT);
    /// Curtailment's own source id is numeric, so callers parse/compare it as a number, not lexically.</summary>
    public required string LastCommittedId { get; init; }

    /// <summary>Monotonic fencing token for this run; reused as the Raw path's `extraction=&lt;token&gt;` segment.</summary>
    public required long LeaseToken { get; init; }

    public required DateTime? LeaseExpiresAt { get; init; }
}
