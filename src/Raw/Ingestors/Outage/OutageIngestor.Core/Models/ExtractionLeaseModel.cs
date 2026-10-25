namespace OutageIngestor.Models;

/// <summary>
/// Committed cursor state plus the fencing token granted by a successful lease acquisition.
/// Backed by the shared `extraction_cursor` table (see src/db/migrations/0002_create_extraction_schema.sql
/// and 0006_extraction_cursor_deleted_feed_checkpoint.sql); this ingestor's tie-break identity is
/// the source `OutageClassificationID`, carried as TEXT.
/// <para>
/// Outage has two physically separate incremental source feeds whose LastUpdatedUtc progress is
/// not synchronized (the live `OutageClassification` table and the `OutageClassificationPeriodDeleted`
/// table), so each feed carries its own checkpoint pair (Incremental Refresh Spec, Section 2.2):
/// <see cref="LastUpdatedUtc"/>/<see cref="LastCommittedId"/> for the live/changed feed and
/// <see cref="DeletedLastUpdatedUtc"/>/<see cref="DeletedLastCommittedId"/> for the deletion feed.
/// </para>
/// </summary>
public sealed class ExtractionLeaseModel
{
    /// <summary>Internal PostgreSQL windfarm primary key; never the DART source name.</summary>
    public required int WindfarmId { get; init; }

    /// <summary>The windfarm's SiteRDSPPID (joined from the `windfarm` table); used to query the
    /// DART source system and as the Raw path's `windfarm=&lt;name&gt;` segment, mirroring
    /// ProductionIngestor's ExtractionLeaseModel.WindfarmName.</summary>
    public required string WindfarmName { get; init; }

    public required string SourceType { get; init; }

    /// <summary>The committed change-timestamp position of the live/changed feed at lease acquisition time.</summary>
    public required DateTime LastUpdatedUtc { get; init; }

    /// <summary>Committed keyset tie-break identity of the live/changed feed (source OutageClassificationID); "0" before the first commit.</summary>
    public required string LastCommittedId { get; init; }

    /// <summary>The committed change-timestamp position of the deletion feed at lease acquisition time.</summary>
    public required DateTime DeletedLastUpdatedUtc { get; init; }

    /// <summary>Committed keyset tie-break identity of the deletion feed (source OutageClassificationID); "0" before the first commit.</summary>
    public required string DeletedLastCommittedId { get; init; }

    /// <summary>Monotonic fencing token for this run; reused as the Raw path's `extraction=&lt;token&gt;` segment.</summary>
    public required long LeaseToken { get; init; }

    public required DateTime? LeaseExpiresAtUtc { get; init; }
}
