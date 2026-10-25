using System.Text.Json.Serialization;

namespace ProductionIngestor.Models;

/// <summary>Committed cursor state plus the fencing token granted by a successful lease acquisition.</summary>
public sealed class ExtractionLeaseModel
{
    public required int WindfarmId { get; init; }

    /// <summary>The windfarm's display name (joined from the `windfarm` table); used as the Raw path's `windfarm=&lt;name&gt;` segment.</summary>
    public required string WindfarmName { get; init; }

    public required string SourceType { get; init; }

    /// <summary>The committed change-timestamp position at lease acquisition time.</summary>
    [JsonPropertyName("lastUpdatedDate")]
    public required DateTime LastUpdatedUtc { get; init; }

    /// <summary>Committed tie-break identity as TEXT (numeric ids are rendered as text for this source).</summary>
    public required string LastCommittedId { get; init; }

    /// <summary>Monotonic fencing token for this run; reused as the Raw path's `extraction=&lt;token&gt;` segment.</summary>
    public required long LeaseToken { get; init; }

    [JsonPropertyName("leaseExpiresAt")]
    public required DateTime? LeaseExpiresAtUtc { get; init; }
}
