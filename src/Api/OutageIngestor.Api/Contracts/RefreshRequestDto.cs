using System.ComponentModel.DataAnnotations;

namespace OutageIngestor.Api.Contracts;

/// <summary>API input contract for an Outage refresh. Unlike the earlier full-snapshot design,
/// refresh always requires an explicit half-open event-time scope
/// [<see cref="StartDateUtc"/>, <see cref="EndDateUtc"/>) to re-pull, matching
/// ProductionIngestor's dated-refresh pattern.</summary>
public sealed record RefreshRequestDto
{
    [Required]
    public required string WindfarmId { get; init; }

    [Required]
    public required string WindfarmName { get; init; }

    [Required]
    public required string Reason { get; init; }

    [Required]
    public required DateTime StartDateUtc { get; init; }

    [Required]
    public required DateTime EndDateUtc { get; init; }
}
