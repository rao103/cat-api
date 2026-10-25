using System.ComponentModel.DataAnnotations;

namespace Centralize.Api.SourceTypes.Production.Contracts;

public sealed record RefreshRequestDto
{
    [Required]
    public required string WindfarmId { get; init; }

    [Required]
    public required string WindfarmName { get; init; }

    public required DateOnly StartDate { get; init; }

    public required DateOnly EndDate { get; init; }

    [Required]
    public required string Reason { get; init; }
}