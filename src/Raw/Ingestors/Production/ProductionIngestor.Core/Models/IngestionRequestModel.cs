using ProductionIngestor.Enums;
using ProductionIngestor.Operations.Execution;

namespace ProductionIngestor.Models;

/// <summary>
/// One bounded unit of work for <see cref="ProductionIngestionRunner"/>: either "discover and
/// catch up this windfarm/source" (Incremental) or "re-extract these specific dates" (Refresh).
/// </summary>
public sealed record IngestionRequestModel(
    int WindfarmId,
    string WindfarmName,
    string SourceType,
    IngestionMode Mode,
    IReadOnlyList<DateOnly>? ExplicitDates = null,
    string? RunId = null)
{
    public string? OperationId { get; init; }
}
