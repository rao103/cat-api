using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Operations.Execution;

namespace CurtailmentIngestor.Models;

/// <summary>
/// One bounded unit of work for <see cref="CurtailmentIngestionRunner"/>: either "discover and
/// catch up this windfarm/source" (Incremental) or "re-pull the full authoritative current
/// event set for this windfarm" (Refresh). Events are not Windfarm-Day shaped, so Refresh has
/// no per-day granularity.
/// </summary>
public sealed record IngestionRequestModel(
    string WindfarmId,
    string SourceType,
    IngestionMode Mode,
    string? RunId = null)
{
    public string? OperationId { get; init; }
}
