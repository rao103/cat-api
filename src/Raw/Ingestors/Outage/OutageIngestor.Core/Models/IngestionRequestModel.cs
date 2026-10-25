using OutageIngestor.Enums;
using OutageIngestor.Operations.Execution;

namespace OutageIngestor.Models;

/// <summary>
/// One bounded unit of work for <see cref="OutageIngestionRunner"/>: either "discover and
/// catch up this windfarm/source" (Incremental) or "re-pull the full authoritative current
/// event set for this windfarm" (Refresh). Unlike Production, Refresh has no per-day
/// granularity — events are not Windfarm-Day shaped.
/// </summary>
public sealed record IngestionRequestModel(
    int WindfarmId,
    string WindfarmName,
    string SourceType,
    IngestionMode Mode,
    string? RunId = null,
    DateTime? RefreshScopeStartUtc = null,
    DateTime? RefreshScopeEndUtc = null)
{
    public string? OperationId { get; init; }
}
