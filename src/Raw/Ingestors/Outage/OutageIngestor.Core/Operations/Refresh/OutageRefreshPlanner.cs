using OutageIngestor.Enums;
using OutageIngestor.Models;
using OutageIngestor.Operations.Execution;

namespace OutageIngestor.Operations.Refresh;

/// <summary>
/// Planning only: turns a requested full Outage refresh into an <see cref="IngestionRequestModel"/>
/// for <see cref="OutageIngestionRunner"/> in <see cref="IngestionMode.Refresh"/>.
/// </summary>
public sealed class OutageRefreshPlanner
{
    public IngestionRequestModel Plan(RefreshRequestModel request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(request.WindfarmId);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WindfarmName);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RunId);
        if (request.ScopeEndUtc <= request.ScopeStartUtc)
            throw new ArgumentOutOfRangeException(nameof(request), "Refresh scope end must be strictly after scope start.");

        return new IngestionRequestModel(request.WindfarmId, request.WindfarmName, SourceType: "Outage", IngestionMode.Refresh, request.RunId,
            request.ScopeStartUtc, request.ScopeEndUtc);
    }
}
