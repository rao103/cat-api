using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Models;
using CurtailmentIngestor.Operations.Execution;

namespace CurtailmentIngestor.Operations.Refresh;

/// <summary>
/// Planning only: turns a requested full Curtailment refresh into an <see cref="IngestionRequestModel"/>
/// for <see cref="CurtailmentIngestionRunner"/> in <see cref="IngestionMode.Refresh"/>.
/// </summary>
public sealed class CurtailmentRefreshPlanner
{
    public IngestionRequestModel Plan(RefreshRequestModel request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WindfarmId);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RunId);

        return new IngestionRequestModel(request.WindfarmId, SourceType: "Curtailment", IngestionMode.Refresh, request.RunId);
    }
}
