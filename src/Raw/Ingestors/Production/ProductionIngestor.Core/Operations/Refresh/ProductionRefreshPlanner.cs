using ProductionIngestor.Models;
using ProductionIngestor.Enums;

namespace ProductionIngestor.Operations.Refresh;

/// <summary>
/// Planning only: validates a requested windfarm/date range and splits it into Windfarm-Day
/// work items for <see cref="ProductionIngestionRunner"/> in <see cref="IngestionMode.Refresh"/>.
/// The public `RefreshAndRecalculate` HTTP endpoint that coordinates every source lives in a
/// separate Application.Api component (out of scope here).
/// </summary>
public sealed class ProductionRefreshPlanner
{
    public IngestionRequestModel Plan(RefreshRequestModel request)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.WindfarmName);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.RunId);

        if (!int.TryParse(request.WindfarmId, out var selectedWindfarmId))
        {
            throw new ArgumentException("WindfarmId must be a valid integer.", nameof(request.WindfarmId));

        } 

        return new IngestionRequestModel(
            WindfarmId: selectedWindfarmId,
            request.WindfarmName,
            SourceType: "Production",
            IngestionMode.Refresh,
            ExplicitDates: request.GetDataDates(),
            RunId: request.RunId);
    }
}
