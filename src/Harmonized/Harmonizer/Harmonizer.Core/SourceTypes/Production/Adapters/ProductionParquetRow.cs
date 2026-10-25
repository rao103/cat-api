using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Production.Models;
namespace Harmonizer.Core.SourceTypes.Production.Adapters;

public sealed class ProductionParquetRow
{
    public int WindfarmId { get; set; }
    public string WindfarmName { get; set; } = "";
    public int WtgId { get; set; }
    public string WtgName { get; set; } = "";
    public string WtgRdsppId { get; set; } = "";
    public DateTime StartTimeInterval { get; set; }
    public DateTime EndTimeInterval { get; set; }
    public double? ActualProductionKWh { get; set; }
    public double? PossibleProductionKWh { get; set; }
    public double? LostProductionCapacitykWh { get; set; }
    public double? ContractualPoPEColumn { get; set; }
    public double? SiteAverageWindSpeedMps { get; set; }
    public DateTime LastUpdatedDate { get; set; }
    public string RawPath { get; set; } = "";
    public long ExtractionToken { get; set; }

    public static ProductionParquetRow From(WindfarmIdentity farm, ProductionRow row) => new()
    {
        WindfarmId = farm.WindfarmId,
        WindfarmName = farm.WindfarmName,
        WtgId = row.WtgId,
        WtgName = row.WtgName,
        WtgRdsppId = row.WtgName,
        StartTimeInterval = row.StartTimeInterval,
        EndTimeInterval = row.EndTimeInterval,
        LastUpdatedDate = row.LastUpdatedDate,
        ActualProductionKWh = row.Values.ActualProductionKWh,
        PossibleProductionKWh = row.Values.PossibleProductionKWh,
        LostProductionCapacitykWh = row.Values.LostProductionCapacitykWh,
        ContractualPoPEColumn = row.Values.ContractualPoPEColumn,
        SiteAverageWindSpeedMps = row.Values.SiteAverageWindSpeedMps,
        RawPath = row.Origin.BlobPath,
        ExtractionToken = row.Origin.ExtractionToken
    };
}
