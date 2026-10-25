namespace ProductionIngestor.Contracts;

public static class ProductionParquetSchemaV2
{
    public const int Version = 2;

    public static IReadOnlyList<string> Columns { get; } = Array.AsReadOnly(
    [
        WtgRdsppId,
        StartTimeInterval,
        EndTimeInterval,
        ActualProductionKWh,
        PossibleProductionKWh,
        LostProductionCapacitykWh,
        ContractualPoPEColumn,
        SiteAverageWindSpeedMps,
        LastUpdatedDate
    ]);

    public const string WtgRdsppId = "WtgRdsppId";
    public const string StartTimeInterval = "StartTimeInterval";
    public const string EndTimeInterval = "EndTimeInterval";
    public const string ActualProductionKWh = "ActualProductionKWh";
    public const string PossibleProductionKWh = "PossibleProductionKWh";
    public const string LostProductionCapacitykWh = "LostProductionCapacitykWh";
    public const string ContractualPoPEColumn = "ContractualPoPEColumn";
    public const string SiteAverageWindSpeedMps = "SiteAverageWindSpeedMps";
    public const string LastUpdatedDate = "LastUpdatedDate";
}