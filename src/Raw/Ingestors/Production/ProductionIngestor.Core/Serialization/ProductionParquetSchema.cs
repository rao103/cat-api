using Parquet.Schema;
using ProductionIngestor.Contracts;

namespace ProductionIngestor.Serialization;

internal static class ProductionParquetSchema
{
    public static DataField[] CreateFields(
        IEnumerable<decimal?> actualProduction,
        IEnumerable<decimal?> possibleProduction,
        IEnumerable<decimal?> lostProductionCapacity,
        IEnumerable<decimal?> siteAverageWindSpeed)
        =>
        [
            new DataField<string>(ProductionParquetSchemaV2.WtgRdsppId),
            new DateTimeDataField(ProductionParquetSchemaV2.StartTimeInterval, DateTimeFormat.Impala),
            new DateTimeDataField(ProductionParquetSchemaV2.EndTimeInterval, DateTimeFormat.Impala),
            CreateDecimalField(ProductionParquetSchemaV2.ActualProductionKWh, actualProduction),
            CreateDecimalField(ProductionParquetSchemaV2.PossibleProductionKWh, possibleProduction),
            CreateDecimalField(ProductionParquetSchemaV2.LostProductionCapacitykWh, lostProductionCapacity),
            new DataField<string>(ProductionParquetSchemaV2.ContractualPoPEColumn),
            CreateDecimalField(ProductionParquetSchemaV2.SiteAverageWindSpeedMps, siteAverageWindSpeed),
            new DateTimeDataField(ProductionParquetSchemaV2.LastUpdatedDate, DateTimeFormat.Impala),
        ];

    private static DecimalDataField CreateDecimalField(string name, IEnumerable<decimal?> values)
    {
        var scale = values.Where(value => value.HasValue)
            .Select(value => (decimal.GetBits(value!.Value)[3] >> 16) & 0xff).DefaultIfEmpty(0).Max();
        return new DecimalDataField(name, 38, scale, forceByteArrayEncoding: true, isNullable: true);
    }
}