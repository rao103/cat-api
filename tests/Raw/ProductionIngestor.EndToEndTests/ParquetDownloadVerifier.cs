using Parquet;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.EndToEndTests;

/// <summary>Downloads and parses the Parquet shard(s) referenced by an observed artifact message, for comparison against the expected snapshot.</summary>
public static class ParquetDownloadVerifier
{
    public static async Task<IReadOnlyList<ProductionRowModel>> DownloadAndParseAsync(
        IRawObjectStore store, IEnumerable<string> blobPaths, CancellationToken cancellationToken)
    {
        var rows = new List<ProductionRowModel>();
        foreach (var path in blobPaths)
        {
            var bytes = await store.DownloadAsync(path, cancellationToken);
            using var stream = new MemoryStream(bytes);
            await using var reader = await ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken);
            var fields = reader.Schema.GetDataFields();
            var itemIdField = fields.Single(f => f.Name == "WtgRdsppId");
            var startField = fields.Single(f => f.Name == "StartTimeInterval");
            var endField = fields.Single(f => f.Name == "EndTimeInterval");
            var actualField = fields.Single(f => f.Name == "ActualProductionKWh");
            var possibleField = fields.Single(f => f.Name == "PossibleProductionKWh");
            var lostField = fields.Single(f => f.Name == "LostProductionCapacitykWh");
            var contractualField = fields.Single(f => f.Name == "ContractualPoPEColumn");
            var windSpeedField = fields.Single(f => f.Name == "SiteAverageWindSpeedMps");
            var lastUpdatedField = fields.Single(f => f.Name == "LastUpdatedDate");

            for (var groupIndex = 0; groupIndex < reader.RowGroupCount; groupIndex++)
            {
                using var group = reader.OpenRowGroupReader(groupIndex);
                var rowCount = checked((int)group.RowCount);
                var itemIds = new string?[rowCount];
                var starts = new DateTime[rowCount];
                var ends = new DateTime[rowCount];
                var actuals = new decimal?[rowCount];
                var possibles = new decimal?[rowCount];
                var lost = new decimal?[rowCount];
                var contractual = new string?[rowCount];
                var windSpeeds = new decimal?[rowCount];
                var lastUpdated = new DateTime[rowCount];

                // Parquet.Net 6.1: the string overload is non-generic (resolved by Memory<string?>
                // parameter type); the nullable numeric overload is ReadAsync<T> with T = the
                // non-nullable element type and a Memory<T?> destination.
                await group.ReadAsync(itemIdField, itemIds);
                await group.ReadAsync<DateTime>(startField, starts);
                await group.ReadAsync<DateTime>(endField, ends);
                await group.ReadAsync<decimal>(actualField, actuals);
                await group.ReadAsync<decimal>(possibleField, possibles);
                await group.ReadAsync<decimal>(lostField, lost);
                await group.ReadAsync(contractualField, contractual);
                await group.ReadAsync<decimal>(windSpeedField, windSpeeds);
                await group.ReadAsync<DateTime>(lastUpdatedField, lastUpdated);

                for (var i = 0; i < rowCount; i++)
                {
                    rows.Add(new ProductionRowModel(
                        string.Empty,
                        itemIds[i] ?? string.Empty,
                        starts[i],
                        actuals[i],
                        possibles[i],
                        lost[i],
                        windSpeeds[i],
                        lastUpdated[i],
                        null,
                        ends[i],
                        contractual[i]));
                }
            }
        }
        return rows;
    }
}
