using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using ProductionIngestor.Adapters.Source;
using ProductionIngestor.Configuration;
using ProductionIngestor.Factories;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Extraction;

namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// Reads the expected Windfarm-Day source snapshot directly, independent of the Worker process and
/// the Parquet writer. Reuses the Worker's own connection/authentication factories (never the
/// runner) so the query semantics, deduplication, and validation stay identical.
/// </summary>
public sealed class ExpectedSnapshotReader : IDisposable
{
    private readonly SqlProductionConnectionFactory _connectionFactory;
    private readonly WindfarmDayExtractor _extractor;
    private readonly int _maxSnapshotRows;

    public ExpectedSnapshotReader(IConfiguration configuration, int maxSnapshotRows = 1_000_000)
    {
        var sqlSourceOptions = new SqlSourceOptions();
        configuration.Bind(sqlSourceOptions);
        var legacyConnectionString = configuration["Source:ConnectionString"];

        _connectionFactory = new SqlProductionConnectionFactory(sqlSourceOptions, legacyConnectionString);
        var ingestionOptions = new IngestionOptions();
        configuration.GetSection(IngestionOptions.SectionName).Bind(ingestionOptions);
        _maxSnapshotRows = maxSnapshotRows;
        var adapter = new SqlProductionSourceAdapter(_connectionFactory, ingestionOptions);
        _extractor = new WindfarmDayExtractor(adapter, _maxSnapshotRows);
    }

    /// <summary>Reads the deduplicated snapshot, bounding the read so a runaway source cannot hang verification.</summary>
    public async Task<IReadOnlyList<ProductionRowModel>> ReadSnapshotAsync(
        string windfarmId, DateOnly productionDate, CancellationToken cancellationToken)
    {
        var rows = new List<ProductionRowModel>();
        await foreach (var row in _extractor.GetDeduplicatedSnapshotAsync(windfarmId, productionDate, cancellationToken))
        {
            rows.Add(row);
            if (rows.Count > _maxSnapshotRows)
                throw new InvalidOperationException("Expected-snapshot verification read limit exceeded.");
        }
        return rows;
    }

    public void Dispose() => _connectionFactory.Dispose();
}
