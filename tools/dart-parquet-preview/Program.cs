// Throwaway dev tool: extracts a hardcoded date range directly from DART and writes the
// resulting Raw-zone parquet artifact(s) to a local folder, so the artifact shape/content can be
// inspected before IRSA (AWS IAM Roles for Service Accounts) is wired up for the control plane
// and blob storage. Deliberately bypasses ProductionIngestionRunner: no lease, no cursor, no
// control-plane (RDS/Postgres) connection, and no SQS publish — just DART -> parquet -> disk.
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Configuration;
using Parquet;
using ProductionIngestor.Adapters.Source;
using ProductionIngestor.Configuration;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Extraction;
using ProductionIngestor.Serialization;

// ============================================================================================
// EDIT THESE for the extraction you want to preview.
// ============================================================================================
const string WindfarmId = "WOW03"; // DART SiteRDSPPID, e.g. "123"
var startDate = new DateOnly(2026, 1, 3);
var endDate = new DateOnly(2026, 1, 4); // inclusive; set > startDate to pull a range of days
const long ExtractionToken = 1; // fixed so reruns overwrite the same local preview path
const int MaxPreviewRowsPerDay = 20;
// ============================================================================================

// Anchored to this source file rather than CWD, so it works regardless of where `dotnet run` is invoked from.
static string GetSourceFilePath([CallerFilePath] string path = "") => path;
var repoRoot = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(GetSourceFilePath())!, "..", ".."));
var repoAppSettingsPath = Path.Combine(repoRoot, "src", "Raw", "Ingestors", "Production", "ProductionIngestor.Worker", "appsettings.json");
var outputRoot = Path.Combine(repoRoot, "tools", "dart-parquet-preview", "local-raw-output");

Console.WriteLine("=== DART -> Raw parquet local preview ===");
Console.WriteLine($"Windfarm       : {WindfarmId}");
Console.WriteLine($"Date range     : {startDate:yyyy-MM-dd} .. {endDate:yyyy-MM-dd}");
Console.WriteLine($"Output folder  : {outputRoot}");
Console.WriteLine();

if (WindfarmId.StartsWith("REPLACE_", StringComparison.Ordinal))
{
    Console.WriteLine("Set WindfarmId at the top of Program.cs to a real DART SiteRDSPPID before running.");
    return 2;
}

var configuration = new ConfigurationBuilder()
    .AddJsonFile(repoAppSettingsPath, optional: false, reloadOnChange: false)
    .Build();

// Only the DART (Source) connection is needed — no control-plane/RDS, no blob, no SQS.
var sqlSourceOptions = new SqlSourceOptions();
configuration.Bind(sqlSourceOptions);
var sourceOptions = new SourceOptions { SourceSystemName = "production", BlobContainerName = "n/a", ControlPlaneConnectionString = "n/a" };
configuration.GetSection(SourceOptions.SectionName).Bind(sourceOptions);
var ingestionOptions = new IngestionOptions();
configuration.GetSection(IngestionOptions.SectionName).Bind(ingestionOptions);

using var connectionFactory = new SqlProductionConnectionFactory(sqlSourceOptions, sourceOptions.ConnectionString);
var sourceAdapter = new SqlProductionSourceAdapter(connectionFactory, ingestionOptions);
var dayExtractor = new WindfarmDayExtractor(sourceAdapter, ingestionOptions.MaxSnapshotRows);
var store = new LocalFileObjectStore(outputRoot);
var writer = new ProductionParquetWriter(store, ingestionOptions.MaxRowsPerShard,
    ingestionOptions.ParquetRowGroupSize, ingestionOptions.MaxArtifactShardSizeBytes);

try
{
    for (var date = startDate; date <= endDate; date = date.AddDays(1))
    {
        Console.WriteLine($"--- {date:yyyy-MM-dd} ---");

        var rows = new List<ProductionRowModel>();
        await foreach (var row in dayExtractor.GetDeduplicatedSnapshotAsync(WindfarmId, date, CancellationToken.None))
        {
            rows.Add(row);
        }
        Console.WriteLine($"  Read from DART: {rows.Count} row(s)");

        var result = await writer.WriteDayAsync(WindfarmId, date, ExtractionToken, rows, CancellationToken.None);
        Console.WriteLine($"  Written        : {result.RowCount} valid row(s), {result.QuarantinedRows.Count} quarantined");

        var writtenPaths = result.BlobPath is not null ? [result.BlobPath] : result.ShardPaths ?? [];
        foreach (var path in writtenPaths)
        {
            var localPath = store.ResolvePath(path);
            var length = new FileInfo(localPath).Length;
            Console.WriteLine($"  Artifact       : {localPath} ({length:N0} bytes)");
            await PrintParquetPreviewAsync(localPath, MaxPreviewRowsPerDay);
        }
        Console.WriteLine();
    }

    Console.WriteLine("Done.");
    return 0;
}
catch (Exception error)
{
    Console.WriteLine($"FAILED: {error.GetType().Name}: {error.Message}");
    return 1;
}

static async Task PrintParquetPreviewAsync(string filePath, int maxRows)
{
    await using var stream = File.OpenRead(filePath);
    await using var reader = await ParquetReader.CreateAsync(stream);
    var fields = reader.Schema.GetDataFields();

    var itemIdField = fields.Single(field => field.Name == "WtgRdsppId");
    var startField = fields.Single(field => field.Name == "StartTimeInterval");
    var endField = fields.Single(field => field.Name == "EndTimeInterval");
    var actualField = fields.Single(field => field.Name == "ActualProductionKWh");
    var possibleField = fields.Single(field => field.Name == "PossibleProductionKWh");
    var lostProductionField = fields.Single(field => field.Name == "LostProductionCapacitykWh");
    var contractualField = fields.Single(field => field.Name == "ContractualPoPEColumn");
    var windSpeedField = fields.Single(field => field.Name == "SiteAverageWindSpeedMps");
    var updatedField = fields.Single(field => field.Name == "LastUpdatedDate");

    var printed = 0;
    for (var groupIndex = 0; groupIndex < reader.RowGroupCount && printed < maxRows; groupIndex++)
    {
        using var group = reader.OpenRowGroupReader(groupIndex);
        var rowCount = (int)group.RowCount;

        var itemIds = new string[rowCount];
        var starts = new DateTime[rowCount];
        var ends = new DateTime[rowCount];
        var actuals = new decimal?[rowCount];
        var possibles = new decimal?[rowCount];
        var lostProductions = new decimal?[rowCount];
        var windSpeeds = new decimal?[rowCount];
        var contractual = new string?[rowCount];
        var lastUpdated = new DateTime[rowCount];

        await group.ReadAsync(itemIdField, itemIds);
        await group.ReadAsync<DateTime>(startField, starts);
        await group.ReadAsync<DateTime>(endField, ends);
        await group.ReadAsync<decimal>(actualField, actuals);
        await group.ReadAsync<decimal>(possibleField, possibles);
        await group.ReadAsync<decimal>(lostProductionField, lostProductions);
        await group.ReadAsync<decimal>(windSpeedField, windSpeeds);
        await group.ReadAsync(contractualField, contractual);
        await group.ReadAsync<DateTime>(updatedField, lastUpdated);

        for (var rowIndex = 0; rowIndex < rowCount && printed < maxRows; rowIndex++, printed++)
        {
            Console.WriteLine(
                $"    [{printed + 1}] WtgRdsppId={itemIds[rowIndex]} Start={starts[rowIndex]:O} End={ends[rowIndex]:O} " +
                $"Actual={actuals[rowIndex]} Possible={possibles[rowIndex]} " +
                $"LostProductionCapacitykWh={lostProductions[rowIndex]} ContractualPoPEColumn={contractual[rowIndex]} " +
                $"SiteAverageWindSpeedMps={windSpeeds[rowIndex]} LastUpdatedDate={lastUpdated[rowIndex]:O}");
        }
    }
    if (printed == 0)
    {
        Console.WriteLine("    (zero rows in artifact)");
    }
}

/// <summary>Writes Raw artifacts to a local folder instead of Azure Blob, mirroring the raw path convention on disk.</summary>
internal sealed class LocalFileObjectStore : IRawObjectStore
{
    private readonly string _baseFolder;

    public LocalFileObjectStore(string baseFolder) => _baseFolder = baseFolder;

    public string ResolvePath(string path)
        => Path.Combine(_baseFolder, path.TrimStart('/').Replace('/', Path.DirectorySeparatorChar));

    public async Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
    {
        var fullPath = ResolvePath(path);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        await File.WriteAllBytesAsync(fullPath, content.ToArray(), cancellationToken);
    }

    public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken)
        => Task.FromResult(File.Exists(ResolvePath(path)));

    public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken)
        => File.ReadAllBytesAsync(ResolvePath(path), cancellationToken);
}
