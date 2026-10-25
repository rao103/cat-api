using Parquet;
using Parquet.Schema;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Raw;
using ProductionIngestor.Contracts;

namespace ProductionIngestor.Serialization;

/// <summary>Everything needed to write and identify one bounded ProductionChanges Raw part.</summary>
public sealed record ProductionChangesPartRequest(
    int WindfarmId,
    string WindfarmName,
    string SourceType,
    string SourceSystem,
    string RunId,
    long ExtractionToken,
    DateTime WindowMinInclusiveUtc,
    DateTime WindowMaxInclusiveUtc,
    int PartNumber);

/// <summary>Durable result of writing one ProductionChanges part, including its observed extents.</summary>
public sealed record ProductionChangesPartResult(
    string BlobPath,
    int RowCount,
    DateTime? MinProductionTimestampUtc,
    DateTime? MaxProductionTimestampUtc,
    DateTime? MinSourceUpdatedUtc,
    DateTime? MaxSourceUpdatedUtc);

/// <summary>
/// Writes one immutable ProductionChanges Raw part (schemaVersion 2 payload): the source rows in
/// their original order, with no aggregation, deduplication or reordering. A same-identity retry
/// with identical bytes is idempotent; a conflicting existing object with different bytes fails.
/// </summary>
public sealed class ProductionChangesPartWriter
{
    private readonly IRawObjectStore _store;
    private readonly int _rowGroupSize;

    public ProductionChangesPartWriter(IRawObjectStore store, int rowGroupSize = 50_000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowGroupSize);
        _store = store;
        _rowGroupSize = rowGroupSize;
    }

    public async Task<ProductionChangesPartResult> WritePartAsync(
        ProductionChangesPartRequest request, IReadOnlyList<ProductionChangeRowModel> rows, CancellationToken cancellationToken)
    {
        var path = RawPathBuilder.BuildPartPath(
            request.WindfarmName, ProductionIngestor.Enums.IngestionMode.Incremental, request.ExtractionToken,
            request.WindowMinInclusiveUtc, request.WindowMaxInclusiveUtc.AddTicks(1), request.PartNumber);
        var bytes = await SerializeAsync(request, rows, cancellationToken);

        if (await _store.ExistsAsync(path, cancellationToken))
        {
            var existing = await _store.DownloadAsync(path, cancellationToken);
            if (!existing.AsSpan().SequenceEqual(bytes))
            {
                throw new InvalidDataException($"A conflicting artifact already exists at '{path}' with different content.");
            }
        }
        else
        {
            await _store.UploadAsync(path, bytes, cancellationToken);
            var verify = await _store.DownloadAsync(path, cancellationToken);
            if (!verify.AsSpan().SequenceEqual(bytes))
            {
                throw new InvalidDataException($"Uploaded artifact '{path}' differs from the generated content.");
            }
        }

        return new ProductionChangesPartResult(
            path,
            rows.Count,
            rows.Count == 0 ? null : rows.Min(row => row.ProductionTimestampUtc),
            rows.Count == 0 ? null : rows.Max(row => row.ProductionTimestampUtc),
            rows.Count == 0 ? null : rows.Min(row => row.LastUpdatedUtc),
            rows.Count == 0 ? null : rows.Max(row => row.LastUpdatedUtc));
    }

    private async Task<byte[]> SerializeAsync(
        ProductionChangesPartRequest request, IReadOnlyList<ProductionChangeRowModel> rows, CancellationToken cancellationToken)
    {
        var fields = ProductionParquetSchema.CreateFields(
            rows.Select(row => row.ActualProduction),
            rows.Select(row => row.PossibleProduction),
            rows.Select(row => row.LostProductionCapacitykWh),
            rows.Select(row => row.SiteAverageWindSpeedMps));

        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            writer.CustomMetadata = BuildMetadata(request, rows);
            foreach (var batch in rows.Chunk(_rowGroupSize))
            {
                using var group = writer.CreateRowGroup();
                await group.WriteAsync(fields[0], batch.Select(row => row.WtgName).ToArray());
                await group.WriteAsync<DateTime>(fields[1], batch.Select(row => row.ProductionTimestampUtc).ToArray(), cancellationToken: cancellationToken);
                await group.WriteAsync<DateTime>(fields[2], batch.Select(row => row.EndDateAndTimeUtc).ToArray(), cancellationToken: cancellationToken);
                await group.WriteAsync<decimal>(fields[3], batch.Select(row => row.ActualProduction).ToArray(), cancellationToken: cancellationToken);
                await group.WriteAsync<decimal>(fields[4], batch.Select(row => row.PossibleProduction).ToArray(), cancellationToken: cancellationToken);
                await group.WriteAsync<decimal>(fields[5], batch.Select(row => row.LostProductionCapacitykWh).ToArray(), cancellationToken: cancellationToken);
                await group.WriteAsync(fields[6], batch.Select(row => row.ContractualPoPEColumn).ToArray());
                await group.WriteAsync<decimal>(fields[7], batch.Select(row => row.SiteAverageWindSpeedMps).ToArray(), cancellationToken: cancellationToken);
                await group.WriteAsync<DateTime>(fields[8], batch.Select(row => row.LastUpdatedUtc).ToArray(), cancellationToken: cancellationToken);
            }
        }
        return stream.ToArray();
    }

    private static Dictionary<string, string> BuildMetadata(ProductionChangesPartRequest request, IReadOnlyList<ProductionChangeRowModel> rows)
    {
        var metadata = new Dictionary<string, string>
        {
            ["envelope_version"] = "2",
            ["payload_schema_version"] = ProductionParquetSchemaV2.Version.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["extraction_mode"] = "Incremental",
            ["payload_kind"] = "ProductionChanges",
            ["source_type"] = request.SourceType,
            ["windfarm_id"] = request.WindfarmId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["windfarm_name"] = request.WindfarmName,
            ["source_system"] = request.SourceSystem,
            ["run_id"] = request.RunId,
            ["request_id"] = request.RunId,
            ["attempt_id"] = request.ExtractionToken.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["extraction_token"] = request.ExtractionToken.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["query_axis"] = "LastUpdatedUtc",
            ["query_start_inclusive_utc"] = request.WindowMinInclusiveUtc.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            ["query_end_exclusive_utc"] = request.WindowMaxInclusiveUtc.AddTicks(1).ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            ["part_number"] = request.PartNumber.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["row_count"] = rows.Count.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["extracted_at_utc"] = DateTime.UtcNow.ToString("O"),
        };

        if (rows.Count > 0)
        {
            metadata["min_production_timestamp"] = rows.Min(row => row.ProductionTimestampUtc).ToString("O");
            metadata["max_production_timestamp"] = rows.Max(row => row.ProductionTimestampUtc).ToString("O");
            metadata["min_source_updated"] = rows.Min(row => row.LastUpdatedUtc).ToString("O");
            metadata["max_source_updated"] = rows.Max(row => row.LastUpdatedUtc).ToString("O");
        }

        return metadata;
    }
}
