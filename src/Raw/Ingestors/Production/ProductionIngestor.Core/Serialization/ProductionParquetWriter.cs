using CAT.Messaging.Contracts;
using Parquet;
using Parquet.Schema;
using ProductionIngestor.Contracts;
using ProductionIngestor.Enums;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Raw;

namespace ProductionIngestor.Serialization;

/// <summary>
/// Writes an immutable Raw artifact for one Windfarm-Day: validates/quarantines rows,
/// determines deterministic shard boundaries when the artifact exceeds the configured
/// size limit, uploads via <see cref="IRawObjectStore"/>, and verifies the write is durable.
/// A legitimately empty day still produces a (zero-row) artifact.
/// </summary>
public sealed class ProductionParquetWriter
{
    private readonly IRawObjectStore _store;
    private readonly int _maxRowsPerShard;
    private readonly int _rowGroupSize;
    private readonly long _maxShardSizeBytes;

    private sealed record PartNumberState(int Next);

    public ProductionParquetWriter(IRawObjectStore store, int maxRowsPerShard = 200_000,
        int rowGroupSize = 50_000, long maxShardSizeBytes = 128L * 1024 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRowsPerShard);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowGroupSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxShardSizeBytes);
        _store = store;
        _maxRowsPerShard = maxRowsPerShard;
        _rowGroupSize = rowGroupSize;
        _maxShardSizeBytes = maxShardSizeBytes;
    }

    /// <summary>
    /// Validates and writes the given rows (already-deduplicated snapshot input). Rows that fail
    /// validation are excluded from the artifact and reported in the quarantine artifact/result.
    /// </summary>
    public async Task<RawWriteResultModel> WriteDayAsync(
        string windfarmName,
        DateOnly productionDate,
        long extractionToken,
        IReadOnlyList<ProductionRowModel> rows,
        CancellationToken cancellationToken,
        int? windfarmId = null)
    {
        var (validRows, quarantined) = Validate(rows);

        var shards = DetermineShards(validRows, _maxRowsPerShard);
        var pending = new Stack<IReadOnlyList<ProductionRowModel>>(shards.Reverse());
        if (pending.Count == 0) pending.Push(Array.Empty<ProductionRowModel>());
        var split = pending.Count > 1;
        var shardPaths = new List<string>();
        string? singlePath = null;
        while (pending.TryPop(out var shard))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var dayStartUtc = DateTime.SpecifyKind(productionDate.ToDateTime(TimeOnly.MinValue), DateTimeKind.Utc);
            var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["source_type"] = "Production",
                ["windfarm_name"] = windfarmName,
                ["extraction_mode"] = "Incremental",
                ["payload_kind"] = "ProductionSnapshot",
                ["query_axis"] = "ProductionTimestampUtc",
                ["payload_schema_version"] = ProductionParquetSchemaV2.Version.ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["extraction_token"] = extractionToken.ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["query_start_inclusive_utc"] = dayStartUtc.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
                ["query_end_exclusive_utc"] = dayStartUtc.AddDays(1).ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            };
            if (windfarmId.HasValue)
                metadata["windfarm_id"] = windfarmId.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            var bytes = await SerializeAsync(shard, cancellationToken, metadata);
            if (bytes.Length > _maxShardSizeBytes)
            {
                if (shard.Count <= 1) throw new InvalidDataException("A single record or empty artifact exceeds the shard byte limit.");
                var midpoint = shard.Count / 2;
                pending.Push(shard.Skip(midpoint).ToArray());
                pending.Push(shard.Take(midpoint).ToArray());
                split = true;
                continue;
            }
            var path = split
                ? RawPathBuilder.BuildShardPath(windfarmName, productionDate, extractionToken, shardPaths.Count + 1)
                : RawPathBuilder.BuildSingleArtifactPath(windfarmName, productionDate, extractionToken);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyUploadedContentAsync(path, bytes, cancellationToken);
            if (split) shardPaths.Add(path);
            else singlePath = path;
        }

        if (quarantined.Count > 0)
        {
            await WriteQuarantineAsync(windfarmName, productionDate, extractionToken, quarantined, cancellationToken);
        }

        return new RawWriteResultModel(windfarmName, productionDate, extractionToken, singlePath,
            split ? shardPaths : null, validRows.Count, quarantined);
    }

    public async Task WriteSnapshotManifestAsync(RawSnapshotManifestV1 manifest, CancellationToken cancellationToken)
    {
        manifest.Validate();
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(manifest);
        var path = manifest.Envelope.ManifestPath!;
        await _store.UploadAsync(path, bytes, cancellationToken);
        await VerifyUploadedContentAsync(path, bytes, cancellationToken);
    }

    public async Task<IReadOnlyList<ProductionArtifactReferenceV1>> WriteRefreshPartAsync(
        string windfarmName, int windfarmId, long extractionToken, string requestId, string attemptId,
        int payloadSchemaVersion, int queryWindowNumber, DateTime queryStartUtc, DateTime queryEndExclusiveUtc,
        int firstPartNumber, IReadOnlyList<ProductionDateRangeV1> selectedScope,
        IReadOnlyList<ProductionRowModel> rows, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(payloadSchemaVersion);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queryWindowNumber);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(firstPartNumber);
        ArgumentNullException.ThrowIfNull(selectedScope);
        ArgumentNullException.ThrowIfNull(rows);

        var references = new List<ProductionArtifactReferenceV1>();
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["source_type"] = "Production",
            ["windfarm_id"] = windfarmId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["windfarm_name"] = windfarmName,
            ["extraction_mode"] = "Refresh",
            ["payload_kind"] = "ProductionSnapshot",
            ["query_axis"] = "ProductionTimestampUtc",
            ["request_id"] = requestId,
            ["attempt_id"] = attemptId,
            ["extraction_token"] = extractionToken.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["payload_schema_version"] = payloadSchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["query_start_inclusive_utc"] = queryStartUtc.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            ["query_end_exclusive_utc"] = queryEndExclusiveUtc.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            ["query_window_number"] = queryWindowNumber.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["requested_scope"] = string.Join(",", selectedScope.Select(range => $"{range.StartDate}/{range.EndDateInclusive}")),
        };

        async Task<int> PersistPartAsync(IReadOnlyList<ProductionRowModel> partRows, int partNumber)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var partMetadata = new Dictionary<string, string>(metadata, StringComparer.Ordinal)
            {
                ["part_number"] = partNumber.ToString(System.Globalization.CultureInfo.InvariantCulture),
                ["row_count"] = partRows.Count.ToString(System.Globalization.CultureInfo.InvariantCulture),
            };
            var bytes = await SerializeAsync(partRows, cancellationToken, partMetadata);
            if (bytes.Length > _maxShardSizeBytes)
            {
                if (partRows.Count <= 1)
                    throw new InvalidDataException("A single refresh record exceeds the configured Parquet part byte limit.");
                var midpoint = partRows.Count / 2;
                var nextPartNumber = await PersistPartAsync(partRows.Take(midpoint).ToArray(), partNumber);
                return await PersistPartAsync(partRows.Skip(midpoint).ToArray(), nextPartNumber);
            }

            var path = RawPathBuilder.BuildPartPath(windfarmName, IngestionMode.Refresh, extractionToken,
                queryStartUtc, queryEndExclusiveUtc, partNumber);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyUploadedContentAsync(path, bytes, cancellationToken);
            references.Add(new ProductionArtifactReferenceV1(path, queryWindowNumber, partNumber, partRows.Count));
            return partNumber + 1;
        }

        if (rows.Count > 0)
            await PersistPartAsync(rows, firstPartNumber);
        return references;
    }

    /// <summary>
    /// Deterministically shards a row set by a stable ordering key (ItemId, then timestamp) so that
    /// a retry before cursor advance rewrites identical shard contents at identical paths.
    /// </summary>
    public static IReadOnlyList<IReadOnlyList<ProductionRowModel>> DetermineShards(
        IReadOnlyList<ProductionRowModel> rows, int maxRowsPerShard)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRowsPerShard);
        if (rows.Count == 0)
        {
            return Array.Empty<IReadOnlyList<ProductionRowModel>>();
        }

        var ordered = rows
            .OrderBy(row => row.WtgName, StringComparer.Ordinal)
            .ThenBy(row => row.ProductionTimestampUtc)
            .ToList();

        if (ordered.Count <= maxRowsPerShard)
        {
            return new[] { (IReadOnlyList<ProductionRowModel>)ordered };
        }

        var shards = new List<IReadOnlyList<ProductionRowModel>>();
        for (var rowIndex = 0; rowIndex < ordered.Count; rowIndex += maxRowsPerShard)
        {
            shards.Add(ordered.Skip(rowIndex).Take(maxRowsPerShard).ToList());
        }

        return shards;
    }

    /// <summary>
    /// Rejects malformed timestamps, ambiguous duplicate keys, and invalid measurement values, tagging each with
    /// a reason. Everything else passes through unchanged.
    /// </summary>
    internal static (List<ProductionRowModel> Valid, List<QuarantinedRowModel> Quarantined) Validate(
        IReadOnlyList<ProductionRowModel> rows)
    {
        var valid = new List<ProductionRowModel>();
        var quarantined = new List<QuarantinedRowModel>();

        var groups = rows.GroupBy(row => (row.WtgName, row.ProductionTimestampUtc));

        foreach (var group in groups)
        {
            var groupRows = group.ToList();

            if (groupRows.Count > 1)
            {
                var distinctValues = groupRows
                    .Select(row => (row.ActualProduction, row.PossibleProduction))
                    .Distinct()
                    .ToList();

                if (distinctValues.Count > 1)
                {
                    var ambiguous = groupRows.All(row => row.LastUpdatedUtc == groupRows[0].LastUpdatedUtc);
                    if (ambiguous)
                    {
                        foreach (var row in groupRows)
                        {
                            quarantined.Add(new QuarantinedRowModel(row.WtgName, row.ProductionTimestampUtc,
                                "duplicate contradictory key: no clear latest signal"));
                        }
                        continue;
                    }
                }
            }

            var chosen = groupRows.OrderByDescending(row => row.LastUpdatedUtc).First();

            if (chosen.ProductionTimestampUtc == default || chosen.ProductionTimestampUtc.Kind != DateTimeKind.Utc
                || chosen.EndDateAndTimeUtc == default || chosen.EndDateAndTimeUtc.Kind != DateTimeKind.Utc
                || chosen.LastUpdatedUtc == default || chosen.LastUpdatedUtc.Kind != DateTimeKind.Utc)
            {
                quarantined.Add(new QuarantinedRowModel(chosen.WtgName, chosen.ProductionTimestampUtc,
                    "malformed/missing ProductionTimestampUtc"));
                continue;
            }

            if (HasInvalidMeasurementValues(chosen))
            {
                quarantined.Add(new QuarantinedRowModel(chosen.WtgName, chosen.ProductionTimestampUtc,
                    "negative production measurement"));
                continue;
            }

            valid.Add(chosen);
        }

        return (valid, quarantined);
    }

    private static bool HasInvalidMeasurementValues(ProductionRowModel row)
    {
        return row.ActualProduction < 0 || row.PossibleProduction < 0;
    }

    private async Task<byte[]> SerializeAsync(IReadOnlyList<ProductionRowModel> rows,
        CancellationToken cancellationToken, Dictionary<string, string>? customMetadata = null)
    {
        var fields = ProductionParquetSchema.CreateFields(
            rows.Select(row => row.ActualProduction),
            rows.Select(row => row.PossibleProduction),
            rows.Select(row => row.LostProductionCapacitykWh),
            rows.Select(row => row.SiteAverageWindSpeedMps));
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            if (customMetadata is not null)
                writer.CustomMetadata = customMetadata;
            foreach (var batch in rows.Chunk(_rowGroupSize))
            {
                using var group = writer.CreateRowGroup();
                Array[] columns =
                [
                    batch.Select(row => row.WtgName).ToArray(),
                    batch.Select(row => row.ProductionTimestampUtc).ToArray(),
                    batch.Select(row => row.EndDateAndTimeUtc).ToArray(),
                    batch.Select(row => row.ActualProduction).ToArray(),
                    batch.Select(row => row.PossibleProduction).ToArray(),
                    batch.Select(row => row.LostProductionCapacitykWh).ToArray(),
                    batch.Select(row => row.ContractualPoPEColumn).ToArray(),
                    batch.Select(row => row.SiteAverageWindSpeedMps).ToArray(),
                    batch.Select(row => row.LastUpdatedUtc).ToArray()
                ];
                for (var columnIndex = 0; columnIndex < fields.Length; columnIndex++)
                {
                    await WriteColumnAsync(group, fields[columnIndex], columns[columnIndex], cancellationToken);
                }
            }
        }
        return stream.ToArray();
    }

    private static Task WriteColumnAsync(ParquetRowGroupWriter group, DataField field, Array values, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return values switch
        {
            string[] strings => group.WriteAsync(field, strings),
            decimal?[] decimals => group.WriteAsync<decimal>(field, decimals, cancellationToken: cancellationToken),
            DateTime[] timestamps => group.WriteAsync<DateTime>(field, timestamps, cancellationToken: cancellationToken),
            long[] tokens => group.WriteAsync<long>(field, tokens, cancellationToken: cancellationToken),
            int[] versions => group.WriteAsync<int>(field, versions, cancellationToken: cancellationToken),
            _ => throw new InvalidOperationException($"Unsupported Parquet column type: {values.GetType()}.")
        };
    }

    private async Task VerifyUploadedContentAsync(string path, byte[] expected, CancellationToken cancellationToken)
    {
        var actual = await _store.DownloadAsync(path, cancellationToken);
        if (!actual.AsSpan().SequenceEqual(expected))
            throw new InvalidDataException($"Uploaded artifact '{path}' differs from the generated content.");
    }

    private async Task WriteQuarantineAsync(
        string windfarmName, DateOnly productionDate, long extractionToken,
        IReadOnlyList<QuarantinedRowModel> quarantined, CancellationToken cancellationToken)
    {
        var path = RawPathBuilder.BuildQuarantinePath(windfarmName, productionDate, extractionToken);
        var fields = new DataField[] { new DataField<string>("ItemId"),
            new DateTimeDataField("ProductionTimestampUtc", DateTimeFormat.Impala), new DataField<string>("Reason") };
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            foreach (var batch in quarantined.Chunk(_rowGroupSize))
            {
                using var group = writer.CreateRowGroup();
                await WriteColumnAsync(group, fields[0], batch.Select(row => row.ItemId).ToArray(), cancellationToken);
                await WriteColumnAsync(group, fields[1], batch.Select(row => row.ProductionTimestampUtc).ToArray(), cancellationToken);
                await WriteColumnAsync(group, fields[2], batch.Select(row => row.Reason).ToArray(), cancellationToken);
            }
        }
        var bytes = stream.ToArray();
        await _store.UploadAsync(path, bytes, cancellationToken);
        await VerifyUploadedContentAsync(path, bytes, cancellationToken);
    }
}
