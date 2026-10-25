using System.Globalization;
using System.Text.Json;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Harmonizer.Core.SourceTypes.Production.Models;
using Harmonizer.Core.SourceTypes.Production.Serialization;
using Microsoft.Extensions.Options;
using Parquet;
using Parquet.Serialization;
using ProductionIngestor.Contracts;
namespace Harmonizer.Core.SourceTypes.Production.Adapters;

public sealed class ParquetProductionArtifactStore(IRawObjectStore rawObjects, IHarmonizedArtifactStore artifacts,
    HarmonizedArtifactPathBuilder paths, IOptions<HarmonizerOptions> options,
    ITelemetry<ParquetProductionArtifactStore> telemetry) : IProductionArtifactStore
{
    public async Task<IReadOnlyList<ProductionRow>> ReadRawAsync(ProductionWork work, CancellationToken cancellationToken)
    {
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["StorageRole"] = "Raw" });
        var turbines = work.Turbines.ToDictionary(turbine => turbine.WtgName, StringComparer.Ordinal);
        var result = new List<ProductionRow>();
        var manifest = work.IsSnapshot ? RefreshManifestValidator.Validate(work) : null;
        var requiredPaths = manifest is null ? work.RawPaths : RefreshManifestValidator.RequiredArtifacts(manifest, work.DataDate)
            .Select(artifact => artifact.BlobPath).ToArray();
        if (requiredPaths.Count > options.Value.MaximumRawPaths)
        {
            throw new WorkValidationException("Refresh part set exceeds the configured bound.");
        }

        foreach (var path in requiredPaths)
        {
            await using var stream = await rawObjects.OpenReadAsync(path, cancellationToken);
            if (!stream.CanSeek || stream.Length > options.Value.MaximumArtifactBytes)
            {
                throw new WorkValidationException("Raw artifact must be seekable and within the configured byte limit.");
            }

            int groupCount;
            long count = 0;
            await using (var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: cancellationToken))
            {
                if (!ProductionParquetSchemaV2.Columns.All(name => reader.Schema.GetDataFields().Any(field => field.Name == name)))
                {
                    throw new WorkValidationException("Raw Production Parquet is missing schema-v2 columns.");
                }

                groupCount = reader.RowGroupCount;
                foreach (var group in reader.RowGroups)
                {
                    count = checked(count + group.RowCount);
                }

                if (count > options.Value.MaximumRowsPerArtifact)
                {
                    throw new WorkValidationException("Raw row count exceeds the configured bound.");
                }
            }
            if (manifest is not null && manifest.Artifacts.Single(artifact => artifact.BlobPath == path).RowCount != count)
            {
                throw new WorkValidationException("Raw artifact row count differs from the completed refresh manifest.");
            }

            telemetry.Debug("Production Raw part validated. BlobPath={BlobPath} RowCount={RowCount} RowGroups={RowGroups}",
                path, count, groupCount);

            for (var group = 0; group < groupCount; group++)
            {
                stream.Position = 0;
                var data = await ParquetSerializer.DeserializeUntypedAsync(stream, rowGroupIndex: group, cancellationToken: cancellationToken);
                foreach (var row in data.Data)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var name = row[ProductionParquetSchemaV2.WtgRdsppId] as string
                        ?? throw new WorkValidationException("WtgRdsppId must contain a turbine name.");
                    if (!turbines.TryGetValue(name, out var turbine))
                    {
                        continue;
                    }

                    var start = Utc(row, ProductionParquetSchemaV2.StartTimeInterval);
                    if (DateOnly.FromDateTime(start) != work.DataDate)
                    {
                        continue;
                    }

                    result.Add(new(turbine.WtgId, name, start, Utc(row, ProductionParquetSchemaV2.EndTimeInterval),
                        Utc(row, ProductionParquetSchemaV2.LastUpdatedDate),
                        new(Number(row, ProductionParquetSchemaV2.ActualProductionKWh), Number(row, ProductionParquetSchemaV2.PossibleProductionKWh),
                            Number(row, ProductionParquetSchemaV2.LostProductionCapacitykWh), Number(row, ProductionParquetSchemaV2.ContractualPoPEColumn),
                            Number(row, ProductionParquetSchemaV2.SiteAverageWindSpeedMps)),
                        new(path, work.ExtractionToken, work.RequestId, work.AttemptId,
                            work.Operation.OperationId, work.Operation.ExecutionId)));
                    if (result.Count > options.Value.MaximumRowsPerArtifact)
                    {
                        throw new WorkValidationException("Selected work rows exceed the configured batch bound.");
                    }
                }
            }
        }
        if (!work.IsSnapshot && work.Turbines.Any(turbine => !result.Any(row => row.WtgId == turbine.WtgId)))
        {
            throw new WorkValidationException("An incremental batch member has no rows in the exact artifact/date.");
        }

        if (work.HasNoSourceRows != (result.Count == 0) && work.IsSnapshot)
        {
            throw new WorkValidationException("Refresh empty-scope flag disagrees with actual selected rows.");
        }

        telemetry.Information("Production Raw work selected. BatchId={BatchId} DataDate={DataDate} ArtifactCount={ArtifactCount} RowCount={RowCount}",
            work.Batch.BatchId, work.DataDate, requiredPaths.Count, result.Count);
        return result;
    }

    public async Task<ProductionSnapshot> ReadSnapshotAsync(string path, CancellationToken cancellationToken)
    {
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["StorageRole"] = "Harmonized" });
        await using var stream = await artifacts.OpenReadAsync(path, cancellationToken);
        if (stream.Length > options.Value.MaximumArtifactBytes)
        {
            throw new WorkValidationException("Snapshot exceeds byte limit.");
        }

        var snapshot = await JsonSerializer.DeserializeAsync<ProductionSnapshot>(stream, HarmonizerJson.Options, cancellationToken)
            ?? throw new WorkValidationException("Committed snapshot is missing.");
        telemetry.Debug("Production snapshot loaded. SnapshotPath={SnapshotPath} RowCount={RowCount}", path, snapshot.Rows.Count);
        return snapshot;
    }

    public async Task<ComponentCandidate> WriteCandidateAsync(ComponentLease lease, ProductionWork work,
        MergeResult result, CancellationToken cancellationToken)
    {
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["StorageRole"] = "Harmonized" });
        var turbine = work.Turbines.Single(member => member.WtgId == lease.Key.WtgId);
        var prefix = paths.CreateComponentPrefix(lease, work.Windfarm, turbine);
        string? parquetPath = null;
        if (result.Snapshot.Rows.Count > 0)
        {
            parquetPath = prefix + "/production-10minute.parquet";
            using var parquet = new MemoryStream();
            await ParquetSerializer.SerializeAsync(result.Snapshot.Rows.Select(row => ProductionParquetRow.From(work.Windfarm, row)),
                parquet, cancellationToken: cancellationToken);
            // Validate the newly generated candidate's rows before uploading or committing its pointer.
            parquet.Position = 0;
            var verified = await ParquetSerializer.DeserializeAsync<ProductionParquetRow>(parquet, cancellationToken: cancellationToken);
            if (verified.Data.Count != result.Snapshot.Rows.Count)
            {
                throw new WorkValidationException("Candidate Parquet row count mismatch.");
            }

            parquet.Position = 0;
            await artifacts.WriteNewAsync(parquetPath, parquet, "application/vnd.apache.parquet", cancellationToken);
        }
        var snapshotPath = prefix + "/snapshot.json";
        var manifestPath = prefix + "/lineage.json";
        await WriteJsonAsync(snapshotPath, result.Snapshot, cancellationToken);
        await WriteJsonAsync(manifestPath, new
        {
            SchemaVersion = 1,
            work.Operation.OperationId,
            work.Operation.ExecutionId,
            TransformVersion = result.Snapshot.TransformVersion,
            lease.Key,
            ComponentRevision = lease.Revision + 1,
            CommittedAttempt = lease.FenceToken,
            result.CanonicalHash,
            RecordCount = result.Snapshot.Rows.Count,
            Status = result.Complete ? "Complete" : "Partial",
            ParquetPath = parquetPath,
            SnapshotPath = snapshotPath,
            PreviousManifestPath = lease.ManifestPath,
            work.Batch,
            work.OriginalMessageBody,
            Contributions = result.Snapshot.Rows.Select(row => row.Origin).Distinct().ToArray(),
            RowLineage = result.Snapshot.Rows.Select(row => new { row.WtgId, row.StartTimeInterval, row.LastUpdatedDate, row.Origin }).ToArray()
        }, cancellationToken);
        telemetry.Information("Production candidate artifacts written. WindfarmId={WindfarmId} WtgId={WtgId} DataDate={DataDate} Revision={Revision} RowCount={RowCount}",
            lease.Key.WindfarmId, lease.Key.WtgId, lease.Key.DataDate, lease.Revision + 1, result.Snapshot.Rows.Count);
        return new(parquetPath, snapshotPath, manifestPath);
    }

    private async Task WriteJsonAsync<T>(string path, T value, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream(JsonSerializer.SerializeToUtf8Bytes(value, HarmonizerJson.Options));
        await artifacts.WriteNewAsync(path, stream, "application/json", cancellationToken);
    }
    private static DateTime Utc(IDictionary<string, object> row, string column)
    {
        if (row[column] is not DateTime value || value.Kind == DateTimeKind.Local)
        {
            throw new WorkValidationException($"{column} must be a UTC timestamp.");
        }
        // The supplied Raw contract uses UTC, including INT96 values with no DateTimeKind metadata.
        return DateTime.SpecifyKind(value, DateTimeKind.Utc);
    }
    private static double? Number(IDictionary<string, object> row, string column) => (row.TryGetValue(column, out var raw) ? raw : null) switch
    {
        null => null,
        double value => value,
        float value => value,
        decimal value => (double)value,
        int value => value,
        long value => value,
        _ => throw new WorkValidationException($"{column} must be a numeric Parquet column, not text.")
    };
}
