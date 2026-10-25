using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using Parquet;
using Parquet.Schema;

namespace CAT.RawFanOut.Adapters.Parquet;

/// <summary>Projects only identity columns. Calculation values stay in immutable Raw files.</summary>
public sealed class ParquetIdentityReader(IRawObjectStore objects, FanOutOptions options, ITelemetry<ParquetIdentityReader>? telemetry = null) : IRawIdentityReader
{
    public async IAsyncEnumerable<RawIdentity> ReadAsync(RawEnvelope artifact,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long totalRows = 0;
        var paths = await new RawArtifactPathResolver(objects, options).ResolveAsync(artifact, cancellationToken);
        telemetry?.Information(
            $"Resolved {paths.Length} Raw files for source {artifact.SourceType}, sealed prefix {artifact.ArtifactPrefix is not null}",
            artifact.RunId, callerId: artifact.ArtifactId);
        foreach (var path in paths)
        {
            await using var input = await objects.OpenSeekableAsync(path, cancellationToken);
            await using var parquet = await ParquetReader.CreateAsync(input, cancellationToken: cancellationToken);
            var fields = parquet.Schema.GetDataFields().ToDictionary(field => field.Name, StringComparer.Ordinal);
            var layout = ResolveLayout(fields, artifact);
            for (var groupIndex = 0; groupIndex < parquet.RowGroupCount; groupIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                using var group = parquet.OpenRowGroupReader(groupIndex);
                var rowCount = group.RowCount;
                if (rowCount < 0 || rowCount > int.MaxValue || rowCount > options.MaxRowsPerRowGroup ||
                    rowCount > options.MaxRowsPerArtifact - totalRows)
                    throw new InvalidDataException("Parquet row-group or artifact limit exceeded.");
                totalRows += rowCount;
                var count = checked((int)rowCount);
                if (count == 0) continue;
                var columns = await ReadIdentityColumnsAsync(group, fields, layout, count, artifact, cancellationToken);
                for (var index = 0; index < count; index++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return ProjectIdentity(layout, columns, index, path);
                }
            }
        }
    }

    // Source/schema decisions are made once per file, before row-group reads.
    private static IdentityLayout ResolveLayout(IReadOnlyDictionary<string, DataField> fields, RawEnvelope artifact)
    {
        var production = artifact.SourceType == "Production";
        var outage = artifact.SourceType == "Outage";
        var schema = IdentitySchema.ForSource(artifact.SourceType);
        fields.TryGetValue("WindfarmId", out var farmField);
        fields.TryGetValue("WtgId", out var turbineField);
        fields.TryGetValue("ItemId", out var nameField);
        DataField? farmNameField = null;
        if (!production && farmField is null) throw new InvalidDataException("WindfarmId is required.");
        if (production)
        {
            var hasV2Name = fields.ContainsKey(ProductionIngestor.Contracts.ProductionParquetSchemaV2.WtgRdsppId);
            if (hasV2Name)
            {
                if (!fields.Keys.SequenceEqual(ProductionIngestor.Contracts.ProductionParquetSchemaV2.Columns, StringComparer.Ordinal))
                    throw new InvalidDataException("Production v2 Parquet columns do not match the declared schema.");
                nameField = fields["WtgRdsppId"];
            }
            if (artifact.PayloadSchemaVersion is { } declared && declared != (hasV2Name ? 2 : 1))
                throw new InvalidDataException("Production payload schema version does not match its Parquet columns.");
            if (turbineField is null && nameField is null) throw new InvalidDataException("Production turbine identity is required.");
            schema = new IdentitySchema(fields.ContainsKey("StartTimeInterval") ? "StartTimeInterval" : "ProductionTimestampUtc", IdentityKind.Timestamp);
        }
        if (outage)
        {
            if (turbineField is null) throw new InvalidDataException("Outage WtgId is required.");
            var numericSchema = fields.ContainsKey("WindfarmName") && fields.ContainsKey("WtgName");
            if (artifact.PayloadSchemaVersion is { } declared && declared != (numericSchema ? 2 : 1))
                throw new InvalidDataException("Outage payload schema version does not match its Parquet identity columns.");
            if (numericSchema)
            {
                if (farmField!.ClrType != typeof(int) || turbineField.ClrType != typeof(int))
                    throw new InvalidDataException("Outage schema 2 requires INT32 farm/WTG IDs and separate names.");
                farmNameField = RequireField(fields, "WindfarmName");
                nameField = RequireField(fields, "WtgName");
            }
            else if (turbineField.ClrType != typeof(int))
            {
                if (!IsText(turbineField)) throw new InvalidDataException("Legacy Outage WtgId must be a display-name string or INT32.");
                nameField = turbineField;
                turbineField = null;
            }
        }
        return new IdentityLayout(schema, RequireField(fields, schema.IdentityColumn), farmField,
            turbineField, nameField, farmNameField, production, outage, artifact.SourceType == "Adjustment");
    }

    private static async Task<IdentityColumns> ReadIdentityColumnsAsync(ParquetRowGroupReader group,
        IReadOnlyDictionary<string, DataField> fields, IdentityLayout layout, int count,
        RawEnvelope artifact, CancellationToken cancellationToken)
    {
        int?[] farms;
        string?[] farmNames = new string?[count];
        if (layout.Outage)
        {
            (farms, farmNames) = await ReadOutageFarmsAsync(group, layout.FarmField!, count, artifact, cancellationToken);
        }
        else
        {
            farms = layout.FarmField is null
                ? Enumerable.Repeat<int?>(artifact.WindfarmId, count).ToArray()
                : await ReadNumericIdsAsync(group, layout.FarmField, count, cancellationToken);
        }
        if (layout.FarmNameField is not null)
        {
            farmNames = await ReadNamesAsync(group, layout.FarmNameField, count, cancellationToken);
            if (farmNames.Any(name => !string.Equals(name, artifact.WindfarmName, StringComparison.Ordinal)))
                throw new InvalidDataException("Outage farm name differs from the envelope.");
        }
        var turbines = layout.TurbineField is null ? new int?[count]
            : await ReadNumericIdsAsync(group, layout.TurbineField, count, cancellationToken);
        var names = layout.NameField is null ? new string?[count]
            : await ReadNamesAsync(group, layout.NameField, count, cancellationToken);
        var keys = await ReadKeysAsync(group, layout.KeyField, layout.Schema, count, cancellationToken);
        if (layout.Outage) await ValidateOutageRowsAsync(group, fields, count, artifact, cancellationToken);
        if (layout.Adjustment) await ValidateAdjustmentAsync(group, fields, count, cancellationToken);
        return new IdentityColumns(farms, turbines, names, farmNames, keys);
    }

    private static RawIdentity ProjectIdentity(IdentityLayout layout, IdentityColumns columns, int index, string path)
    {
        var farmId = columns.Farms[index];
        if (farmId is null or <= 0) throw new InvalidDataException("WindfarmId must be a positive integer.");
        var turbineId = columns.Turbines[index];
        if (turbineId is <= 0) throw new InvalidDataException("WtgId must be a positive integer.");
        var turbineName = columns.Names[index];
        if (string.IsNullOrWhiteSpace(turbineName)) turbineName = null;
        if (!layout.Production && !layout.Outage && turbineName is not null &&
            int.TryParse(turbineName, NumberStyles.None, CultureInfo.InvariantCulture, out var numericId))
        {
            if (numericId <= 0) throw new InvalidDataException("Legacy turbine ID must be positive.");
            turbineId = numericId;
            turbineName = null;
        }
        if ((layout.Production || layout.Outage) && turbineId is null && turbineName is null)
            throw new InvalidDataException("Turbine identity cannot be null or empty.");
        var key = columns.Keys.GetValue(index);
        return layout.Schema.IdentityKind == IdentityKind.Timestamp
            ? new RawIdentity(farmId.Value, turbineId, null, ToProductionDate(key))
                { WtgName = turbineName, RawFarmName = columns.FarmNames[index], RawPath = path }
            : new RawIdentity(farmId.Value, turbineId, ToEventId(key), null)
                { WtgName = turbineName, RawFarmName = columns.FarmNames[index], RawPath = path };
    }

    private static DateOnly ToProductionDate(object? key) => key switch
    {
        DateTime timestamp when timestamp.Kind != DateTimeKind.Local => DateOnly.FromDateTime(timestamp),
        DateTimeOffset timestamp => DateOnly.FromDateTime(timestamp.UtcDateTime),
        _ => throw new InvalidDataException("Production identity must be a non-null UTC timestamp.")
    };

    private static string ToEventId(object? key) => key switch
    {
        long id => id.ToString(CultureInfo.InvariantCulture),
        int id => id.ToString(CultureInfo.InvariantCulture),
        string id when !string.IsNullOrWhiteSpace(id) => id,
        _ => throw new InvalidDataException("Canonical identity cannot be null or empty.")
    };

    private static bool IsText(DataField field) => field.ClrType == typeof(string) || field.ClrType == typeof(ReadOnlyMemory<char>);

    private static async Task<string?[]> ReadNamesAsync(ParquetRowGroupReader group, DataField field,
        int count, CancellationToken cancellationToken)
    {
        if (!IsText(field)) throw new InvalidDataException($"{field.Name} must be a string name.");
        var names = new string?[count];
        await group.ReadAsync(field, names.AsMemory(), null, cancellationToken);
        if ((field.Name is "WindfarmName" or "WtgName") && names.Any(string.IsNullOrWhiteSpace))
            throw new InvalidDataException($"{field.Name} cannot be null or empty.");
        return names;
    }

    private sealed record IdentityLayout(IdentitySchema Schema, DataField KeyField, DataField? FarmField,
        DataField? TurbineField, DataField? NameField, DataField? FarmNameField,
        bool Production, bool Outage, bool Adjustment);

    private sealed record IdentityColumns(int?[] Farms, int?[] Turbines, string?[] Names, string?[] FarmNames, Array Keys);

    private static async Task<(int?[] Ids, string?[] Names)> ReadOutageFarmsAsync(
        ParquetRowGroupReader group, DataField field, int count, RawEnvelope artifact, CancellationToken cancellationToken)
    {
        if (field.ClrType == typeof(int))
            return (await ReadNumericIdsAsync(group, field, count, cancellationToken), new string?[count]);
        if (field.ClrType != typeof(string) && field.ClrType != typeof(ReadOnlyMemory<char>))
            throw new InvalidDataException("Outage WindfarmId must be an INT32 or display-name string.");
        var names = new string?[count];
        await group.ReadAsync(field, names.AsMemory(), null, cancellationToken);
        if (names.Any(string.IsNullOrWhiteSpace)) throw new InvalidDataException("Outage WindfarmId cannot be null or empty.");
        if (artifact.WindfarmName is { } declaredName && names.Any(name => !string.Equals(name, declaredName, StringComparison.Ordinal)))
            throw new InvalidDataException("Outage row farm name differs from the envelope.");
        return (Enumerable.Repeat<int?>(artifact.WindfarmId, count).ToArray(), names);
    }

    private static async Task ValidateOutageRowsAsync(ParquetRowGroupReader group,
        IReadOnlyDictionary<string, DataField> fields, int count, RawEnvelope artifact, CancellationToken cancellationToken)
    {
        var deletedFeed = artifact.PayloadKind == "OutageDeletedEvents";
        if (fields.TryGetValue("IsTombstone", out var tombstoneField))
        {
            var flags = new bool?[count];
            await group.ReadAsync<bool>(tombstoneField, flags.AsMemory(), null, cancellationToken);
            if (flags.Any(flag => flag is null || flag.Value != deletedFeed))
                throw new InvalidDataException("Outage tombstone flags disagree with PayloadKind.");
        }
        var versionField = RequireField(fields, "LastUpdatedDate");
        var versions = await ReadUtcTimestampsAsync(group, versionField, count, allowNull: false, cancellationToken);
        if (versions.Any(value => value is null)) throw new InvalidDataException("Outage LastUpdatedDate cannot be null.");

        if (artifact.PayloadKind != "OutageSnapshot") return;
        var scope = artifact.EventSnapshotScope ?? throw new InvalidDataException("Outage snapshot interval scope is required.");
        var starts = await ReadUtcTimestampsAsync(group, RequireField(fields, "OriginalStartUtc"), count,
            allowNull: false, cancellationToken);
        var ends = await ReadUtcTimestampsAsync(group, RequireField(fields, "OriginalEndUtc"), count,
            allowNull: true, cancellationToken);
        for (var index = 0; index < count; index++)
        {
            var start = starts[index] ?? throw new InvalidDataException("Outage snapshot start time cannot be null.");
            var end = ends[index];
            if (start >= scope.EndExclusiveUtc || end is { } endUtc && endUtc <= scope.StartInclusiveUtc)
                throw new InvalidDataException("Outage snapshot row is outside the declared overlap interval.");
        }
    }

    private static async Task<DateTimeOffset?[]> ReadUtcTimestampsAsync(ParquetRowGroupReader group,
        DataField field, int count, bool allowNull, CancellationToken cancellationToken)
    {
        var result = new DateTimeOffset?[count];
        if (field.ClrType == typeof(DateTimeOffset))
        {
            var values = new DateTimeOffset?[count];
            await group.ReadAsync<DateTimeOffset>(field, values.AsMemory(), null, cancellationToken);
            for (var index = 0; index < count; index++)
                result[index] = values[index] is { } offset
                    ? offset.Offset == TimeSpan.Zero ? offset : throw new InvalidDataException($"{field.Name} must be UTC.")
                    : null;
        }
        else if (field.ClrType == typeof(DateTime))
        {
            var values = new DateTime?[count];
            await group.ReadAsync<DateTime>(field, values.AsMemory(), null, cancellationToken);
            for (var index = 0; index < count; index++)
                result[index] = values[index] is { } timestamp
                    ? timestamp.Kind == DateTimeKind.Local
                        ? throw new InvalidDataException($"{field.Name} must be UTC.")
                        : new DateTimeOffset(DateTime.SpecifyKind(timestamp, DateTimeKind.Utc))
                    : null;
        }
        else throw new InvalidDataException($"{field.Name} must be a UTC timestamp.");

        if (!allowNull && result.Any(value => value is null))
            throw new InvalidDataException($"{field.Name} cannot be null.");
        return result;
    }

    private static DataField RequireField(IReadOnlyDictionary<string, DataField> fields, string name) =>
        fields.TryGetValue(name, out var field) ? field : throw new InvalidDataException($"Missing Parquet column {name}.");

    private static async Task<int?[]> ReadNumericIdsAsync(ParquetRowGroupReader group, DataField field,
        int count, CancellationToken cancellationToken)
    {
        var result = new int?[count];
        if (field.ClrType == typeof(int))
        {
            await group.ReadAsync<int>(field, result.AsMemory(), null, cancellationToken);
            return result;
        }
        if (field.ClrType != typeof(string) && field.ClrType != typeof(ReadOnlyMemory<char>)) throw new InvalidDataException($"{field.Name} must be INT32 or a numeric legacy string.");
        var strings = new string?[count];
        await group.ReadAsync(field, strings.AsMemory(), null, cancellationToken);
        for (var index = 0; index < count; index++)
        {
            if (!int.TryParse(strings[index], NumberStyles.None, CultureInfo.InvariantCulture, out var id))
                throw new InvalidDataException($"{field.Name} contains a name instead of a numeric ID.");
            result[index] = id;
        }
        return result;
    }

    private static async Task<Array> ReadKeysAsync(ParquetRowGroupReader group, DataField field,
        IdentitySchema schema, int count, CancellationToken cancellationToken)
    {
        switch (schema.IdentityKind)
        {
            case IdentityKind.Timestamp:
                if (field.ClrType == typeof(DateTimeOffset))
                {
                    var offsets = new DateTimeOffset?[count];
                    await group.ReadAsync<DateTimeOffset>(field, offsets.AsMemory(), null, cancellationToken);
                    return offsets;
                }
                var timestamps = new DateTime?[count];
                await group.ReadAsync<DateTime>(field, timestamps.AsMemory(), null, cancellationToken);
                return timestamps;
            case IdentityKind.Int64:
                var longs = new long?[count];
                await group.ReadAsync<long>(field, longs.AsMemory(), null, cancellationToken);
                return longs;
            case IdentityKind.Int32:
                return await ReadNumericIdsAsync(group, field, count, cancellationToken);
            default:
                var strings = new string?[count];
                await group.ReadAsync(field, strings.AsMemory(), null, cancellationToken);
                return strings;
        }
    }

    private static async Task ValidateAdjustmentAsync(ParquetRowGroupReader group,
        IReadOnlyDictionary<string, DataField> fields, int count, CancellationToken cancellationToken)
    {
        var revisions = new long?[count];
        await group.ReadAsync<long>(RequireField(fields, "AdjustmentRevision"), revisions.AsMemory(), null, cancellationToken);
        var tombstones = new bool?[count];
        await group.ReadAsync<bool>(RequireField(fields, "IsTombstone"), tombstones.AsMemory(), null, cancellationToken);
        if (revisions.Any(revision => revision is null or <= 0) || tombstones.Any(value => value is null))
            throw new InvalidDataException("Adjustment revision and tombstone fields are required.");
        // Full parent and membership data are consumed by Harmonizer. Separate heterogeneous
        // parent/membership shards are not treated as interchangeable identity files.
        RequireField(fields, "ItemIdsJson");
        RequireField(fields, "PreviousItemIdsJson");
        RequireField(fields, "ParentJson");
    }
}

internal enum IdentityKind { Timestamp, Int64, Int32, Text }
internal sealed record IdentitySchema(string IdentityColumn, IdentityKind IdentityKind)
{
    public static IdentitySchema ForSource(string sourceType) => sourceType switch
    {
        "Production" => new("StartTimeInterval", IdentityKind.Timestamp),
        "Alarm" => new("AlarmId", IdentityKind.Int64),
        "Outage" or "Curtailment" => new("EventId", IdentityKind.Text),
        "Adjustment" => new("AdjustmentId", IdentityKind.Int32),
        _ => throw new InvalidDataException("Unsupported Raw Parquet schema.")
    };
}
