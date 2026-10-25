using Parquet;
using Parquet.Schema;
using CurtailmentIngestor.Interfaces;
using CurtailmentIngestor.Models;
using CurtailmentIngestor.Raw;

namespace CurtailmentIngestor.Serialization;

/// <summary>
/// Writes an immutable, append-only Raw Parquet batch for one extraction run: validates/quarantines
/// events, shards deterministically by EventId when the batch exceeds the configured size
/// limit, uploads via <see cref="IRawObjectStore"/>, and verifies the write is durable. Raw
/// event batches are never rewritten (Section 4.3.1) — there is no per-day re-read here.
/// Adjacent-interval merging is deliberately not performed here (Section 6.8 places that
/// aggregation in the Harmonized zone); this writer extracts raw events verbatim.
/// </summary>
public sealed class CurtailmentBatchWriter
{
    private readonly IRawObjectStore _store;
    private readonly int _maxEventsPerShard;
    private readonly int _rowGroupSize;

    public CurtailmentBatchWriter(IRawObjectStore store, int maxEventsPerShard = 50_000, int rowGroupSize = 50_000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEventsPerShard);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(rowGroupSize);
        _store = store;
        _maxEventsPerShard = maxEventsPerShard;
        _rowGroupSize = rowGroupSize;
    }

    /// <summary>
    /// Validates and writes the given events (already-discovered changed batch). Events that
    /// fail validation are excluded from the artifact and reported in the quarantine artifact/result.
    /// </summary>
    public async Task<RawWriteResultModel> WriteBatchAsync(
        string windfarmId,
        long extractionToken,
        IReadOnlyList<CurtailmentEventModel> events,
        CancellationToken cancellationToken)
    {
        var (validEvents, quarantined) = Validate(events);

        var shards = DetermineShards(validEvents, _maxEventsPerShard);

        if (shards.Count <= 1)
        {
            var path = RawPathBuilder.BuildSingleArtifactPath(windfarmId, extractionToken);
            var bytes = await SerializeAsync(
                shards.Count == 1 ? shards[0] : Array.Empty<CurtailmentEventModel>(), extractionToken, cancellationToken);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyDurableAsync(path, cancellationToken);

            if (quarantined.Count > 0)
            {
                await WriteQuarantineAsync(windfarmId, extractionToken, quarantined, cancellationToken);
            }

            return new RawWriteResultModel(windfarmId, extractionToken, path, null, validEvents.Count, quarantined);
        }

        var shardPaths = new List<string>();
        for (var i = 0; i < shards.Count; i++)
        {
            var path = RawPathBuilder.BuildShardPath(windfarmId, extractionToken, i + 1);
            var bytes = await SerializeAsync(shards[i], extractionToken, cancellationToken);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyDurableAsync(path, cancellationToken);
            shardPaths.Add(path);
        }

        if (quarantined.Count > 0)
        {
            await WriteQuarantineAsync(windfarmId, extractionToken, quarantined, cancellationToken);
        }

        return new RawWriteResultModel(windfarmId, extractionToken, null, shardPaths, validEvents.Count, quarantined);
    }

    /// <summary>
    /// Deterministically shards a batch by EventId so that a retry inside one run rewrites
    /// identical content at identical paths.
    /// </summary>
    public static IReadOnlyList<IReadOnlyList<CurtailmentEventModel>> DetermineShards(
        IReadOnlyList<CurtailmentEventModel> events, int maxEventsPerShard)
    {
        if (events.Count == 0)
        {
            return Array.Empty<IReadOnlyList<CurtailmentEventModel>>();
        }

        var ordered = events.OrderBy(e => e.EventId, StringComparer.Ordinal).ToList();

        if (ordered.Count <= maxEventsPerShard)
        {
            return new[] { (IReadOnlyList<CurtailmentEventModel>)ordered };
        }

        var shards = new List<IReadOnlyList<CurtailmentEventModel>>();
        for (var i = 0; i < ordered.Count; i += maxEventsPerShard)
        {
            shards.Add(ordered.Skip(i).Take(maxEventsPerShard).ToList());
        }

        return shards;
    }

    /// <summary>
    /// Rejects events with a missing identity or malformed start time, tagging each with a
    /// reason. Tombstones skip the start/end validation since they only assert deletion.
    /// </summary>
    internal static (List<CurtailmentEventModel> Valid, List<QuarantinedEventModel> Quarantined) Validate(
        IReadOnlyList<CurtailmentEventModel> events)
    {
        var valid = new List<CurtailmentEventModel>();
        var quarantined = new List<QuarantinedEventModel>();

        foreach (var evt in events)
        {
            if (string.IsNullOrWhiteSpace(evt.EventId))
            {
                quarantined.Add(new QuarantinedEventModel(evt.EventId ?? string.Empty, "missing EventId"));
                continue;
            }

            if (evt.IsTombstone)
            {
                valid.Add(evt);
                continue;
            }

            if (evt.OriginalStartUtc == default)
            {
                quarantined.Add(new QuarantinedEventModel(evt.EventId, "malformed/missing OriginalStartUtc"));
                continue;
            }

            if (evt.OriginalEndUtc is { } end && end < evt.OriginalStartUtc)
            {
                quarantined.Add(new QuarantinedEventModel(evt.EventId, "OriginalEndUtc precedes OriginalStartUtc"));
                continue;
            }

            valid.Add(evt);
        }

        return (valid, quarantined);
    }

    private async Task<byte[]> SerializeAsync(
        IReadOnlyList<CurtailmentEventModel> events, long extractionToken, CancellationToken cancellationToken)
    {
        var fields = new DataField[]
        {
            new DataField<string>("EventId"), new DataField<string>("WindfarmId"),
            new DataField<string>("ItemId"), new DataField<string>("EventType"),
            new DateTimeDataField("OriginalStartUtc", DateTimeFormat.Impala),
            new DateTimeDataField("OriginalEndUtc", DateTimeFormat.Impala, isNullable: true),
            new DataField<string>("Status"), new DataField<string>("CurtailmentTypeId"),
            new DataField<string>("SourceRevision"),
            new DateTimeDataField("SourceWatermarkUtc", DateTimeFormat.Impala),
            new DataField<bool>("IsTombstone"),
            new DateTimeDataField("LastUpdatedDate", DateTimeFormat.Impala),
            new DataField<long>("Id"), new DataField<long>("ExtractionToken"),
            new DataField<int>("SchemaVersion")
        };

        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            foreach (var batch in events.Chunk(_rowGroupSize))
            {
                using var group = writer.CreateRowGroup();
                Array[] columns =
                [
                    batch.Select(evt => evt.EventId).ToArray(), batch.Select(evt => evt.WindfarmId).ToArray(),
                    batch.Select(evt => evt.ItemId).ToArray(), batch.Select(evt => evt.EventType).ToArray(),
                    batch.Select(evt => evt.OriginalStartUtc).ToArray(),
                    batch.Select(evt => evt.OriginalEndUtc).ToArray(),
                    batch.Select(evt => evt.Status).ToArray(), batch.Select(evt => evt.CurtailmentTypeId).ToArray(),
                    batch.Select(evt => evt.SourceRevision).ToArray(),
                    batch.Select(evt => evt.SourceWatermarkUtc).ToArray(),
                    batch.Select(evt => evt.IsTombstone).ToArray(),
                    batch.Select(evt => evt.LastUpdatedDate).ToArray(),
                    batch.Select(evt => evt.Id).ToArray(),
                    Enumerable.Repeat(extractionToken, batch.Length).ToArray(),
                    Enumerable.Repeat(1, batch.Length).ToArray()
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
            DateTime?[] nullableTimestamps => group.WriteAsync<DateTime>(field, nullableTimestamps, cancellationToken: cancellationToken),
            DateTime[] timestamps => group.WriteAsync<DateTime>(field, timestamps, cancellationToken: cancellationToken),
            bool[] flags => group.WriteAsync<bool>(field, flags, cancellationToken: cancellationToken),
            long[] numbers => group.WriteAsync<long>(field, numbers, cancellationToken: cancellationToken),
            int[] versions => group.WriteAsync<int>(field, versions, cancellationToken: cancellationToken),
            _ => throw new InvalidOperationException($"Unsupported Parquet column type: {values.GetType()}.")
        };
    }

    private async Task VerifyDurableAsync(string path, CancellationToken cancellationToken)
    {
        var exists = await _store.ExistsAsync(path, cancellationToken);
        if (!exists)
        {
            throw new InvalidOperationException($"Raw artifact write at '{path}' could not be verified as durable.");
        }
    }

    private async Task WriteQuarantineAsync(
        string windfarmId, long extractionToken,
        IReadOnlyList<QuarantinedEventModel> quarantined, CancellationToken cancellationToken)
    {
        var path = RawPathBuilder.BuildQuarantinePath(windfarmId, extractionToken);
        var fields = new DataField[] { new DataField<string>("EventId"), new DataField<string>("Reason") };

        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            foreach (var batch in quarantined.Chunk(_rowGroupSize))
            {
                using var group = writer.CreateRowGroup();
                await WriteColumnAsync(group, fields[0], batch.Select(row => row.EventId).ToArray(), cancellationToken);
                await WriteColumnAsync(group, fields[1], batch.Select(row => row.Reason).ToArray(), cancellationToken);
            }
        }

        await _store.UploadAsync(path, stream.ToArray(), cancellationToken);
        await VerifyDurableAsync(path, cancellationToken);
    }
}
