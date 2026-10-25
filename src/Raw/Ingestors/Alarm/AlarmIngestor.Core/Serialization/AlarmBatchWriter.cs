using Parquet;
using Parquet.Schema;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;
using AlarmIngestor.Raw;

namespace AlarmIngestor.Serialization;

/// <summary>
/// Writes an immutable, append-only Raw batch for one bounded window (Discovery, Polling or
/// Historical). Raw ends are preserved unmodified — no duration cap is applied here. Batches
/// larger than the configured limit are sharded deterministically by AlarmId. Any quarantined
/// row fails the entire batch closed (quarantine audit written, then throws) before any valid
/// Raw artifact is published — a partial success would let a checkpoint/poll-removal skip past
/// an unresolved row.
/// </summary>
public sealed class AlarmBatchWriter
{
    private readonly IRawObjectStore _store;
    private readonly int _maxEventsPerShard;

    public AlarmBatchWriter(IRawObjectStore store, int maxEventsPerShard = 50_000)
    {
        _store = store;
        _maxEventsPerShard = maxEventsPerShard;
    }

    public async Task<RawWriteResultModel> WriteBatchAsync(
        string windfarmId,
        long extractionToken,
        int windowSequence,
        IReadOnlyList<AlarmEventModel> events,
        CancellationToken cancellationToken)
    {
        var (validEvents, quarantined) = Validate(events);

        if (quarantined.Count > 0)
        {
            // Fail the whole batch closed BEFORE any valid Raw publication: a partial-success
            // return here is exactly what let a cursor/poll checkpoint past an unresolved
            // quarantined row (e.g. lower AlarmId invalid, higher AlarmId valid). Audit the
            // quarantine for diagnosis, then throw — never return a result for this batch.
            await WriteQuarantineAsync(windfarmId, extractionToken, windowSequence, quarantined, cancellationToken);
            throw new InvalidDataException(
                $"Batch for windfarm '{windfarmId}' window {windowSequence} contains {quarantined.Count} quarantined row(s); failing closed with no Raw publication or checkpoint advance.");
        }

        var shards = DetermineShards(validEvents, _maxEventsPerShard);

        if (shards.Count <= 1)
        {
            var path = RawPathBuilder.BuildSingleArtifactPath(windfarmId, extractionToken, windowSequence);
            var bytes = await SerializeAsync(shards.Count == 1 ? shards[0] : [], cancellationToken);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyDurableAsync(path, cancellationToken);

            return new RawWriteResultModel(windfarmId, extractionToken, path, null, validEvents.Count, quarantined, validEvents);
        }

        var shardPaths = new List<string>();
        for (var i = 0; i < shards.Count; i++)
        {
            var path = RawPathBuilder.BuildShardPath(windfarmId, extractionToken, windowSequence, i + 1);
            var bytes = await SerializeAsync(shards[i], cancellationToken);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyDurableAsync(path, cancellationToken);
            shardPaths.Add(path);
        }

        return new RawWriteResultModel(windfarmId, extractionToken, null, shardPaths, validEvents.Count, quarantined, validEvents);
    }

    public static IReadOnlyList<IReadOnlyList<AlarmEventModel>> DetermineShards(
        IReadOnlyList<AlarmEventModel> events, int maxEventsPerShard)
    {
        if (events.Count == 0)
        {
            return [];
        }

        var ordered = events.OrderBy(e => e.TurbineRdspp, StringComparer.Ordinal).ThenBy(e => e.AlarmId).ToList();

        if (ordered.Count <= maxEventsPerShard)
        {
            return [ordered];
        }

        var shards = new List<IReadOnlyList<AlarmEventModel>>();
        for (var i = 0; i < ordered.Count; i += maxEventsPerShard)
        {
            shards.Add(ordered.Skip(i).Take(maxEventsPerShard).ToList());
        }

        return shards;
    }

    internal static (List<AlarmEventModel> Valid, List<QuarantinedEventModel> Quarantined) Validate(
        IReadOnlyList<AlarmEventModel> events)
    {
        var valid = new List<AlarmEventModel>();
        var quarantined = new List<QuarantinedEventModel>();

        foreach (var evt in events)
        {
            if (string.IsNullOrWhiteSpace(evt.TurbineRdspp) || evt.AlarmId <= 0)
            {
                quarantined.Add(new QuarantinedEventModel(evt.AlarmId.ToString(), "missing turbine identity or AlarmId"));
                continue;
            }

            if (evt.StartTimeUtc == default)
            {
                quarantined.Add(new QuarantinedEventModel(evt.AlarmId.ToString(), "malformed/missing StartTimeUtc"));
                continue;
            }

            if (evt.EndTimeUtc is { } end && end < evt.StartTimeUtc)
            {
                quarantined.Add(new QuarantinedEventModel(evt.AlarmId.ToString(), "EndTimeUtc precedes StartTimeUtc"));
                continue;
            }

            valid.Add(evt);
        }

        return (valid, quarantined);
    }

    private static async Task<byte[]> SerializeAsync(IReadOnlyList<AlarmEventModel> events, CancellationToken cancellationToken)
    {
        var fields = new DataField[]
        {
            new DataField<string>("WindfarmId"),
            new DataField<string>("TurbineRdspp"),
            new DataField<string>("ItemId"),
            new DataField<long>("AlarmId"),
            new DataField<string>("AlarmCode"),
            new DataField<string>("AlarmText"),
            new DateTimeDataField("StartTimeUtc", DateTimeFormat.Impala),
            new DateTimeDataField("EndTimeUtc", DateTimeFormat.Impala, isNullable: true),
            new DataField<string>("AlarmType"),
            new DataField<string>("AlarmState"),
            new DataField<bool>("IsTombstone"),
            new DateTimeDataField("ObservedAtUtc", DateTimeFormat.Impala),
            new DataField<int>("SequenceInRun"),
        };

        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync(fields[0], events.Select(e => e.WindfarmId).ToArray());
            await group.WriteAsync(fields[1], events.Select(e => e.TurbineRdspp).ToArray());
            await group.WriteAsync(fields[2], events.Select(e => e.ItemId).ToArray());
            await group.WriteAsync<long>(fields[3], events.Select(e => e.AlarmId).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync(fields[4], events.Select(e => e.AlarmCode).ToArray());
            await group.WriteAsync(fields[5], events.Select(e => e.AlarmText).ToArray());
            await group.WriteAsync<DateTime>(fields[6], events.Select(e => e.StartTimeUtc).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync<DateTime>(fields[7], events.Select(e => e.EndTimeUtc).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync(fields[8], events.Select(e => e.AlarmType).ToArray());
            await group.WriteAsync(fields[9], events.Select(e => e.AlarmState).ToArray());
            await group.WriteAsync<bool>(fields[10], events.Select(e => e.IsTombstone).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync<DateTime>(fields[11], events.Select(e => e.ObservedAtUtc).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync<int>(fields[12], events.Select(e => e.SequenceInRun).ToArray(), cancellationToken: cancellationToken);
        }
        return stream.ToArray();
    }

    private async Task WriteQuarantineAsync(
        string windfarmId, long extractionToken, int windowSequence,
        IReadOnlyList<QuarantinedEventModel> quarantined, CancellationToken cancellationToken)
    {
        var fields = new DataField[] { new DataField<string>("AlarmId"), new DataField<string>("Reason") };
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync(fields[0], quarantined.Select(q => q.AlarmId).ToArray());
            await group.WriteAsync(fields[1], quarantined.Select(q => q.Reason).ToArray());
        }

        var path = RawPathBuilder.BuildQuarantinePath(windfarmId, extractionToken, windowSequence);
        await _store.UploadAsync(path, stream.ToArray(), cancellationToken);
    }

    private async Task VerifyDurableAsync(string path, CancellationToken cancellationToken)
    {
        var exists = await _store.ExistsAsync(path, cancellationToken);
        if (!exists)
        {
            throw new InvalidOperationException($"Raw artifact write at '{path}' could not be verified as durable.");
        }
    }
}
