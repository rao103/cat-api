using CAT.Messaging.Contracts;
using Parquet;
using Parquet.Schema;
using OutageIngestor.Enums;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Raw;

namespace OutageIngestor.Serialization;

/// <summary>
/// Writes an immutable, append-only Raw batch for one extraction run: validates/quarantines
/// events, shards deterministically by EventId when the batch exceeds the configured size
/// limit, uploads via <see cref="IRawObjectStore"/>, and verifies the write is durable. Raw
/// event batches are never rewritten (Section 4.3.1) — there is no per-day re-read here.
/// </summary>
public sealed class OutageBatchWriter
{
    private readonly IRawObjectStore _store;
    private readonly int _maxEventsPerShard;

    public OutageBatchWriter(IRawObjectStore store, int maxEventsPerShard = 50_000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEventsPerShard);
        _store = store;
        _maxEventsPerShard = maxEventsPerShard;
    }

    /// <summary>
    /// Validates and writes the given events (already-discovered changed batch). Live and
    /// deleted (tombstone) events are two separate source feeds (Outage source design) and are
    /// written as two separate Raw artifacts under `events/` and `deleted-events/` rather than
    /// merged into one file, so Raw stays close to what each source table actually supplied.
    /// Events that fail validation are excluded from either artifact and reported in the
    /// quarantine artifact/result. <paramref name="windowStartUtc"/>/<paramref name="windowEndUtc"/>
    /// (the catch-up window's own scope) distinguish multiple windows written under the same
    /// lease token within one run so their artifacts never collide/overwrite each other.
    /// </summary>
    public async Task<RawWriteResultModel> WriteBatchAsync(
        string windfarmName,
        long extractionToken,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        IReadOnlyList<OutageEventModel> events,
        CancellationToken cancellationToken)
    {
        RequireResolvedIdentities(events);
        var (validEvents, quarantined) = Validate(events);
        var liveEvents = validEvents.Where(e => !e.IsTombstone).ToList();
        var deletedEvents = validEvents.Where(e => e.IsTombstone).ToList();

        var (eventsBlobPath, eventsShardPaths, eventCounts) = await WriteFeedAsync(
            windfarmName, extractionToken, windowStartUtc, windowEndUtc, liveEvents, isDeletedFeed: false, cancellationToken);
        var (deletedBlobPath, deletedShardPaths, deletedCounts) = await WriteFeedAsync(
            windfarmName, extractionToken, windowStartUtc, windowEndUtc, deletedEvents, isDeletedFeed: true, cancellationToken);

        if (quarantined.Count > 0)
        {
            await WriteQuarantineAsync(windfarmName, extractionToken, windowStartUtc, windowEndUtc, quarantined, IngestionMode.Incremental, cancellationToken);
        }

        return new RawWriteResultModel(
            windfarmName, extractionToken, eventsBlobPath, eventsShardPaths, liveEvents.Count, quarantined,
            deletedBlobPath, deletedShardPaths, deletedEvents.Count, eventCounts, deletedCounts);
    }

    /// <summary>
    /// Writes one feed (live events or tombstones) as a single artifact or deterministic shard
    /// set, matching the existing sharding/verification behaviour. Writes nothing and returns
    /// null paths when the feed has no valid rows this batch — there is no artifact to point
    /// downstream at, and an empty placeholder file would be indistinguishable from "not yet
    /// processed" without also carrying a distinguishing signal.
    /// </summary>
    private async Task<(string? BlobPath, IReadOnlyList<string>? ShardPaths, IReadOnlyList<int> PartRowCounts)> WriteFeedAsync(
        string windfarmName, long extractionToken, DateTime windowStartUtc, DateTime windowEndUtc,
        IReadOnlyList<OutageEventModel> events, bool isDeletedFeed, CancellationToken cancellationToken)
    {
        if (events.Count == 0)
        {
            return (null, null, Array.Empty<int>());
        }

        var shards = DetermineShards(events, _maxEventsPerShard);

        if (shards.Count <= 1)
        {
            var path = isDeletedFeed
                ? RawPathBuilder.BuildDeletedEventsSingleArtifactPath(windfarmName, extractionToken, windowStartUtc, windowEndUtc)
                : RawPathBuilder.BuildEventsSingleArtifactPath(windfarmName, extractionToken, windowStartUtc, windowEndUtc);
            var bytes = await SerializeAsync(shards[0], cancellationToken);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyDurableAsync(path, cancellationToken);
            return (path, null, new[] { shards[0].Count });
        }

        var shardPaths = new List<string>();
        for (var i = 0; i < shards.Count; i++)
        {
            var path = isDeletedFeed
                ? RawPathBuilder.BuildDeletedEventsShardPath(windfarmName, extractionToken, windowStartUtc, windowEndUtc, i + 1)
                : RawPathBuilder.BuildEventsShardPath(windfarmName, extractionToken, windowStartUtc, windowEndUtc, i + 1);
            var bytes = await SerializeAsync(shards[i], cancellationToken);
            await _store.UploadAsync(path, bytes, cancellationToken);
            await VerifyDurableAsync(path, cancellationToken);
            shardPaths.Add(path);
        }

        return (null, shardPaths, shards.Select(shard => shard.Count).ToArray());
    }

    /// <summary>
    /// Deterministically shards a batch by EventId so that a retry before cursor advance
    /// rewrites identical shard contents at identical paths.
    /// </summary>
    public static IReadOnlyList<IReadOnlyList<OutageEventModel>> DetermineShards(
        IReadOnlyList<OutageEventModel> events, int maxEventsPerShard)
    {
        if (events.Count == 0)
        {
            return Array.Empty<IReadOnlyList<OutageEventModel>>();
        }

        var ordered = events.OrderBy(e => e.EventId, StringComparer.Ordinal).ToList();

        if (ordered.Count <= maxEventsPerShard)
        {
            return new[] { (IReadOnlyList<OutageEventModel>)ordered };
        }

        var shards = new List<IReadOnlyList<OutageEventModel>>();
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
    internal static (List<OutageEventModel> Valid, List<QuarantinedEventModel> Quarantined) Validate(
        IReadOnlyList<OutageEventModel> events)
    {
        var valid = new List<OutageEventModel>();
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

    private static void RequireResolvedIdentities(IReadOnlyList<OutageEventModel> events)
    {
        if (events.Any(e => e.WindfarmId <= 0 || e.WtgId <= 0 || string.IsNullOrWhiteSpace(e.WindfarmName) || string.IsNullOrWhiteSpace(e.WtgName)))
            throw new InvalidDataException("Resolve numeric farm/WTG IDs and names before writing Raw; do not quarantine unresolved identities.");
    }

    private static async Task<byte[]> SerializeAsync(IReadOnlyList<OutageEventModel> events, CancellationToken cancellationToken)
    {
        RequireResolvedIdentities(events);
        var fields = new DataField[]
        {
            new DataField<string>("EventId"), new DataField<int>("WindfarmId"), new DataField<int>("WtgId"),
            new DataField<string>("EventType"),
            new DateTimeDataField("OriginalStartUtc", DateTimeFormat.Impala),
            new DateTimeDataField("OriginalEndUtc", DateTimeFormat.Impala, isNullable: true),
            new DataField<string>("Status"),
            new DataField<string>("SourceRevision"),
            new DateTimeDataField("SourceWatermarkUtc", DateTimeFormat.Impala),
            new DataField<bool>("IsTombstone"),
            new DateTimeDataField("LastUpdatedDate", DateTimeFormat.Impala),
            new DataField<string>("OutageId"),
            new DataField<string>("Comment"),
            new DataField<string>("AlarmCode"),
            new DataField<string>("AlarmText"),
            new DataField<string>("Category"),
            new DataField<string>("SubCategory"),
            new DataField<string>("WindfarmName"),
			new DataField<string>("WtgName"),
        };

        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync(fields[0], events.Select(e => e.EventId).ToArray());
            await group.WriteAsync<int>(fields[1], events.Select(e => e.WindfarmId).ToArray());
            await group.WriteAsync<int>(fields[2], events.Select(e => e.WtgId).ToArray());
            await group.WriteAsync(fields[3], events.Select(e => e.EventType).ToArray());
            await group.WriteAsync<DateTime>(fields[4], events.Select(e => e.OriginalStartUtc).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync<DateTime>(fields[5], events.Select(e => e.OriginalEndUtc).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync(fields[6], events.Select(e => e.Status).ToArray());
            await group.WriteAsync(fields[7], events.Select(e => e.SourceRevision).ToArray());
            await group.WriteAsync<DateTime>(fields[8], events.Select(e => e.SourceWatermarkUtc).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync<bool>(fields[9], events.Select(e => e.IsTombstone).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync<DateTime>(fields[10], events.Select(e => e.LastUpdatedDate).ToArray(), cancellationToken: cancellationToken);
            await group.WriteAsync(fields[11], events.Select(e => e.OutageId).ToArray());
            await group.WriteAsync(fields[12], events.Select(e => e.Comment).ToArray());
            await group.WriteAsync(fields[13], events.Select(e => e.AlarmCode).ToArray());
            await group.WriteAsync(fields[14], events.Select(e => e.AlarmText).ToArray());
            await group.WriteAsync(fields[15], events.Select(e => e.Category).ToArray());
            await group.WriteAsync(fields[16], events.Select(e => e.SubCategory).ToArray());
            await group.WriteAsync(fields[17], events.Select(e => e.WindfarmName).ToArray());
            await group.WriteAsync(fields[18], events.Select(e => e.WtgName).ToArray());
        }
        return stream.ToArray();
    }

    /// <summary>
    /// Writes one scoped Refresh snapshot (Incremental Refresh Spec, Sections 9-10): validates
    /// events, buffers at most one part from the source's stable keyset order, uploads every part
    /// under `snapshot/`, then uploads `snapshot/manifest.json` only after every part is verified
    /// durable, listing all part paths, the requested scope and the resulting record count. A
    /// scope with zero source events still uploads a manifest with an empty part list and
    /// <c>RecordCount = 0</c> rather than skipping the manifest \u2014 a valid empty snapshot is a
    /// legitimate outcome, not the absence of one.
    /// </summary>
    public async Task<RawSnapshotWriteResultModel> WriteSnapshotAsync(
        int windfarmId,
        string windfarmName,
        long extractionToken,
        DateTime scopeStartUtc,
        DateTime scopeEndUtc,
        string runId,
        IAsyncEnumerable<OutageEventModel> events,
        IReadOnlyList<string> scopeWtgNames, DateTime minRetrieveDateUtc,
        CancellationToken cancellationToken, string? operationId = null)
    {
        // This is an observation time, not a source-issued revision. Capture it before reading,
        // never after uploads (which would incorrectly make an older read appear newer).
        var snapshotAsOfUtc = DateTime.UtcNow;
        var buffer = new List<OutageEventModel>();
        var partPaths = new List<string>();
        var parts = new List<RawSnapshotPartV1>();
        var recordCount = 0;
        await foreach (var evt in events.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            buffer.Add(evt);
            if (buffer.Count == _maxEventsPerShard)
            {
                partPaths.Add(await WriteSnapshotPartAsync(windfarmName, extractionToken,
                    scopeStartUtc, scopeEndUtc, partPaths.Count + 1, buffer, cancellationToken));
                parts.Add(new RawSnapshotPartV1(partPaths[^1], 1, partPaths.Count, buffer.Count));
                recordCount = checked(recordCount + buffer.Count);
                buffer.Clear();
            }
        }
        if (buffer.Count > 0)
        {
            partPaths.Add(await WriteSnapshotPartAsync(windfarmName, extractionToken,
                scopeStartUtc, scopeEndUtc, partPaths.Count + 1, buffer, cancellationToken));
            parts.Add(new RawSnapshotPartV1(partPaths[^1], 1, partPaths.Count, buffer.Count));
            recordCount = checked(recordCount + buffer.Count);
        }

        cancellationToken.ThrowIfCancellationRequested();
        var manifestPath = RawPathBuilder.BuildSnapshotManifestPath(windfarmName, extractionToken);
        var manifest = new RawSnapshotManifestV1
        {
            Envelope = new RawArtifactAvailableV2
            {
                SourceType = "Outage", WindfarmId = windfarmId, WindfarmName = windfarmName,
                RunId = runId, OperationId = operationId, ExtractionToken = extractionToken, AttemptId = RawArtifactConventions.AttemptId(extractionToken),
                PayloadSchemaVersion = RawArtifactConventions.OutagePayloadSchemaVersion,
                ExtractionMode = "Refresh", PayloadKind = "OutageSnapshot", QueryAxis = RawArtifactConventions.EventOverlapUtc,
                QueryStartInclusiveUtc = scopeStartUtc, QueryEndExclusiveUtc = scopeEndUtc,
                SnapshotScope = new RawSnapshotScopeV2(RawArtifactConventions.EventsOverlappingInterval, scopeStartUtc, scopeEndUtc),
                ManifestPath = manifestPath, ManifestSchemaVersion = RawSnapshotManifestV1.CurrentSchemaVersion,
                RowCount = recordCount,
                TraceId = System.Diagnostics.Activity.Current?.TraceId.ToString(),
            },
            SnapshotAsOfUtc = snapshotAsOfUtc,
            RequestedIntervals = [new RawSnapshotIntervalV1(scopeStartUtc, scopeEndUtc)],
            ScopeWtgNames = scopeWtgNames.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray(),
            MinRetrieveDateUtc = minRetrieveDateUtc,
            CompletedWindows = [new RawSnapshotWindowV1(1, scopeStartUtc, scopeEndUtc, recordCount, parts.Count, true)],
            Parts = parts,
        };
        manifest.Validate();
        var manifestBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(manifest);
        await _store.UploadAsync(manifestPath, manifestBytes, cancellationToken);
        await VerifyDurableAsync(manifestPath, cancellationToken);

        return new RawSnapshotWriteResultModel(windfarmName, extractionToken, manifestPath, partPaths, recordCount, []);
    }

    private async Task<string> WriteSnapshotPartAsync(
        string windfarmName, long extractionToken, DateTime scopeStartUtc, DateTime scopeEndUtc,
        int partNumber, IReadOnlyList<OutageEventModel> events, CancellationToken cancellationToken)
    {
        var (_, quarantined) = Validate(events);
        // Dropping even one row from a complete snapshot could falsely delete it downstream.
        // Unlike incremental quarantine, an invalid refresh must fail without a manifest.
        if (quarantined.Count > 0 || events.Any(evt => evt.IsTombstone))
            throw new InvalidOperationException("An authoritative outage snapshot must contain only valid live events; no manifest will be published.");

        var path = RawPathBuilder.BuildSnapshotPartPath(windfarmName, extractionToken, scopeStartUtc, scopeEndUtc, partNumber);
        var bytes = await SerializeAsync(events, cancellationToken);
        await _store.UploadAsync(path, bytes, cancellationToken);
        await VerifyDurableAsync(path, cancellationToken);
        return path;
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
        string windfarmName, long extractionToken, DateTime windowStartUtc, DateTime windowEndUtc,
        IReadOnlyList<QuarantinedEventModel> quarantined, IngestionMode mode, CancellationToken cancellationToken)
    {
        var path = RawPathBuilder.BuildQuarantinePath(windfarmName, extractionToken, windowStartUtc, windowEndUtc, mode);
        var fields = new DataField[] { new DataField<string>("EventId"), new DataField<string>("Reason") };
        using var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: cancellationToken))
        {
            using var group = writer.CreateRowGroup();
            await group.WriteAsync(fields[0], quarantined.Select(q => q.EventId).ToArray());
            await group.WriteAsync(fields[1], quarantined.Select(q => q.Reason).ToArray());
        }
        var bytes = stream.ToArray();
        await _store.UploadAsync(path, bytes, cancellationToken);
    }
}
