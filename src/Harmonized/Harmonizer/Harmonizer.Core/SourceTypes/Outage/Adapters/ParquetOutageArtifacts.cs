using System.Security.Cryptography;
using System.Text.Json;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Harmonizer.Core.SourceTypes.Outage.Operations;
using Microsoft.Extensions.Options;
using Parquet;
using Parquet.Serialization;

namespace Harmonizer.Core.SourceTypes.Outage.Adapters;

public sealed class ParquetOutageArtifacts(IRawObjectStore raw, IHarmonizedArtifactStore output,
    NormalizedOutageRowMapper mapper, OutageMergeService merger, TimeProvider clock,
    IOptions<HarmonizerOptions> options, ITelemetry<ParquetOutageArtifacts> telemetry) : IOutageArtifacts
{
    private static readonly string[] RequiredColumns = ["EventId", "WindfarmId", "WtgId", "OriginalStartUtc",
        "OriginalEndUtc", "IsTombstone", "LastUpdatedDate", "SourceWatermarkUtc"];

    public async Task<IReadOnlyList<CanonicalOutage>> ReadRawAsync(OutageWork work, CancellationToken cancellationToken)
    {
        var selected = new Dictionary<string, CanonicalOutage>(StringComparer.Ordinal);
        var seenMembers = new HashSet<string>(StringComparer.Ordinal);
        long totalRows = 0;
        foreach (var path in work.Message.RawPaths)
        {
            await using var stream = await raw.OpenReadAsync(path, cancellationToken);
            CheckStream(stream);
            int groups;
            string[] columns;
            await using (var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: cancellationToken))
            {
                if (RequiredColumns.Any(column => !reader.Schema.GetDataFields().Any(field => field.Name == column)) ||
                    reader.RowGroups.Sum(group => group.RowCount) > options.Value.MaximumRowsPerArtifact)
                    throw new WorkValidationException("Raw Outage schema is incomplete or exceeds the row bound.");
                var fields = reader.Schema.GetDataFields().ToDictionary(field => field.Name, StringComparer.Ordinal);
                var numeric = fields.ContainsKey("WindfarmName") && fields.ContainsKey("WtgName");
                if ((work.Message.PayloadSchemaVersion is { } schema && schema != (numeric ? 2 : 1)) ||
                    (numeric && (fields["WindfarmId"].ClrType != typeof(int) || fields["WtgId"].ClrType != typeof(int))))
                    throw new WorkValidationException("Outage payload version and identity columns disagree.");
                groups = reader.RowGroupCount;
                columns = reader.Schema.GetDataFields().Select(field => field.Name).ToArray();
            }
            for (var group = 0; group < groups; group++)
            {
                stream.Position = 0;
                var data = await ParquetSerializer.DeserializeUntypedAsync(stream, rowGroupIndex: group, cancellationToken: cancellationToken);
                foreach (var row in data.Data)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (++totalRows > options.Value.MaximumRowsPerArtifact)
                        throw new WorkValidationException("Combined Outage Raw parts exceed the configured row bound.");
                    // Untyped Parquet deserialization omits dictionary entries for null cells.
                    foreach (var column in columns)
                        if (!row.ContainsKey(column)) row[column] = null!;
                    var change = mapper.Map(row, work.Windfarm, work.Turbines);
                    var expectsDeletion = work.Message.PayloadKind == "OutageDeletedEvents";
                    if (expectsDeletion != (change is OutageDeletion))
                        throw new WorkValidationException("Raw tombstone flag disagrees with the declared payload kind.");
                    if (!work.IsReconciliation && !work.IsObservation)
                    {
                        var member = work.Message.Members.SingleOrDefault(member => member.EventId == change.Key.OutageClassificationId &&
                            member.WtgId == change.Turbine.WtgId && member.RawPaths.Contains(path, StringComparer.Ordinal));
                        if (member is null) continue;
                        seenMembers.Add(member.MemberKey);
                    }
                    if (work.Message.IsFullSnapshot && change is OutageEvent snapshotEvent &&
                        (!work.Message.SnapshotScope!.WtgIds.Contains(change.Turbine.WtgId) ||
                         !snapshotEvent.Overlaps(work.Message.SnapshotScope.StartInclusiveUtc.UtcDateTime, work.Message.SnapshotScope.EndExclusiveUtc.UtcDateTime)))
                        throw new WorkValidationException("Snapshot row is outside its declared interval/WTG scope.");
                    selected.TryGetValue(change.Key.OutageClassificationId, out var previous);
                    var merged = merger.Merge(previous?.State, change);
                    var origin = new OutageOrigin(path, work.Message.ExtractionToken, work.Message.RunId,
                        work.Message.AttemptId, work.Operation.OperationId, work.Operation.ExecutionId,
                        row["SourceWatermarkUtc"] is DateTime watermark && watermark.Kind != DateTimeKind.Local
                            ? DateTime.SpecifyKind(watermark, DateTimeKind.Utc)
                            : throw new WorkValidationException("SourceWatermarkUtc must contain a UTC timestamp."));
                    var origins = merged.Changed ? new[] { origin } : change.LastUpdatedUtc == previous!.State.LastUpdatedUtc
                        ? previous.Origins.Append(origin).Distinct().ToArray() : previous.Origins;
                    selected[change.Key.OutageClassificationId] = new(merged.State, 0, origins);
                    if (selected.Count > options.Value.MaximumRowsPerArtifact)
                        throw new WorkValidationException("Selected Outage batch exceeds the configured event bound.");
                }
            }
        }
        if (!work.IsReconciliation && work.Message.Members.Any(member => !seenMembers.Contains(member.MemberKey)))
            throw new WorkValidationException("A declared event member has no row in its exact Raw paths.");
        telemetry.Information("Outage Raw data validated and selected. BatchId={BatchId} EventCount={EventCount} ArtifactCount={ArtifactCount}",
            work.Message.BatchId, selected.Count, work.Message.RawPaths.Length);
        return selected.Values.OrderBy(item => item.State.Key.OutageClassificationId, StringComparer.Ordinal).ToArray();
    }

    public Task<OutageCatalog> ReadCatalogAsync(string? path, CancellationToken cancellationToken) =>
        path is null ? Task.FromResult(OutageCatalog.Empty) : ReadJsonAsync<OutageCatalog>(path, cancellationToken);
    public Task<EventMonthSnapshot> ReadMonthAsync(EventMonthReference reference, CancellationToken cancellationToken) =>
        ReadJsonAsync<EventMonthSnapshot>(reference.SnapshotPath, cancellationToken);
    public Task<OutageLifecyclePlan> ReadPlanAsync(string path, CancellationToken cancellationToken) =>
        ReadJsonAsync<OutageLifecyclePlan>(path, cancellationToken);

    public async Task<EventMonthReference> WriteMonthAsync(OutageCatalogLease lease, string partitionKey,
        long revision, IReadOnlyList<CanonicalOutage> events, CancellationToken cancellationToken)
    {
        var partition = partitionKey == "_tombstones" ? "deleted-events" :
            $"startyear={partitionKey[..4]}/startmonth={partitionKey[5..]}";
        var prefix = $"harmonized/events/outage/windfarm={Encode(lease.Windfarm.WindfarmName)}/{partition}/revisions/revision={revision}/attempts/token={lease.Token}";
        var parquetPath = prefix + (partitionKey == "_tombstones" ? "/deleted-events.parquet" : "/outage-events.parquet");
        var rows = events.Select(item => OutageParquetRow.From(lease.Windfarm, item)).ToArray();
        await WriteParquetAsync(parquetPath, rows, cancellationToken);
        await WriteJsonAsync(prefix + "/snapshot.json", new EventMonthSnapshot(1, events), cancellationToken);
        await WriteJsonAsync(prefix + "/lineage.json", new
        {
            SchemaVersion = 1, SourceType = "Outage", lease.Windfarm, PartitionKey = partitionKey,
            Revision = revision, CommittedAttempt = lease.Token, CreatedDateUtc = clock.GetUtcNow(),
            ParquetPath = parquetPath, Rows = events.Select(item => new
            { EventId = item.State.Key.OutageClassificationId, item.EventRevision, item.State.LastUpdatedUtc, item.Origins })
        }, cancellationToken);
        telemetry.Information("Canonical Outage partition candidate written. PartitionKey={PartitionKey} Revision={Revision} RowCount={RowCount}",
            partitionKey, revision, events.Count);
        return new(partitionKey, revision, lease.Token, parquetPath, prefix + "/snapshot.json", prefix + "/lineage.json");
    }

    public async Task<string> WriteCatalogAsync(OutageCatalogLease lease, OutageCatalog catalog, CancellationToken cancellationToken)
    {
        var prefix = $"harmonized/events/outage/windfarm={Encode(lease.Windfarm.WindfarmName)}/_catalog/revisions/revision={lease.Revision + 1}/attempts/token={lease.Token}";
        var path = prefix + "/manifest.json";
        await WriteJsonAsync(path, catalog, cancellationToken);
        await WriteJsonAsync(prefix + "/lineage.json", new
        { SchemaVersion = 1, PreviousManifestPath = lease.CatalogPath, CreatedDateUtc = clock.GetUtcNow(), catalog.Months }, cancellationToken);
        return path;
    }

    public async Task<string> WritePlanAsync(OutageCatalogLease lease, OutageLifecyclePlan plan, CancellationToken cancellationToken)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(plan, HarmonizerJson.Options);
        var hash = Convert.ToHexStringLower(SHA256.HashData(bytes));
        var path = $"harmonized/lifecycle/outage/windfarm={Encode(lease.Windfarm.WindfarmName)}/event={Encode(plan.WorkId)}/generation={plan.Generation}/attempts/token={lease.Token}/progress={hash}/plan.json";
        await WriteBytesAsync(path, bytes, "application/json", cancellationToken);
        return path;
    }

    public async Task<OutageDailyCandidate> WriteDayAsync(OutageCatalogLease lease, OutageLifecyclePlan plan,
        OutageRange range, DateOnly date, long previousRevision, IReadOnlyList<OutageDayReference> rows, OutageDayEvidence? trigger, CancellationToken cancellationToken)
    {
        var key = new WtgDayKey(lease.Windfarm.WindfarmId, range.WtgId, date, "Outage");
        var prefix = $"harmonized/wtg/windfarm={Encode(lease.Windfarm.WindfarmName)}/wtg={Encode(range.WtgName)}/date={date:yyyy-MM-dd}/outage/revision={previousRevision + 1}/attempts/token={lease.Token}";
        var parquetPath = prefix + "/outage-segments.parquet";
        var mapped = rows.Select(row => OutageParquetRow.FromDay(lease.Windfarm, row)).ToArray();
        var hash = Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(mapped, HarmonizerJson.Options)));
        await WriteParquetAsync(parquetPath, mapped, cancellationToken);
        await WriteJsonAsync(prefix + "/snapshot.json", new
        { SchemaVersion = 1, Key = key, RecordCount = rows.Count, CanonicalHash = hash, CatalogPath = lease.CatalogPath, Rows = mapped }, cancellationToken);
        await WriteJsonAsync(prefix + "/lineage.json", new
        {
            SchemaVersion = 1, plan.Operation.OperationId, plan.Operation.ExecutionId, plan.TriggerKind,
            plan.WorkId, plan.EventId, PlanningGeneration = plan.Generation, plan.OriginBatch, plan.RawPaths,
            plan.OriginalMessageBody, TriggerEvidence = trigger,
            CreatedDateUtc = clock.GetUtcNow(), ComponentRevision = previousRevision + 1, CommittedAttempt = lease.Token,
            CatalogPath = lease.CatalogPath, ParquetPath = parquetPath, Key = key,
            Contributions = rows.Select(row => new { EventId = row.Segment.Key.OutageClassificationId,
                row.EventRevision, row.Canonical.PartitionKey, row.Canonical.Revision, row.Canonical.Attempt,
                CanonicalEventPath = row.Canonical.ParquetPath, CanonicalLineagePath = row.Canonical.LineagePath, row.Origins }).Distinct()
        }, cancellationToken);
        telemetry.Information("Outage WTG-day candidate written. WtgId={WtgId} DataDate={DataDate} Revision={Revision} RecordCount={RecordCount}",
            range.WtgId, date, previousRevision + 1, rows.Count);
        return new(key, previousRevision, previousRevision + 1, hash, parquetPath, prefix + "/snapshot.json",
            prefix + "/lineage.json", rows.Count, plan.Operation);
    }

    private async Task<T> ReadJsonAsync<T>(string path, CancellationToken cancellationToken)
    {
        await using var stream = await output.OpenReadAsync(path, cancellationToken);
        CheckStream(stream);
        var result = await JsonSerializer.DeserializeAsync<T>(stream, HarmonizerJson.Options, cancellationToken)
            ?? throw new WorkValidationException("Committed Outage metadata is missing.");
        telemetry.Debug("Committed Outage metadata loaded. MetadataType={MetadataType} BlobPath={BlobPath}", typeof(T).Name, path);
        return result;
    }

    private async Task WriteParquetAsync(string path, IReadOnlyList<OutageParquetRow> rows, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();
        await ParquetSerializer.SerializeAsync(rows, stream, cancellationToken: cancellationToken);
        CheckStream(stream);
        stream.Position = 0;
        await output.WriteNewAsync(path, stream, "application/vnd.apache.parquet", cancellationToken);
    }

    private Task WriteJsonAsync<T>(string path, T value, CancellationToken cancellationToken) =>
        WriteBytesAsync(path, JsonSerializer.SerializeToUtf8Bytes(value, HarmonizerJson.Options), "application/json", cancellationToken);

    private async Task WriteBytesAsync(string path, byte[] bytes, string contentType, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream(bytes);
        CheckStream(stream);
        await output.WriteNewAsync(path, stream, contentType, cancellationToken);
    }

    private void CheckStream(Stream stream)
    {
        if (!stream.CanSeek || stream.Length > options.Value.MaximumArtifactBytes)
            throw new WorkValidationException("Outage artifact must be seekable and within the configured byte limit.");
    }
    private static string Encode(string text) => Uri.EscapeDataString(text);
}
