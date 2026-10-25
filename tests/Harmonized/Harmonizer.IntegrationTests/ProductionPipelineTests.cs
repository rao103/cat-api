using Harmonizer.Core.SourceTypes.Production.Repositories;
using Harmonizer.Core.SourceTypes.Production.Models;
using Harmonizer.Core.SourceTypes.Production.Serialization;
using Harmonizer.Core.SourceTypes.Production.Adapters;
using Harmonizer.Core.SourceTypes.Production.Operations;
using System.Security.Cryptography;
using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Adapters;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Operations;
using Harmonizer.Core.Repositories;
using Harmonizer.Core.Serialization;
using Microsoft.Extensions.Options;
using Parquet.Serialization;
using ProductionIngestor.Contracts;
using Xunit;
namespace Harmonizer.IntegrationTests;

public sealed class ProductionPipelineTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    private static int nextWtgId = 500;

    [PostgresFact]
    public Task Multipart_refresh_missing_part_blocks_commit_then_recovers_and_replays_without_new_revision() =>
        VerifyRefreshAsync(3);

    [PostgresFact]
    public Task V2_refresh_resolves_catalog_identity_and_commits_receipts_against_the_original_wire_body() =>
        VerifyRefreshAsync(2);

    private async Task VerifyRefreshAsync(int schemaVersion)
    {
        var dataSource = fixture.DataSource ?? throw new InvalidOperationException("No database.");
        var wtgId = Interlocked.Increment(ref nextWtgId);
        var wtgName = $"WTG{wtgId}";
        await using (var identity = dataSource.CreateCommand("INSERT INTO wind_turbine(id,windfarm_id,wtg_name) VALUES(@id,2,@name)"))
        {
            identity.Parameters.AddWithValue("id", wtgId);
            identity.Parameters.AddWithValue("name", wtgName);
            await identity.ExecuteNonQueryAsync();
        }
        var options = Options.Create(new HarmonizerOptions());
        var stateTelemetry = new RecordingTelemetry<PostgresProductionControlStateRepository>();
        var artifactTelemetry = new RecordingTelemetry<ParquetProductionArtifactStore>();
        var readerTelemetry = new RecordingTelemetry<ProductionWorkMessageReader>();
        var state = new PostgresProductionControlStateRepository(dataSource, options, stateTelemetry, new());
        var rawObjects = new TestObjectStore();
        var silverObjects = new TestObjectStore();
        var artifacts = new ParquetProductionArtifactStore(rawObjects, silverObjects, new(), options, artifactTelemetry);
        var processor = new ProductionBatchProcessor(state, artifacts, new(), options, TestTelemetry.For<ProductionBatchProcessor>(), TestTelemetry.For<AsyncHeartbeat>());
        var start = new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc);
        var artifactId = Convert.ToHexStringLower(SHA256.HashData(Guid.NewGuid().ToByteArray()));
        var batchId = Convert.ToHexStringLower(SHA256.HashData(Guid.NewGuid().ToByteArray()));
        var scope = new ProductionDateRangeV1("2026-02-01", "2026-02-01");
        var manifest = new ProductionExtractionReadyV1(2, "ExtractionReady", "Production", 2, "WOW02", "Refresh", "ProductionSnapshot", 2,
            "ProductionTimestamp", "request", "attempt", 1, [scope],
            [new(1, start, start.AddDays(1), [scope], 144, 2, true)],
            [new("raw/one.parquet", 1, 1, 72), new("raw/two.parquet", 1, 2, 72)], 144, false, [wtgName], null);
        var wire = new ProductionWtgDayWorkV3(3, "ProductionWtgDays", artifactId, batchId, "Production", 2, "WOW02", "2026-02-01",
            "Refresh", "ProductionSnapshot", 2, "request", "attempt", 1, ["raw/one.parquet"], [new(wtgId, wtgName)], [], true, false, false, null,
            JsonSerializer.Serialize(manifest, HarmonizerJson.Options), null);
        var v2 = new ProductionItemDayWorkV2(2, wire.WorkKind, artifactId, batchId, "Production", "2", "WOW02", wire.DataDate,
            wire.ExtractionMode, wire.PayloadKind, wire.PayloadSchemaVersion, wire.RequestId, wire.AttemptId, wire.ExtractionToken,
            wire.RawPaths, [wtgName], [], wire.IsExtractionReady, wire.HasNoSourceRows, wire.AllowRemovalReconciliation,
            wire.SourceVersionEvidence, wire.OriginalMessageBody, wire.TraceId);
        var body = schemaVersion == 2
            ? JsonSerializer.Serialize(v2, HarmonizerJson.Options)
            : JsonSerializer.Serialize(wire, HarmonizerJson.Options);
        var catalog = new PostgresIdentityCatalog(dataSource, TestTelemetry.For<PostgresIdentityCatalog>());
        var work = await new ProductionWorkMessageReader(options, new(catalog, options, TestTelemetry.For<ProductionWorkV2Reader>()), readerTelemetry).ReadAsync(body, default);
        Assert.Equal(body, work.WireBody);
        Assert.Equal(new Harmonizer.Core.Models.TurbineIdentity(wtgId, wtgName), Assert.Single(work.Turbines));
        Assert.Contains(readerTelemetry.Events, entry => entry.Template.StartsWith("Production work validated.", StringComparison.Ordinal));
        await using (var seed = dataSource.CreateCommand("""
            SELECT * FROM control.raw_artifact_save_plan(@artifact,'Production',2,'request',1,'{}'::jsonb,@plan::jsonb,'test');
            SELECT control.raw_artifact_mark_dispatched(@artifact,'test');
            """))
        {
            seed.Parameters.AddWithValue("artifact", artifactId);
            seed.Parameters.AddWithValue("plan", JsonSerializer.Serialize(new[] { new { Id = batchId, Body = body } }));
            await seed.ExecuteNonQueryAsync();
        }
        async Task WritePart(string path, int firstSlot)
        {
            var rows = Enumerable.Range(firstSlot, 72).Select(slot => new ProductionParquetRow
            {
                WindfarmId = 2,
                WindfarmName = "WOW02",
                WtgId = wtgId,
                WtgName = wtgName,
                WtgRdsppId = wtgName,
                StartTimeInterval = start.AddMinutes(slot * 10),
                EndTimeInterval = start.AddMinutes((slot + 1) * 10),
                LastUpdatedDate = start.AddDays(1),
                ActualProductionKWh = slot,
                PossibleProductionKWh = 144,
                ContractualPoPEColumn = null,
                LostProductionCapacitykWh = null,
                SiteAverageWindSpeedMps = 7
            });
            using var stream = new MemoryStream();
            await ParquetSerializer.SerializeAsync(rows, stream);
            stream.Position = 0;
            await rawObjects.WriteNewAsync(path, stream, "application/vnd.apache.parquet", default);
        }
        await WritePart("raw/one.parquet", 0);
        await Assert.ThrowsAsync<FileNotFoundException>(() => processor.ProcessAsync(work, default));
        Assert.False(await state.IsItemAppliedAsync(work.Batch, wtgId, default));
        Assert.DoesNotContain(stateTelemetry.Events, entry => entry.Template.StartsWith("Production component committed", StringComparison.Ordinal));
        await using (var check = StateCheck("SELECT status,component_revision,first_failed_batch_id FROM control.wtg_day_input_state WHERE wtgid=@id"))
        await using (var reader = await check.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.Equal("Failed", reader.GetString(0));
            Assert.Equal(0, reader.GetInt64(1));
            Assert.Equal(batchId, reader.GetString(2));
        }
        await WritePart("raw/two.parquet", 72);
        await processor.ProcessAsync(work, default);
        Assert.True(await state.IsItemAppliedAsync(work.Batch, wtgId, default));
        Assert.Contains(artifactTelemetry.Events, entry => entry.Template.StartsWith("Production Raw work selected.", StringComparison.Ordinal));
        Assert.Contains(artifactTelemetry.Events, entry => entry.Template.StartsWith("Production candidate artifacts written.", StringComparison.Ordinal));
        Assert.Contains(stateTelemetry.Events, entry => entry.Template.StartsWith("Production component committed", StringComparison.Ordinal));
        string snapshotPath;
        await using (var check = StateCheck("SELECT status,component_revision,record_count,snapshot_path FROM control.wtg_day_input_state WHERE wtgid=@id"))
        await using (var reader = await check.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.Equal("Complete", reader.GetString(0));
            Assert.Equal(1, reader.GetInt64(1));
            Assert.Equal(144, reader.GetInt32(2));
            snapshotPath = reader.GetString(3);
        }
        var snapshot = await artifacts.ReadSnapshotAsync(snapshotPath, default);
        Assert.Equal(144, snapshot.Rows.Count);
        Assert.Equal(2, snapshot.Rows.Select(row => row.Origin.BlobPath).Distinct().Count());
        Assert.Equal(2, rawObjects.Data.Count);
        Assert.Equal(3, silverObjects.Data.Count);
        var count = silverObjects.Data.Count;
        await processor.ProcessAsync(work, default);
        Assert.Equal(count, silverObjects.Data.Count);
        await using (var check = dataSource.CreateCommand("SELECT materialized_at_utc IS NOT NULL FROM control.raw_artifact_batch WHERE batch_id=@batch"))
        {
            check.Parameters.AddWithValue("batch", batchId);
            Assert.Equal(true, await check.ExecuteScalarAsync());
        }
        var notifications = await new PostgresNotificationOutbox(dataSource, TestTelemetry.For<PostgresNotificationOutbox>()).ReadPendingAsync(100, default);
        var currentNotifications = notifications.Where(notification =>
        {
            using var document = JsonDocument.Parse(notification.Body);
            return document.RootElement.GetProperty("WtgId").GetInt32() == wtgId;
        }).ToArray();
        Assert.Equal(2, currentNotifications.Length);

        Npgsql.NpgsqlCommand StateCheck(string sql)
        {
            var command = dataSource.CreateCommand(sql);
            command.Parameters.AddWithValue("id", wtgId);
            return command;
        }
    }

    private sealed class TestObjectStore : IRawObjectStore, IHarmonizedArtifactStore
    {
        public Dictionary<string, byte[]> Data { get; } = new();
        public Task<Stream> OpenReadAsync(string path, CancellationToken cancellationToken)
        {
            if (!Data.TryGetValue(path, out var bytes)) throw new FileNotFoundException("Injected missing Raw part", path);
            return Task.FromResult<Stream>(new MemoryStream(bytes));
        }
        public async Task WriteNewAsync(string path, Stream content, string contentType, CancellationToken cancellationToken)
        {
            using var copy = new MemoryStream();
            await content.CopyToAsync(copy, cancellationToken);
            Data.Add(path, copy.ToArray());
        }
    }
}
