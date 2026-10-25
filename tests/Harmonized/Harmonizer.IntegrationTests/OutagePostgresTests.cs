using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Harmonizer.Core.SourceTypes.Outage.Repositories;
using Harmonizer.Core.SourceTypes.Outage.Serialization;
using Microsoft.Extensions.Options;
using Xunit;

namespace Harmonizer.IntegrationTests;

public sealed class OutagePostgresTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    private PostgresOutageControlRepository Repository => new(fixture.DataSource!, Options.Create(new HarmonizerOptions()), TestTelemetry.For<PostgresOutageControlRepository>());
    private static int nextFarm = 1000;
    private static WindfarmIdentity Farm() => new(Interlocked.Increment(ref nextFarm), "OUTAGE-TEST");
    private static OutageWork Work(WindfarmIdentity farm)
    {
        var identity = Guid.NewGuid().ToString("N");
        var message = new EventWorkV3
        {
            WorkKind = "CanonicalEvents", ArtifactId = identity + identity, BatchId = Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N"),
            SourceType = "Outage", PayloadKind = "OutageEvents", WindfarmId = farm.WindfarmId, WindfarmName = farm.WindfarmName,
            ExtractionToken = 1, RunId = "run", ExtractionMode = "Incremental", RawPaths = ["raw/outage-test.parquet"],
            EventIds = [identity], Members = [new("event-key", "member-key", identity, 11, "T001", ["raw/outage-test.parquet"])],
            OriginalMessageBody = "{}"
        };
        return new(message, farm, [new(11, "T001")], OperationIdentity.Untracked, JsonSerializer.Serialize(message));
    }
    private async Task SeedAsync(OutageWork work)
    {
        await using var command = fixture.DataSource!.CreateCommand("""
            SELECT * FROM control.raw_artifact_save_plan(@artifact,'Outage',@farm,'run',1,'{}'::jsonb,@plan::jsonb,'test');
            SELECT control.raw_artifact_mark_dispatched(@artifact,'test');
            """);
        command.Parameters.AddWithValue("artifact", work.Batch.ArtifactId);
        command.Parameters.AddWithValue("farm", work.Windfarm.WindfarmId);
        command.Parameters.AddWithValue("plan", JsonSerializer.Serialize(new[] { new { Id = work.Batch.BatchId, Body = work.WireBody } }));
        await command.ExecuteNonQueryAsync();
        await Repository.RegisterAsync(work, default);
    }
    private static OutagePublication Publication(OutageCatalogLease lease, OutageWork work, bool pending = true)
    {
        var id = work.Message.EventIds[0];
        var workId = OutageWorkKeys.Event(id);
        OutageRange[] ranges = pending ? [new(11, "T001", new(2026, 7, 18), new(2026, 7, 18))] : [];
        var plan = new OutageLifecyclePlan(workId, id, 1, ranges, ranges, OperationIdentity.Untracked, "SourceChange", work.Batch);
        var update = new OutageLifecycleUpdate(new(workId, id, "2026-07", 1, "lifecycle/plan.json", false,
            new(2026, 7, 18), DateTime.UtcNow), plan);
        return new(new(1, [], []), "catalog/manifest.json", [], [update], ["member-key"]);
    }

    [PostgresFact]
    public async Task Concurrent_farm_claims_have_one_owner()
    {
        var farm = Farm();
        async Task<bool> Claim()
        {
            try { await Repository.ClaimAsync(farm, default); return true; }
            catch (LeaseUnavailableException) { return false; }
        }
        var claimed = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Claim()));
        Assert.Single(claimed, success => success);
    }

    [PostgresFact]
    public async Task Expired_owner_cannot_publish_canonical_or_daily_progress()
    {
        var farm = Farm();
        var work = Work(farm);
        await SeedAsync(work);
        var old = await Repository.ClaimAsync(farm, default);
        await using (var expire = fixture.DataSource!.CreateCommand("UPDATE control.event_partition_state SET lease_expires_at_utc=clock_timestamp()-interval '1 second',updated_by='test' WHERE windfarm_id=@farm AND partition_key='_catalog'"))
        {
            expire.Parameters.AddWithValue("farm", farm.WindfarmId);
            await expire.ExecuteNonQueryAsync();
        }
        var current = await Repository.ClaimAsync(farm, default);
        Assert.True(current.Token > old.Token);
        await Assert.ThrowsAsync<LeaseLostException>(() => Repository.PublishAsync(old, work, Publication(old, work), default));
        await Assert.ThrowsAsync<LeaseLostException>(() => Repository.AdvanceAsync(old, Publication(old, work).Lifecycle[0], null, default));
        await Repository.ReleaseAsync(current, default);
    }

    [PostgresFact]
    public async Task Canonical_receipt_precedes_materialized_receipt_and_daily_commit_is_atomic_with_outbox()
    {
        var farm = Farm();
        var work = Work(farm);
        await SeedAsync(work);
        var lease = await Repository.ClaimAsync(farm, default);
        var publication = Publication(lease, work);
        await Repository.PublishAsync(lease, work, publication, default);
        Assert.True(await Repository.HasCanonicalReceiptAsync(work.Batch, default));
        Assert.False(await Repository.CompleteBatchAsync(work.Batch, default));
        var update = publication.Lifecycle[0];
        var empty = update with { Plan = update.Plan with { Pending = [] } };
        var candidate = new OutageDailyCandidate(new(farm.WindfarmId, 11, new(2026, 7, 18), "Outage"), 0, 1,
            new('c', 64), "daily/outage.parquet", "daily/snapshot.json", "daily/lineage.json", 0, OperationIdentity.Untracked);
        await Repository.AdvanceAsync(lease, empty, candidate, default);
        Assert.True(await Repository.CompleteBatchAsync(work.Batch, default));
        await using var count = fixture.DataSource!.CreateCommand("SELECT count(*) FROM control.harmonizer_outbox WHERE message_body::jsonb->>'WindfarmId'=@farm");
        count.Parameters.AddWithValue("farm", farm.WindfarmId.ToString());
        Assert.Equal(1L, await count.ExecuteScalarAsync());
        await Repository.ReleaseAsync(lease, default);
    }

    [PostgresFact]
    public async Task Pending_ranges_block_an_older_complete_day_until_its_cleanup_commits()
    {
        var farm = Farm();
        var work = Work(farm);
        await SeedAsync(work);
        var lease = await Repository.ClaimAsync(farm, default);
        var publication = Publication(lease, work);
        await Repository.PublishAsync(lease, work, publication, default);
        var update = publication.Lifecycle[0];
        var candidate = new OutageDailyCandidate(new(farm.WindfarmId, 11, new(2026, 7, 18), "Outage"), 0, 1,
            new('d', 64), "daily/outage.parquet", "daily/snapshot.json", "daily/lineage.json", 1, OperationIdentity.Untracked);
        await Repository.AdvanceAsync(lease, update, candidate, default);
        async Task<string> Readiness()
        {
            await using var command = fixture.DataSource!.CreateCommand("SELECT readiness_status FROM control.harmonizer_input_readiness WHERE windfarm_id=@farm AND input_type='Outage'");
            command.Parameters.AddWithValue("farm", farm.WindfarmId);
            return (string)(await command.ExecuteScalarAsync())!;
        }
        Assert.Equal("Pending", await Readiness());
        await Repository.AdvanceAsync(lease, update with { Plan = update.Plan with { Pending = [] } }, null, default);
        Assert.Equal("Complete", await Readiness());
        await Repository.ReleaseAsync(lease, default);
    }

    [PostgresFact]
    public async Task Event_registration_preserves_the_frozen_body_and_source_failure_is_visible()
    {
        var farm = Farm();
        var work = Work(farm);
        await SeedAsync(work);
        await Assert.ThrowsAsync<WorkValidationException>(() => Repository.RegisterAsync(work with { WireBody = "{}" }, default));
        await Repository.RecordSourceFailureAsync(work, "RawReadFailed", default);
        await using var command = fixture.DataSource!.CreateCommand("SELECT last_error_code FROM control.harmonizer_batch WHERE artifact_id=@artifact AND batch_id=@batch");
        command.Parameters.AddWithValue("artifact", work.Batch.ArtifactId);
        command.Parameters.AddWithValue("batch", work.Batch.BatchId);
        Assert.Equal("RawReadFailed", await command.ExecuteScalarAsync());
    }

    [PostgresFact]
    public async Task Receipt_barrier_waits_for_each_expected_canonical_batch()
    {
        var farm = Farm();
        var work = Work(farm);
        await SeedAsync(work);
        var scope = work with { Message = work.Message with { ExpectedEventBatchIds = [work.Batch.BatchId] } };
        Assert.False(await Repository.HasExpectedReceiptsAsync(scope, default));
        var lease = await Repository.ClaimAsync(farm, default);
        await Repository.PublishAsync(lease, work, Publication(lease, work), default);
        Assert.True(await Repository.HasExpectedReceiptsAsync(scope, default));
        await Repository.ReleaseAsync(lease, default);
    }
}
