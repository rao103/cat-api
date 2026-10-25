using Harmonizer.Core.SourceTypes.Production.Repositories;
using Harmonizer.Core.SourceTypes.Production.Models;
using System.Security.Cryptography;
using System.Text.Json;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Models;
using Harmonizer.Core.Repositories;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit;
namespace Harmonizer.IntegrationTests;

public sealed class PostgresFactAttribute : FactAttribute
{
    public PostgresFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("HARMONIZER_TEST_POSTGRES")))
            Skip = "Set HARMONIZER_TEST_POSTGRES to a disposable local PostgreSQL instance with CREATE DATABASE permission.";
    }
}

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly string name = "harmonizer_test_" + Guid.NewGuid().ToString("N");
    private string? adminConnection;
    private bool created;
    public NpgsqlDataSource? DataSource { get; private set; }
    public async Task InitializeAsync()
    {
        adminConnection = Environment.GetEnvironmentVariable("HARMONIZER_TEST_POSTGRES");
        if (string.IsNullOrWhiteSpace(adminConnection)) return;
        await using var admin = new NpgsqlConnection(adminConnection);
        await admin.OpenAsync();
        await using (var create = new NpgsqlCommand($"CREATE DATABASE {name} TEMPLATE template0 ENCODING 'UTF8'", admin))
            await create.ExecuteNonQueryAsync();
        created = true;
        var builder = new NpgsqlConnectionStringBuilder(adminConnection) { Database = name };
        DataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        foreach (var file in new[] { "0003_create_raw_fanout_schema.reference.sql", "0004_create_harmonizer_schema.sql" })
        {
            await using var migration = DataSource.CreateCommand(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Database", file)));
            await migration.ExecuteNonQueryAsync();
        }
        await using var catalog = DataSource.CreateCommand("""
            CREATE TABLE windfarm(id integer PRIMARY KEY,windfarm_name text NOT NULL);
            CREATE TABLE wind_turbine(id integer PRIMARY KEY,windfarm_id integer NOT NULL,wtg_name text NOT NULL);
            CREATE TABLE extraction_cursor(windfarm_id integer NOT NULL,source_type text NOT NULL);
            INSERT INTO windfarm VALUES(2,'WOW02');
            INSERT INTO wind_turbine VALUES(11,2,'WOW02F15');
            INSERT INTO extraction_cursor VALUES(2,'Production'),(2,'Outage');
            """);
        await catalog.ExecuteNonQueryAsync();
    }
    public async Task DisposeAsync()
    {
        if (DataSource is not null) await DataSource.DisposeAsync();
        if (!created || adminConnection is null) return;
        await using var admin = new NpgsqlConnection(adminConnection);
        await admin.OpenAsync();
        await using var drop = new NpgsqlCommand($"DROP DATABASE {name} WITH (FORCE)", admin);
        await drop.ExecuteNonQueryAsync();
    }
}

public sealed class PostgresTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    private static int nextWtgId = 100;
    private NpgsqlDataSource DataSource => fixture.DataSource ?? throw new InvalidOperationException("No test database.");
    private PostgresProductionControlStateRepository Repository => new(DataSource, Options.Create(new HarmonizerOptions()), TestTelemetry.For<PostgresProductionControlStateRepository>(), new());
    private static string Hash() => Convert.ToHexStringLower(SHA256.HashData(Guid.NewGuid().ToByteArray()));
    private static ProductionWork NewWork(params int[] members) => new(new(Hash(), Hash()), new(2, "WOW02"), new(2026, 9, 1),
        members.Select(id => new TurbineIdentity(id, $"WTG{id}")).ToArray(), ["raw/exact.parquet"], 1, "run", "attempt", false, false, false, null,
        "{}", JsonSerializer.Serialize(new { Id = Guid.NewGuid() }), null);
    private static WtgDayKey Key(ProductionWork work, int index = 0) => new(work.Windfarm.WindfarmId, work.Turbines[index].WtgId, work.DataDate);
    private static ComponentCommit Commit(ComponentLease lease) => new(lease, lease.Revision + 1, new('c', 64),
        new("candidate/data.parquet", "candidate/snapshot.json", "candidate/lineage.json"), 1, false, true);
    private async Task SeedAsync(ProductionWork work)
    {
        await using var seed = DataSource.CreateCommand("""
            SELECT * FROM control.raw_artifact_save_plan(@artifact,'Production',2,'run',1,'{}'::jsonb,@plan::jsonb,'test');
            SELECT control.raw_artifact_mark_dispatched(@artifact,'test');
            """);
        seed.Parameters.AddWithValue("artifact", work.Batch.ArtifactId);
        seed.Parameters.AddWithValue("plan", JsonSerializer.Serialize(new[] { new { Id = work.Batch.BatchId, Body = work.WireBody } }));
        await seed.ExecuteNonQueryAsync();
        await Repository.RegisterBatchAsync(work, default);
    }
    private async Task<long> CountAsync(string sql, ProductionWork work)
    {
        await using var command = DataSource.CreateCommand(sql);
        command.Parameters.AddWithValue("artifact", work.Batch.ArtifactId);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
    [PostgresFact]
    public async Task Registered_batch_cannot_change_its_resolved_turbine_membership()
    {
        var work = NewWork(Interlocked.Increment(ref nextWtgId));
        await SeedAsync(work);
        var remapped = work with { Turbines = [new TurbineIdentity(Interlocked.Increment(ref nextWtgId), "remapped-name")] };
        await Assert.ThrowsAsync<WorkValidationException>(() => Repository.RegisterBatchAsync(remapped, default));
    }

    [PostgresFact]
    public async Task Concurrent_claims_have_exactly_one_owner()
    {
        var work = NewWork(Interlocked.Increment(ref nextWtgId)); await SeedAsync(work);
        async Task<bool> TryClaim()
        {
            try { await Repository.ClaimAsync(Key(work), work.Batch, Guid.NewGuid(), default); return true; }
            catch (LeaseUnavailableException) { return false; }
        }
        var outcomes = await Task.WhenAll(TryClaim(), TryClaim());
        Assert.Single(outcomes, value => value);
    }
    [PostgresFact]
    public async Task Expired_owner_cannot_renew_or_commit_after_reclaim()
    {
        var work = NewWork(Interlocked.Increment(ref nextWtgId)); await SeedAsync(work);
        var old = await Repository.ClaimAsync(Key(work), work.Batch, Guid.NewGuid(), default);
        await using (var expire = DataSource.CreateCommand("UPDATE control.wtg_day_input_state SET lease_expires_at_utc=clock_timestamp()-interval '1 second',updated_by='test' WHERE wtgid=@id;"))
        { expire.Parameters.AddWithValue("id", Key(work).WtgId); await expire.ExecuteNonQueryAsync(); }
        var fresh = await Repository.ClaimAsync(Key(work), work.Batch, Guid.NewGuid(), default);
        Assert.True(fresh.FenceToken > old.FenceToken);
        await Assert.ThrowsAsync<LeaseLostException>(() => Repository.RenewAsync(old, default));
        await Assert.ThrowsAsync<LeaseLostException>(() => Repository.CommitAsync(Commit(old), default));
        Assert.False(await Repository.IsItemAppliedAsync(work.Batch, Key(work).WtgId, default));
        await Repository.CommitAsync(Commit(fresh), default);
        Assert.True(await Repository.IsItemAppliedAsync(work.Batch, Key(work).WtgId, default));
    }
    [PostgresFact]
    public async Task Last_member_commit_acknowledges_batch_and_creates_durable_outbox()
    {
        var work = NewWork(Interlocked.Increment(ref nextWtgId), Interlocked.Increment(ref nextWtgId)); await SeedAsync(work);
        var first = await Repository.ClaimAsync(Key(work), work.Batch, Guid.NewGuid(), default);
        await Repository.CommitAsync(Commit(first), default);
        Assert.Equal(0, await CountAsync("SELECT count(*) FROM control.raw_artifact_batch WHERE artifact_id=@artifact AND materialized_at_utc IS NOT NULL", work));
        var second = await Repository.ClaimAsync(Key(work, 1), work.Batch, Guid.NewGuid(), default);
        await Repository.FailAsync(second, work, "InjectedFailure", default);
        Assert.True(await Repository.IsItemAppliedAsync(work.Batch, Key(work).WtgId, default));
        Assert.False(await Repository.IsItemAppliedAsync(work.Batch, Key(work, 1).WtgId, default));
        second = await Repository.ClaimAsync(Key(work, 1), work.Batch, Guid.NewGuid(), default);
        await Repository.CommitAsync(Commit(second), default);
        Assert.Equal(1, await CountAsync("SELECT count(*) FROM control.raw_artifact_batch WHERE artifact_id=@artifact AND materialized_at_utc IS NOT NULL", work));
        Assert.Equal(1, await CountAsync("SELECT count(*) FROM control.raw_artifact_dispatch WHERE artifact_id=@artifact AND harmonization_completed_at_utc IS NOT NULL", work));
        Assert.NotEmpty(await new PostgresNotificationOutbox(DataSource, TestTelemetry.For<PostgresNotificationOutbox>()).ReadPendingAsync(100, default));
    }
    [PostgresFact]
    public async Task Failed_batch_pins_exact_first_reference_and_blocks_later_batch()
    {
        var id = Interlocked.Increment(ref nextWtgId);
        var first = NewWork(id); var later = NewWork(id); await SeedAsync(first); await SeedAsync(later);
        var lease = await Repository.ClaimAsync(Key(first), first.Batch, Guid.NewGuid(), default);
        await Repository.FailAsync(lease, first, "BadRaw", default);
        await Assert.ThrowsAsync<LeaseUnavailableException>(() => Repository.ClaimAsync(Key(later), later.Batch, Guid.NewGuid(), default));
        var retry = await Repository.ClaimAsync(Key(first), first.Batch, Guid.NewGuid(), default);
        Assert.Equal("Failed", retry.Status);
    }
    [PostgresFact]
    public async Task Acknowledgement_failure_rolls_back_component_receipt_and_outbox()
    {
        var work = NewWork(Interlocked.Increment(ref nextWtgId)); await SeedAsync(work);
        var lease = await Repository.ClaimAsync(Key(work), work.Batch, Guid.NewGuid(), default);
        await using (var remove = DataSource.CreateCommand("DELETE FROM control.raw_artifact_batch WHERE artifact_id=@artifact"))
        { remove.Parameters.AddWithValue("artifact", work.Batch.ArtifactId); await remove.ExecuteNonQueryAsync(); }
        await Assert.ThrowsAsync<PostgresException>(() => Repository.CommitAsync(Commit(lease), default));
        Assert.False(await Repository.IsItemAppliedAsync(work.Batch, Key(work).WtgId, default));
        await using var state = DataSource.CreateCommand("SELECT component_revision FROM control.wtg_day_input_state WHERE wtgid=@id");
        state.Parameters.AddWithValue("id", Key(work).WtgId);
        Assert.Equal(0L, await state.ExecuteScalarAsync());
        await using var outbox = DataSource.CreateCommand("SELECT count(*) FROM control.harmonizer_outbox WHERE deduplication_key LIKE @pattern");
        outbox.Parameters.AddWithValue("pattern", $"%:2:{Key(work).WtgId}:2026-09-01:%");
        Assert.Equal(0L, await outbox.ExecuteScalarAsync());
    }
    [PostgresFact]
    public async Task Retained_receipt_handles_redelivery_after_fanout_ledger_cleanup()
    {
        var work = NewWork(Interlocked.Increment(ref nextWtgId)); await SeedAsync(work);
        var lease = await Repository.ClaimAsync(Key(work), work.Batch, Guid.NewGuid(), default);
        await Repository.CommitAsync(Commit(lease), default);
        await using (var remove = DataSource.CreateCommand("DELETE FROM control.raw_artifact_batch WHERE artifact_id=@artifact; DELETE FROM control.raw_artifact_dispatch WHERE artifact_id=@artifact;"))
        { remove.Parameters.AddWithValue("artifact", work.Batch.ArtifactId); await remove.ExecuteNonQueryAsync(); }
        await Repository.RegisterBatchAsync(work, default);
        Assert.True(await Repository.IsItemAppliedAsync(work.Batch, Key(work).WtgId, default));
    }
    [PostgresFact]
    public async Task Tracked_commit_persists_ids_and_rejects_retagging()
    {
        var work = NewWork(Interlocked.Increment(ref nextWtgId)) with { Operation = new("OP-TRACKED", "EXEC-TRACKED") };
        await SeedAsync(work);
        await Assert.ThrowsAsync<WorkValidationException>(() => Repository.RegisterBatchAsync(
            work with { Operation = new("OP-OTHER", "EXEC-TRACKED") }, default));
        var lease = await Repository.ClaimAsync(Key(work), work.Batch, Guid.NewGuid(), default);
        await Assert.ThrowsAsync<WorkValidationException>(() => Repository.CommitAsync(Commit(lease), default));
        await Repository.CommitAsync(Commit(lease) with { Operation = work.Operation }, default);
        await using var state = DataSource.CreateCommand("""
            SELECT b.operation_id,b.execution_id,s.last_operation_id,s.last_execution_id
            FROM control.harmonizer_batch b JOIN control.wtg_day_input_state s ON s.wtgid=@wtgid
            WHERE b.artifact_id=@artifact AND b.batch_id=@batch;
            """);
        state.Parameters.AddWithValue("wtgid", Key(work).WtgId);
        state.Parameters.AddWithValue("artifact", work.Batch.ArtifactId);
        state.Parameters.AddWithValue("batch", work.Batch.BatchId);
        await using (var reader = await state.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.Equal("OP-TRACKED", reader.GetString(0));
            Assert.Equal("EXEC-TRACKED", reader.GetString(1));
            Assert.Equal("OP-TRACKED", reader.GetString(2));
            Assert.Equal("EXEC-TRACKED", reader.GetString(3));
        }
        var pending = await new PostgresNotificationOutbox(DataSource, TestTelemetry.For<PostgresNotificationOutbox>()).ReadPendingAsync(10000, default);
        var correlated = pending.Where(message => message.OperationId == "OP-TRACKED").ToArray();
        Assert.Equal(2, correlated.Length);
        Assert.All(correlated, message =>
        {
            Assert.Equal("EXEC-TRACKED", message.ExecutionId);
            using var body = JsonDocument.Parse(message.Body);
            Assert.Equal("OP-TRACKED", body.RootElement.GetProperty("OperationId").GetString());
        });
    }

    [PostgresFact]
    public async Task New_operation_on_unchanged_data_has_its_own_notification_without_new_revision()
    {
        var id = Interlocked.Increment(ref nextWtgId);
        var original = NewWork(id) with { Operation = new("OP-FIRST", "EXEC-FIRST") };
        await SeedAsync(original);
        var lease = await Repository.ClaimAsync(Key(original), original.Batch, Guid.NewGuid(), default);
        await Repository.CommitAsync(Commit(lease) with { Operation = original.Operation }, default);
        var later = NewWork(id) with { Operation = new("OP-REUSE", "EXEC-REUSE") };
        await SeedAsync(later);
        var reused = await Repository.ClaimAsync(Key(later), later.Batch, Guid.NewGuid(), default);
        var noChange = new ComponentCommit(reused, reused.Revision, new('c', 64),
            new(reused.ParquetPath, reused.SnapshotPath!, reused.ManifestPath!), 1, false, false)
            { Operation = later.Operation };
        await Repository.CommitAsync(noChange, default);
        await Repository.RegisterBatchAsync(later, default);
        Assert.True(await Repository.IsItemAppliedAsync(later.Batch, id, default));
        await using var query = DataSource.CreateCommand("""
            SELECT s.component_revision,count(o.id)
            FROM control.wtg_day_input_state s JOIN control.harmonizer_outbox o ON o.operation_id='OP-REUSE'
            WHERE s.wtgid=@wtgid GROUP BY s.component_revision;
            """);
        query.Parameters.AddWithValue("wtgid", id);
        await using var reader = await query.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1L, reader.GetInt64(0));
        Assert.Equal(1L, reader.GetInt64(1));
    }

    [PostgresFact]
    public async Task Catalog_join_returns_numeric_ids_and_separate_names()
    {
        var catalog = new PostgresIdentityCatalog(DataSource, TestTelemetry.For<PostgresIdentityCatalog>());
        var result = await catalog.ResolveAsync(new(2, "WOW02"), ["WOW02F15"], new(2026, 8, 24), default);
        Assert.Equal(new TurbineIdentity(11, "WOW02F15"), Assert.Single(result));
        var byId = await catalog.ResolveIdsAsync(new(2, "WOW02"), [11], new(2026, 8, 24), default);
        Assert.Equal(result, byId);
        await Assert.ThrowsAsync<WorkValidationException>(() => catalog.ResolveAsync(new(2, "WRONG"), ["WOW02F15"], new(2026, 8, 24), default));
        await Assert.ThrowsAsync<WorkValidationException>(() => catalog.ResolveAsync(new(2, "WOW02"), ["UNKNOWN"], new(2026, 8, 24), default));
        await Assert.ThrowsAsync<WorkValidationException>(() => catalog.ResolveIdsAsync(new(2, "WOW02"), [999999], new(2026, 8, 24), default));
    }
}
