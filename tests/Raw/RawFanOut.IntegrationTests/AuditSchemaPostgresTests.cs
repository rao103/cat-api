using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
namespace CAT.RawFanOut.IntegrationTests;

// Verifies the audit triggers and actor-aware lifecycle functions of 0003_create_raw_fanout_schema.sql.
[Collection("Postgres")]
public sealed class AuditSchemaPostgresTests
{
    private static readonly string[] AuditedTables = ["raw_artifact_dispatch", "raw_artifact_batch", "raw_fan_out_dead_letter"];

    private static string[] Batches(int count) => Enumerable.Range(0, count).Select(_ => Sql.Hex64()).ToArray();

    [ExternalIntegrationFact]
    public async Task FreshDeployHasTheExpectedTablesAndNoLegacyFailureTable()
    {
        await using var db = await MigrationDatabase.CreateAsync(MigrationDatabase.FullChain);

        foreach (var table in new[] { "windfarm", "wind_turbine", "extraction_cursor", "extraction_run", "alarm_known_open_alarm",
                     "control.raw_artifact_dispatch", "control.raw_artifact_batch", "control.raw_fan_out_dead_letter" })
            Assert.NotNull(await Sql.ScalarAsync<string>(db, "SELECT to_regclass(@t)::text", ("t", table)));
        Assert.Null(await Sql.ScalarAsync<string>(db, "SELECT to_regclass('control.fan_out_failure')::text"));
    }

    [ExternalIntegrationFact]
    public async Task FreshDatabaseHasNotNullAuditColumnsTriggersAndOnlyActorAwareOverloads()
    {
        await using var db = await MigrationDatabase.CreateAsync(MigrationDatabase.FullChain);

        foreach (var table in AuditedTables)
        {
            var notNull = await Sql.ScalarAsync<long>(db, """
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema='control' AND table_name=@t AND is_nullable='NO'
                  AND column_name IN ('created_date_utc','updated_date_utc','inserted_by','updated_by')
                """, ("t", table));
            Assert.Equal(4L, notNull);
            var triggers = await Sql.ScalarAsync<long>(db, """
                SELECT count(*) FROM pg_trigger WHERE tgrelid = ('control.' || @t)::regclass AND NOT tgisinternal
                """, ("t", table));
            Assert.Equal(2L, triggers);
        }

        foreach (var function in new[] { "raw_artifact_save_plan", "raw_artifact_mark_dispatched", "raw_artifact_ack" })
        {
            var overloads = await Sql.ScalarAsync<long>(db,
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='control' AND p.proname=@f",
                ("f", function));
            var withActor = await Sql.ScalarAsync<long>(db, """
                SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='control' AND p.proname=@f AND p.proargnames @> ARRAY['actor']::text[]
                """, ("f", function));
            Assert.Equal(1L, overloads);
            Assert.Equal(1L, withActor);
        }
    }

    [ExternalIntegrationFact]
    public async Task InsertStampsActorAndEqualDatesAndDuplicateSaveKeepsOriginalAudit()
    {
        await using var db = await PostgresTests.OpenAsync();
        var artifact = Sql.Hex64();
        var batches = Batches(2);
        await Sql.SavePlanAsync(db, artifact, batches, "raw-fanout-worker");

        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_artifact_dispatch WHERE artifact_id=@a
              AND inserted_by='raw-fanout-worker' AND updated_by='raw-fanout-worker' AND created_date_utc=updated_date_utc
            """, ("a", artifact)));
        Assert.Equal(2L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_artifact_batch WHERE artifact_id=@a
              AND inserted_by='raw-fanout-worker' AND created_date_utc=updated_date_utc
            """, ("a", artifact)));

        var snapshot = await Sql.ScalarAsync<string>(db, """
            SELECT string_agg(created_date_utc::text||updated_date_utc::text||inserted_by||updated_by, '|' ORDER BY batch_ordinal)
            FROM control.raw_artifact_batch WHERE artifact_id=@a
            """, ("a", artifact));
        await Sql.SavePlanAsync(db, artifact, batches, "another-service");
        Assert.Equal(snapshot, await Sql.ScalarAsync<string>(db, """
            SELECT string_agg(created_date_utc::text||updated_date_utc::text||inserted_by||updated_by, '|' ORDER BY batch_ordinal)
            FROM control.raw_artifact_batch WHERE artifact_id=@a
            """, ("a", artifact)));
        Assert.Equal("raw-fanout-worker", await Sql.ScalarAsync<string>(db,
            "SELECT inserted_by FROM control.raw_artifact_dispatch WHERE artifact_id=@a", ("a", artifact)));
    }

    [ExternalIntegrationFact]
    public async Task BlankActorsAndEmptyPlansAreRejected()
    {
        await using var db = await PostgresTests.OpenAsync();
        var artifact = Sql.Hex64();
        var batches = Batches(1);

        foreach (var blank in new[] { "", "   " })
        {
            var save = await Assert.ThrowsAsync<PostgresException>(() => Sql.SavePlanAsync(db, artifact, batches, blank));
            Assert.Contains("actor is required", save.MessageText);
        }
        await Sql.SavePlanAsync(db, artifact, batches, "raw-fanout-worker");
        var dispatch = await Assert.ThrowsAsync<PostgresException>(() =>
            Sql.ExecAsync(db, "SELECT control.raw_artifact_mark_dispatched(@a,'')", ("a", artifact)));
        Assert.Contains("actor is required", dispatch.MessageText);
        var ack = await Assert.ThrowsAsync<PostgresException>(() =>
            Sql.ExecAsync(db, "SELECT control.raw_artifact_ack(@a,@b,' ')", ("a", artifact), ("b", batches[0])));
        Assert.Contains("actor is required", ack.MessageText);

        var empty = await Assert.ThrowsAsync<PostgresException>(() =>
            Sql.SavePlanAsync(db, Sql.Hex64(), [], "raw-fanout-worker"));
        Assert.Contains("Empty artifact plan", empty.MessageText);

        var insert = await Assert.ThrowsAsync<PostgresException>(() => Sql.ExecAsync(db, """
            INSERT INTO control.raw_fan_out_dead_letter(incident_id,queue_url,message_id,original_body,attributes,inserted_by)
            VALUES (gen_random_uuid(),'q','blank','x','{}','  ')
            """));
        Assert.Equal("23502", insert.SqlState);
        var update = await Assert.ThrowsAsync<PostgresException>(() => Sql.ExecAsync(db,
            "UPDATE control.raw_artifact_dispatch SET fan_out_completed_at_utc=now(), updated_by='' WHERE artifact_id=@a", ("a", artifact)));
        Assert.Equal("23502", update.SqlState);
    }

    [ExternalIntegrationFact]
    public async Task CreationFieldsAreImmutableAndUpdatesWithoutActorAreRejected()
    {
        await using var db = await PostgresTests.OpenAsync();
        var artifact = Sql.Hex64();
        await Sql.SavePlanAsync(db, artifact, Batches(1), "raw-fanout-worker");

        var created = await Assert.ThrowsAsync<PostgresException>(() => Sql.ExecAsync(db,
            "UPDATE control.raw_artifact_dispatch SET created_date_utc=now(), updated_by='x' WHERE artifact_id=@a", ("a", artifact)));
        Assert.Equal("23514", created.SqlState);
        var inserter = await Assert.ThrowsAsync<PostgresException>(() => Sql.ExecAsync(db,
            "UPDATE control.raw_artifact_dispatch SET inserted_by='x', updated_by='x' WHERE artifact_id=@a", ("a", artifact)));
        Assert.Equal("23514", inserter.SqlState);

        var missingActor = await Assert.ThrowsAsync<PostgresException>(() => Sql.ExecAsync(db,
            "UPDATE control.raw_artifact_batch SET materialized_at_utc=now() WHERE artifact_id=@a", ("a", artifact)));
        Assert.Contains("updated_by must be set explicitly", missingActor.MessageText);

        // An explicit actor with no business change is a no-op and must not re-attribute the row.
        var before = await Sql.ScalarAsync<string>(db,
            "SELECT updated_date_utc::text||updated_by FROM control.raw_artifact_batch WHERE artifact_id=@a", ("a", artifact));
        await Sql.ExecAsync(db,
            "UPDATE control.raw_artifact_batch SET message_body=message_body, updated_by='intruder' WHERE artifact_id=@a", ("a", artifact));
        Assert.Equal(before, await Sql.ScalarAsync<string>(db,
            "SELECT updated_date_utc::text||updated_by FROM control.raw_artifact_batch WHERE artifact_id=@a", ("a", artifact)));
    }

    [ExternalIntegrationFact]
    public async Task AcknowledgementRecordsTheActualServiceAndDuplicatesChangeNothing()
    {
        await using var db = await PostgresTests.OpenAsync();
        var artifact = Sql.Hex64();
        var batches = Batches(2);
        await Sql.SavePlanAsync(db, artifact, batches, "raw-fanout-worker");
        await Sql.ExecAsync(db, "SELECT control.raw_artifact_mark_dispatched(@a,'raw-fanout-worker')", ("a", artifact));

        await Sql.ExecAsync(db, "SELECT control.raw_artifact_ack(@a,@b,'harmonizer-a')", ("a", artifact), ("b", batches[0]));
        Assert.Null(await Sql.ScalarAsync<DateTime?>(db,
            "SELECT harmonization_completed_at_utc FROM control.raw_artifact_dispatch WHERE artifact_id=@a", ("a", artifact)));
        await Sql.ExecAsync(db, "SELECT control.raw_artifact_ack(@a,@b,'harmonizer-b')", ("a", artifact), ("b", batches[1]));

        Assert.Equal("harmonizer-a", await Sql.ScalarAsync<string>(db,
            "SELECT updated_by FROM control.raw_artifact_batch WHERE artifact_id=@a AND batch_id=@b", ("a", artifact), ("b", batches[0])));
        Assert.Equal("harmonizer-b", await Sql.ScalarAsync<string>(db,
            "SELECT updated_by FROM control.raw_artifact_batch WHERE artifact_id=@a AND batch_id=@b", ("a", artifact), ("b", batches[1])));
        Assert.Equal("harmonizer-b", await Sql.ScalarAsync<string>(db,
            "SELECT updated_by FROM control.raw_artifact_dispatch WHERE artifact_id=@a", ("a", artifact)));

        var stable = await Sql.ScalarAsync<string>(db, """
            SELECT d.harmonization_completed_at_utc::text||d.fan_out_completed_at_utc::text||d.updated_date_utc::text||d.updated_by
                   ||(SELECT string_agg(b.materialized_at_utc::text||b.updated_date_utc::text||b.updated_by,'|' ORDER BY b.batch_ordinal)
                      FROM control.raw_artifact_batch b WHERE b.artifact_id=d.artifact_id)
            FROM control.raw_artifact_dispatch d WHERE d.artifact_id=@a
            """, ("a", artifact));
        await Sql.ExecAsync(db, "SELECT control.raw_artifact_ack(@a,@b,'intruder')", ("a", artifact), ("b", batches[0]));
        await Sql.ExecAsync(db, "SELECT control.raw_artifact_mark_dispatched(@a,'intruder')", ("a", artifact));
        Assert.Equal(stable, await Sql.ScalarAsync<string>(db, """
            SELECT d.harmonization_completed_at_utc::text||d.fan_out_completed_at_utc::text||d.updated_date_utc::text||d.updated_by
                   ||(SELECT string_agg(b.materialized_at_utc::text||b.updated_date_utc::text||b.updated_by,'|' ORDER BY b.batch_ordinal)
                      FROM control.raw_artifact_batch b WHERE b.artifact_id=d.artifact_id)
            FROM control.raw_artifact_dispatch d WHERE d.artifact_id=@a
            """, ("a", artifact)));

        var unknown = await Assert.ThrowsAsync<PostgresException>(() =>
            Sql.ExecAsync(db, "SELECT control.raw_artifact_ack(@a,@b,'harmonizer-a')", ("a", artifact), ("b", Sql.Hex64())));
        Assert.Contains("Unknown artifact batch", unknown.MessageText);
    }

    [ExternalIntegrationFact]
    public async Task AcknowledgementArrivingBeforeFanOutCompletionIsHonouredByDispatchMarking()
    {
        await using var db = await PostgresTests.OpenAsync();
        var artifact = Sql.Hex64();
        var batches = Batches(2);
        await Sql.SavePlanAsync(db, artifact, batches, "raw-fanout-worker");

        foreach (var batch in batches)
            await Sql.ExecAsync(db, "SELECT control.raw_artifact_ack(@a,@b,'harmonizer')", ("a", artifact), ("b", batch));
        // Materialized children alone must not complete the parent while fan-out is still unfinished.
        Assert.Null(await Sql.ScalarAsync<DateTime?>(db,
            "SELECT harmonization_completed_at_utc FROM control.raw_artifact_dispatch WHERE artifact_id=@a", ("a", artifact)));

        await Sql.ExecAsync(db, "SELECT control.raw_artifact_mark_dispatched(@a,'raw-fanout-worker')", ("a", artifact));

        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_artifact_dispatch WHERE artifact_id=@a
              AND fan_out_completed_at_utc IS NOT NULL AND harmonization_completed_at_utc IS NOT NULL
            """, ("a", artifact)));
    }

    [ExternalIntegrationFact]
    public async Task ConcurrentAcknowledgementsAndDispatchMarkingCompleteTheParentExactlyOnce()
    {
        await using var db = await PostgresTests.OpenAsync();
        var artifact = Sql.Hex64();
        var batches = Batches(4);
        await Sql.SavePlanAsync(db, artifact, batches, "raw-fanout-worker");

        var tasks = batches.Select((batch, i) =>
                Sql.ExecAsync(db, "SELECT control.raw_artifact_ack(@a,@b,@actor)", ("a", artifact), ("b", batch), ("actor", $"harmonizer-{i}")))
            .Append(Sql.ExecAsync(db, "SELECT control.raw_artifact_mark_dispatched(@a,'raw-fanout-worker')", ("a", artifact)));
        await Task.WhenAll(tasks);

        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_artifact_dispatch d WHERE d.artifact_id=@a
              AND d.fan_out_completed_at_utc IS NOT NULL AND d.harmonization_completed_at_utc IS NOT NULL
              AND d.harmonization_completed_at_utc >= (SELECT max(materialized_at_utc) FROM control.raw_artifact_batch b WHERE b.artifact_id=d.artifact_id)
            """, ("a", artifact)));
        Assert.Equal(4L, await Sql.ScalarAsync<long>(db,
            "SELECT count(*) FROM control.raw_artifact_batch WHERE artifact_id=@a AND materialized_at_utc IS NOT NULL AND updated_by LIKE 'harmonizer-%'",
            ("a", artifact)));
    }

    [ExternalIntegrationFact]
    public async Task ActorIdentityDoesNotLeakAcrossAPooledConnection()
    {
        await using var db = await PostgresTests.OpenAsync();
        var single = new NpgsqlConnectionStringBuilder(db.ConnectionString) { MaxPoolSize = 1, MinPoolSize = 0 };
        await using var pooled = NpgsqlDataSource.Create(single.ConnectionString);
        var artifact = Sql.Hex64();
        var batches = Batches(3);
        await Sql.SavePlanAsync(pooled, artifact, batches, "raw-fanout-worker");

        // One physical connection serves every call in turn, so any session-level actor state would leak.
        for (var i = 0; i < batches.Length; i++)
            await Sql.ExecAsync(pooled, "SELECT control.raw_artifact_ack(@a,@b,@actor)", ("a", artifact), ("b", batches[i]), ("actor", $"svc-{i}"));

        var actors = new List<string>();
        await using var read = pooled.CreateCommand(
            "SELECT updated_by FROM control.raw_artifact_batch WHERE artifact_id=@a ORDER BY batch_ordinal");
        read.Parameters.AddWithValue("a", artifact);
        await using (var reader = await read.ExecuteReaderAsync())
            while (await reader.ReadAsync()) actors.Add(reader.GetString(0));
        Assert.Equal(new[] { "svc-0", "svc-1", "svc-2" }, actors);
    }
}
