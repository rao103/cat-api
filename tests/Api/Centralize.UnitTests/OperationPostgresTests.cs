using Centralize.Core.Repositories;
using Npgsql;

namespace Centralize.UnitTests;

public sealed class OperationPostgresTests
{
    [OperationPostgresFact]
    public async Task Create_WritesOnlyOperationWithoutExtractionTables()
    {
        var connectionString = Environment.GetEnvironmentVariable("CENTRAL_OPERATION_TEST_POSTGRES")!;
        var settings = new NpgsqlConnectionStringBuilder(connectionString);
        if (settings.Host != "127.0.0.1" || settings.Port != 55591 || settings.Database != "central_operation_test")
            throw new InvalidOperationException("Only the dedicated, empty local central_operation_test database is allowed.");
        await using var source = NpgsqlDataSource.Create(connectionString);
        await using var connection = await source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var empty = new NpgsqlCommand(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname IN ('public','control'));", connection, transaction);
        if ((bool)(await empty.ExecuteScalarAsync())!)
            throw new InvalidOperationException("The dedicated test database must be empty.");
        await using var schema = new NpgsqlCommand("""
            CREATE SCHEMA control;
            CREATE TABLE control.operation(
                operation_id uuid PRIMARY KEY,
                requested_by text NOT NULL,
                created_date_utc timestamptz NOT NULL DEFAULT clock_timestamp());
            """, connection, transaction);
        await schema.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
        try
        {
            var repository = new PostgresOperationRepository(source);
            var operation = await repository.CreateAsync("caller", default);
            Assert.NotEqual(Guid.Empty, operation.OperationId);
            Assert.Empty(operation.Runs);
            Assert.Empty(operation.Stages);
            await using var check = new NpgsqlCommand(
                "SELECT operation_id, requested_by FROM control.operation;", connection);
            await using var reader = await check.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(operation.OperationId, reader.GetGuid(0));
            Assert.Equal("caller", reader.GetString(1));
            Assert.False(await reader.ReadAsync());
        }
        finally
        {
            await using var cleanup = new NpgsqlCommand("DROP TABLE control.operation; DROP SCHEMA control;", connection);
            await cleanup.ExecuteNonQueryAsync();
        }
    }
    [OperationPostgresFact]
    public async Task Get_PreservesTracking_WhenStagesAreMissingEmptyPendingCompletedOrFailed()
    {
        var settings = new NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable("CENTRAL_OPERATION_TEST_POSTGRES")!);
        if (settings.Host != "127.0.0.1" || settings.Port != 55591 || settings.Database != "central_operation_test")
            throw new InvalidOperationException("Only the dedicated, empty local central_operation_test database is allowed.");
        await using var source = NpgsqlDataSource.Create(settings.ConnectionString);
        await using var connection = await source.OpenConnectionAsync();
        await using (var empty = new NpgsqlCommand(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname IN ('public','control'));", connection))
            if ((bool)(await empty.ExecuteScalarAsync())!)
                throw new InvalidOperationException("The dedicated test database must be empty.");
        await using (var schema = new NpgsqlCommand("""
            CREATE SCHEMA control;
            CREATE TABLE control.operation(operation_id uuid PRIMARY KEY, requested_by text NOT NULL,
                created_date_utc timestamptz NOT NULL DEFAULT clock_timestamp());
            CREATE TABLE public.extraction_run(id bigint, windfarm_id integer, source_type text, execution_mode text,
                status text, requested_dates date[], started_at timestamptz, completed_at timestamptz,
                error_message text, operation_id uuid);
            """, connection))
            await schema.ExecuteNonQueryAsync();
        try
        {
            var repository = new PostgresOperationRepository(source);
            var operation = await repository.CreateAsync("caller", default);
            Assert.Null(await repository.GetAsync(operation.OperationId, "other-caller", default));
            Assert.Null(await repository.GetAsync(Guid.NewGuid(), "caller", default));
            var result = await repository.GetAsync(operation.OperationId, "caller", default);
            Assert.NotNull(result);
            Assert.Equal(operation.OperationId, result.OperationId);
            Assert.Empty(result.Runs);
            Assert.Equal(3, result.Stages.Count);
            Assert.All(result.Stages, stage => Assert.Equal("Unavailable", stage.Status));
            await using (var schema = new NpgsqlCommand("""
                CREATE TABLE control.raw_artifact_dispatch(original_envelope jsonb, fan_out_completed_at_utc timestamptz);
                CREATE TABLE control.harmonizer_batch(operation_id text, completed_at_utc timestamptz, last_error_code text);
                CREATE TABLE control.harmonizer_outbox(operation_id text, sent_at_utc timestamptz);
                """, connection))
                await schema.ExecuteNonQueryAsync();
            result = await repository.GetAsync(operation.OperationId, "caller", default);
            Assert.All(result!.Stages, stage =>
            {
                Assert.Equal("NotObserved", stage.Status);
                Assert.Equal(0, stage.Total);
            });
            await using (var seed = new NpgsqlCommand("""
                INSERT INTO public.extraction_run VALUES
                    (1,2,'Production','Refresh','Completed',ARRAY['2026-09-01'::date],now(),now(),NULL,@id);
                INSERT INTO control.raw_artifact_dispatch VALUES (jsonb_build_object('OperationId',@text),NULL);
                INSERT INTO control.harmonizer_batch VALUES (@text,NULL,NULL),('other-operation',now(),NULL);
                INSERT INTO control.harmonizer_outbox VALUES (@text,NULL);
                """, connection))
            {
                seed.Parameters.AddWithValue("id", operation.OperationId);
                seed.Parameters.AddWithValue("text", operation.OperationId.ToString("D"));
                await seed.ExecuteNonQueryAsync();
            }
            result = await repository.GetAsync(operation.OperationId, "caller", default);
            Assert.Equal("Completed", Assert.Single(result!.Runs).Status);
            Assert.All(result.Stages, stage =>
            {
                Assert.Equal("Pending", stage.Status);
                Assert.Equal(1, stage.Total);
                Assert.Equal(0, stage.Completed);
                Assert.Equal(0, stage.Failed);
            });
            await using (var complete = new NpgsqlCommand("""
                UPDATE control.raw_artifact_dispatch SET fan_out_completed_at_utc=now();
                UPDATE control.harmonizer_batch SET completed_at_utc=now();
                UPDATE control.harmonizer_outbox SET sent_at_utc=now();
                """, connection))
                await complete.ExecuteNonQueryAsync();
            result = await repository.GetAsync(operation.OperationId, "caller", default);
            Assert.All(result!.Stages, stage =>
            {
                Assert.Equal("Completed", stage.Status);
                Assert.Equal(1, stage.Completed);
            });
            await using (var fail = new NpgsqlCommand(
                "UPDATE control.harmonizer_batch SET completed_at_utc=NULL,last_error_code='test-failure';", connection))
                await fail.ExecuteNonQueryAsync();
            result = await repository.GetAsync(operation.OperationId, "caller", default);
            var failed = Assert.Single(result!.Stages, stage => stage.Stage == "Harmonizer");
            Assert.Equal("Failed", failed.Status);
            Assert.Equal(1, failed.Failed);
            Assert.Equal(0, failed.Completed);
            await using (var remove = new NpgsqlCommand("DROP TABLE control.harmonizer_batch;", connection))
                await remove.ExecuteNonQueryAsync();
            result = await repository.GetAsync(operation.OperationId, "caller", default);
            Assert.Equal("Unavailable", Assert.Single(result!.Stages, stage => stage.Stage == "Harmonizer").Status);
            Assert.Equal("Completed", Assert.Single(result.Stages, stage => stage.Stage == "RawFanOut").Status);
        }
        finally
        {
            await using var cleanup = new NpgsqlCommand("""
                DROP TABLE IF EXISTS control.harmonizer_batch,control.harmonizer_outbox,
                    control.raw_artifact_dispatch,public.extraction_run,control.operation;
                DROP SCHEMA control;
                """, connection);
            await cleanup.ExecuteNonQueryAsync();
        }
    }

}

public sealed class OperationPostgresFactAttribute : FactAttribute
{
    public OperationPostgresFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("CENTRAL_OPERATION_TEST_POSTGRES")))
            Skip = "Requires the dedicated empty local PostgreSQL fixture; never runs against CAT.";
    }
}
