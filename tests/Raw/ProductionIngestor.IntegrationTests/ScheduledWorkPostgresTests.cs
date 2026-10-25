using Npgsql;
using ProductionIngestor.Configuration;
using ProductionIngestor.Enums;
using ProductionIngestor.Models;
using ProductionIngestor.Repositories;

namespace ProductionIngestor.IntegrationTests;

public sealed class ScheduledWorkPostgresTests
{
    [ProductionPostgresFact]
    public async Task ConcurrentClaimsHaveDistinctFarmsAndAtomicHistory()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        var claims = await Task.WhenAll(fixture.ClaimAsync("first"), fixture.ClaimAsync("second"));
        Assert.All(claims, claim => Assert.NotNull(claim));
        Assert.Equal(2, claims.Select(claim => claim!.Request.WindfarmId).Distinct().Count());
        Assert.Null(await fixture.ClaimAsync("third"));
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Running';"));
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_cursor WHERE active_run_id IS NOT NULL;"));
    }

    [ProductionPostgresFact]
    public async Task DisabledAndOtherSourceRowsAreNotSelected()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        await fixture.ExecuteAsync("""
            UPDATE extraction_cursor SET is_enabled = FALSE WHERE windfarm_id = 2;
            INSERT INTO extraction_cursor(windfarm_id, source_type, is_enabled, last_committed_updated_at)
            VALUES (1, 'Alarm', TRUE, '2026-09-01Z');
            """);
        Assert.True(await fixture.Repository.IsRegisteredAsync(new(new("Production"), 1), default));
        Assert.False(await fixture.Repository.IsRegisteredAsync(new(new("Production"), 2), default));
        Assert.Equal(1, (await fixture.ClaimAsync("owner"))!.Request.WindfarmId);
        Assert.Null(await fixture.ClaimAsync("another"));
        var alarm = await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.Request with { Scope = new("Alarm") }, default);
        Assert.NotNull(alarm);
        Assert.Equal(1, alarm.Lease.LeaseToken);
    }

    [ProductionPostgresFact]
    public async Task SuccessfulIncrementalFinalizationAdvancesCheckpointAndFailureDoesNot()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        var success = (await fixture.ClaimAsync("success"))!;
        var failure = (await fixture.ClaimAsync("failure"))!;
        Assert.True(await fixture.FinishAsync(success, RunOutcome.Success));
        Assert.True(await fixture.FinishAsync(failure, RunOutcome.Failed));
        Assert.Null(await fixture.ClaimAsync("early"));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_cursor WHERE last_committed_updated_at = '2026-09-02Z' AND last_committed_id = 'next-id' AND last_committed_utc IS NOT NULL;"));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_cursor WHERE last_committed_updated_at = '2026-09-01Z' AND last_committed_utc IS NULL;"));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Completed' AND to_updated_at = '2026-09-02Z' AND to_committed_id = 'next-id';"));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Failed' AND to_updated_at = '2026-09-01Z' AND to_committed_id = '0';"));
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET last_run_started_utc = clock_timestamp() - interval '7 minutes', last_run_completed_utc = clock_timestamp() - interval '6 minutes';");
        var retry = (await fixture.ClaimAsync("retry"))!;
        Assert.Equal(failure.Request.WindfarmId, retry.Request.WindfarmId);
        Assert.True(await fixture.FinishAsync(retry, RunOutcome.Success));
        Assert.Equal(0, await fixture.ScalarAsync("SELECT sum(consecutive_failures)::integer FROM extraction_cursor;"));
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET last_run_started_utc = clock_timestamp() - interval '62 minutes', last_run_completed_utc = clock_timestamp() - interval '61 minutes';");
        Assert.NotNull(await fixture.ClaimAsync("due"));
    }

    [ProductionPostgresFact]
    public async Task ExpiredIncrementalRecoversImmediatelyAndOldOwnerIsFenced()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET is_enabled = FALSE WHERE windfarm_id = 2;");
        var old = (await fixture.ClaimAsync("old"))!;
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET lease_expires_at = clock_timestamp() - interval '1 second', last_run_completed_utc = clock_timestamp(), last_run_outcome = 'Completed';");
        Assert.False(await fixture.FinishAsync(old, RunOutcome.Success));
        var replacement = (await fixture.ClaimAsync("new"))!;
        Assert.True(replacement.Lease.LeaseToken > old.Lease.LeaseToken);
        Assert.NotEqual(old.Run.RunId, replacement.Run.RunId);
        Assert.False(await fixture.FinishAsync(old, RunOutcome.Success));
        var cursors = new PostgresExtractionCursorRepository(fixture.ConnectionString);
        Assert.False(await cursors.TryRenewLeaseAsync(1, "Production", "old", old.Lease.LeaseToken, TimeSpan.FromMinutes(5), default));
        Assert.False(await cursors.TryCommitAsync(1, "Production", old.Lease.LeaseToken, DatabaseFixture.InitialDate.AddDays(1), "0", default));
        await cursors.ReleaseLeaseAsync(1, "Production", old.Lease.LeaseToken, default);
        Assert.True(await cursors.TryRenewLeaseAsync(1, "Production", "new", replacement.Lease.LeaseToken, TimeSpan.FromMinutes(5), default));
        Assert.True(await fixture.FinishAsync(replacement, RunOutcome.Success));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Failed' AND error_message = 'LeaseExpired';"));
    }

    [ProductionPostgresFact]
    public async Task RefreshBypassesDelayAndPreservesIncrementalStateOnSuccessAndFailure()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        await fixture.ExecuteAsync("""
            UPDATE extraction_cursor SET is_enabled = FALSE WHERE windfarm_id = 2;
            UPDATE extraction_cursor SET last_run_started_utc = '2026-09-02Z', last_run_completed_utc = clock_timestamp(),
                last_run_outcome = 'Failed', consecutive_failures = 3, last_error = 'OriginalFailure' WHERE windfarm_id = 1;
            """);
        var before = await fixture.ScalarAsync(DatabaseFixture.IncrementalStateSql);
        Assert.Null(await fixture.ClaimAsync("scheduled"));
        foreach (var outcome in new[] { RunOutcome.Success, RunOutcome.Failed })
        {
            var refresh = (await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.RefreshRequest, default))!;
            Assert.Equal(IngestionMode.Refresh, refresh.Request.Mode);
            Assert.True(await fixture.FinishAsync(refresh, outcome));
            Assert.Equal(before, await fixture.ScalarAsync(DatabaseFixture.IncrementalStateSql));
        }
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT count(DISTINCT id) FROM extraction_run WHERE request_id = 'refresh-1';"));
        Assert.Equal(2L, await fixture.ScalarAsync("""
            SELECT count(*) FROM extraction_run WHERE request_id = 'refresh-1'
                AND job_name = 'production-refresh-job' AND requested_by = 'operator'
                AND reason = 'historical replay' AND requested_dates = ARRAY[DATE '2026-01-01'];
            """));
        var history = await new PostgresExtractionRunHistoryRepository(fixture.ConnectionString)
            .GetAttemptsAsync(new("refresh-1"), default);
        Assert.Equal(2, history.Count);
        Assert.All(history, attempt => Assert.Equal(new DateOnly(2026, 1, 1), Assert.Single(attempt.RequestedDates)));
        Assert.Equal("Failed", history[0].Status);
        Assert.Equal("Completed", history[1].Status);
        Assert.Null(await fixture.ClaimAsync("still-not-due"));
    }

    [ProductionPostgresFact]
    public async Task BusyRefreshRecordsSeparateAttemptAndCannotChangeLease()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        var owner = (await fixture.ClaimAsync("owner"))!;
        Assert.Null(await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.RefreshRequest, default));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Busy' AND lease_token IS NULL;"));
        Assert.True(await fixture.FinishAsync(owner, RunOutcome.Success));
    }

    [ProductionPostgresFact]
    public async Task ExpiredRefreshDoesNotResetIncrementalEligibility()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET is_enabled = FALSE WHERE windfarm_id = 2;");
        var incremental = (await fixture.ClaimAsync("initial"))!;
        Assert.True(await fixture.FinishAsync(incremental, RunOutcome.Success));
        var incrementalCheckpoint = await fixture.ScalarAsync(DatabaseFixture.IncrementalStateSql);
        var refresh = (await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.RefreshRequest, default))!;
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET lease_expires_at = clock_timestamp() - interval '1 second';");
        Assert.Null(await fixture.ClaimAsync("scheduled"));
        var retry = (await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.RefreshRequest, default))!;
        Assert.NotEqual(refresh.Run.RunId, retry.Run.RunId);
        Assert.True(retry.Lease.LeaseToken > refresh.Lease.LeaseToken);
        Assert.True(await fixture.FinishAsync(retry, RunOutcome.Success));
        Assert.Equal(incrementalCheckpoint, await fixture.ScalarAsync(DatabaseFixture.IncrementalStateSql));
    }

    [ProductionPostgresFact]
    public async Task RefreshCannotHideAbandonedIncrementalWork()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET is_enabled = FALSE WHERE windfarm_id = 2;");
        Assert.NotNull(await fixture.ClaimAsync("abandoned"));
        await fixture.ExecuteAsync("""
            UPDATE extraction_cursor SET lease_expires_at = clock_timestamp() - interval '1 second',
                last_run_completed_utc = last_run_started_utc - interval '1 minute', last_run_outcome = 'Completed';
            """);
        var refresh = (await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.RefreshRequest, default))!;
        Assert.True(await fixture.FinishAsync(refresh, RunOutcome.Success));
        Assert.NotNull(await fixture.ClaimAsync("recover-incremental"));
    }

    private sealed class DatabaseFixture : IAsyncDisposable
    {
        public static readonly DateTime InitialDate = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        public const string IncrementalStateSql = """
            SELECT jsonb_build_array(last_committed_updated_at, last_committed_id, last_committed_utc,
                last_run_started_utc, last_run_completed_utc, last_run_outcome, consecutive_failures, last_error)::text
            FROM extraction_cursor WHERE windfarm_id = 1 AND source_type = 'Production';
            """;
        private readonly string _schema;
        public string ConnectionString { get; }
        public PostgresExtractionSchedulingRepository Repository { get; }
        public ExtractionClaimRequestModel Request { get; } = new(new("Production"), "owner", new IngestionOptions());
        public ExtractionClaimRequestModel RefreshRequest => Request with
        {
            Refresh = new(1, "farm-1", "Production", IngestionMode.Refresh, [new DateOnly(2026, 1, 1)], "refresh-1"),
            RequestedBy = "operator",
            Reason = "historical replay",
            JobName = "production-refresh-job"
        };

        private DatabaseFixture(string connectionString, string schema)
        {
            _schema = schema;
            ConnectionString = connectionString;
            Repository = new(connectionString);
        }

        public static async Task<DatabaseFixture> CreateAsync()
        {
            var settings = new NpgsqlConnectionStringBuilder(Environment.GetEnvironmentVariable("RAWFANOUT_TEST_POSTGRES") ??
                "Host=localhost;Port=55432;Database=rawfanout_test;Username=rawfanout_test;Password=local-test-only;SSL Mode=Disable");
            if (settings.Host is not ("localhost" or "127.0.0.1") || settings.Database != "rawfanout_test")
                throw new InvalidOperationException("Test DDL is restricted to the isolated local rawfanout_test database.");
            var schema = $"production_test_{Guid.NewGuid():N}";
            settings.SearchPath = schema;
            var fixture = new DatabaseFixture(settings.ConnectionString, schema);
            await fixture.ExecuteAsync($"CREATE SCHEMA {schema};");
            try
            {
                await fixture.ExecuteAsync(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Schema", "reference.sql")));
                await fixture.ExecuteAsync(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Schema", "extraction.sql")));
                await fixture.ExecuteAsync("""
                    INSERT INTO windfarm(id, windfarm_name) VALUES (1, 'farm-1'), (2, 'farm-2');
                    INSERT INTO extraction_cursor(windfarm_id, source_type, is_enabled, last_committed_updated_at)
                    VALUES (1, 'Production', TRUE, '2026-09-01Z'), (2, 'Production', TRUE, '2026-09-01Z');
                    """);
                return fixture;
            }
            catch
            {
                await fixture.DisposeAsync();
                throw;
            }
        }

        public Task<ExtractionClaimModel?> ClaimAsync(string owner)
            => Repository.TryClaimCursorAndStartExtractionRunAsync(Request with { OwnerId = owner }, default);
        public Task<bool> FinishAsync(ExtractionClaimModel claim, RunOutcome outcome)
            => Repository.TryFinalizeAsync(new(claim, new(outcome, [],
                CompletedCheckpointUpdatedUtc: outcome == RunOutcome.Success ? InitialDate.AddDays(1) : null,
                CompletedCheckpointId: outcome == RunOutcome.Success ? "next-id" : null)), default);
        public async Task ExecuteAsync(string sql)
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }
        public async Task<object?> ScalarAsync(string sql)
        {
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            return await command.ExecuteScalarAsync();
        }
        public async ValueTask DisposeAsync() => await ExecuteAsync($"DROP SCHEMA {_schema} CASCADE;");
    }
}

public sealed class ProductionPostgresFactAttribute : FactAttribute
{
    public ProductionPostgresFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("PRODUCTION_POSTGRES_INTEGRATION") != "1")
            Skip = "Set PRODUCTION_POSTGRES_INTEGRATION=1 and start the existing raw-fanout PostgreSQL test service.";
    }
}