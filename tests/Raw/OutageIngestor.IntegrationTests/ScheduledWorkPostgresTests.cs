using Npgsql;
using OutageIngestor.Configuration;
using OutageIngestor.Enums;
using OutageIngestor.Models;
using OutageIngestor.Repositories;

namespace OutageIngestor.IntegrationTests;

/// <summary>
/// Postgres-backed behavior of <see cref="PostgresExtractionSchedulingRepository"/> against the
/// shared `extraction_cursor`/`extraction_run` schema, scoped to source_type = 'Outage'. Mirrors
/// ProductionIngestor.IntegrationTests.ScheduledWorkPostgresTests, preserving Outage's independent
/// live/deleted checkpoints and full-snapshot refresh with integer IDs and joined DART names.
/// </summary>
public sealed class ScheduledWorkPostgresTests
{
    [OutagePostgresFact]
    public async Task ConcurrentClaimsHaveDistinctFarmsAndAtomicHistory()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        var claims = await Task.WhenAll(fixture.ClaimAsync("first"), fixture.ClaimAsync("second"));
        Assert.All(claims, claim => Assert.NotNull(claim));
        Assert.Equal(2, claims.Select(claim => claim!.Request.WindfarmId).Distinct().Count());
        Assert.All(claims, claim =>
        {
            Assert.Equal($"farm-{claim!.Request.WindfarmId}", claim.Request.WindfarmName);
            Assert.Equal(claim.Request.WindfarmId, claim.Lease.WindfarmId);
            Assert.Equal(claim.Request.WindfarmName, claim.Lease.WindfarmName);
            Assert.Equal(claim.Request.WindfarmId, claim.Run.WindfarmId);
            Assert.Equal($"/raw/outage/windfarm={claim.Lease.WindfarmName}/mode=incremental/extraction={claim.Lease.LeaseToken}/", claim.Run.OutputPath);
        });
        Assert.Null(await fixture.ClaimAsync("third"));
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Running';"));
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_cursor WHERE active_run_id IS NOT NULL;"));
    }

    [OutagePostgresFact]
    public async Task DisabledAndOtherSourceRowsAreNotSelected()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        await fixture.ExecuteAsync("""
            UPDATE extraction_cursor SET is_enabled = FALSE WHERE windfarm_id = 2;
            INSERT INTO extraction_cursor(windfarm_id, source_type, is_enabled, last_committed_updated_at)
            VALUES (1, 'Production', TRUE, '2026-09-01Z');
            """);
        Assert.True(await fixture.Repository.IsRegisteredAsync(new(new("Outage"), 1), default));
        Assert.False(await fixture.Repository.IsRegisteredAsync(new(new("Outage"), 2), default));
        Assert.Equal(1, (await fixture.ClaimAsync("owner"))!.Request.WindfarmId);
        Assert.Null(await fixture.ClaimAsync("another"));
        var production = await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.Request with { Scope = new("Production") }, default);
        Assert.NotNull(production);
        Assert.Equal(1, production.Lease.LeaseToken);
    }

    [OutagePostgresFact]
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
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_cursor WHERE deleted_last_committed_updated_at = '2026-09-03Z' AND deleted_last_committed_id = 'deleted-next-id';"));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_cursor WHERE deleted_last_committed_updated_at = '2026-09-01Z' AND deleted_last_committed_id = '0';"));
    }

    [OutagePostgresFact]
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
        Assert.False(await cursors.TryRenewLeaseAsync(1, "Outage", "old", old.Lease.LeaseToken, TimeSpan.FromMinutes(5), default));
        await cursors.ReleaseLeaseAsync(1, "Outage", old.Lease.LeaseToken, default);
        Assert.True(await cursors.TryRenewLeaseAsync(1, "Outage", "new", replacement.Lease.LeaseToken, TimeSpan.FromMinutes(5), default));
        Assert.True(await fixture.FinishAsync(replacement, RunOutcome.Success));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Failed' AND error_message = 'LeaseExpired';"));
    }

    [OutagePostgresFact]
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
            Assert.Equal("farm-1", refresh.Request.WindfarmName);
            Assert.Equal($"/raw/outage/windfarm=farm-1/mode=refresh/extraction={refresh.Lease.LeaseToken}/", refresh.Run.OutputPath);
            Assert.True(await fixture.FinishAsync(refresh, outcome));
            Assert.Equal(before, await fixture.ScalarAsync(DatabaseFixture.IncrementalStateSql));
        }
        var history = await new PostgresExtractionRunHistoryRepository(fixture.ConnectionString)
            .GetAttemptsAsync(new("refresh-1"), default);
        Assert.Equal(2, history.Count);
        Assert.All(history, attempt => Assert.Equal(1, attempt.WindfarmId));
        Assert.Equal(2L, await fixture.ScalarAsync("""
            SELECT count(*) FROM extraction_run WHERE request_id = 'refresh-1'
                AND job_name = 'outage-refresh-job' AND requested_by = 'operator' AND reason = 'historical replay';
            """));
    }

    [OutagePostgresFact]
    public async Task BusyRefreshRecordsSeparateAttemptAndCannotChangeLease()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        var owner = (await fixture.ClaimAsync("owner"))!;
        Assert.Null(await fixture.Repository.TryClaimCursorAndStartExtractionRunAsync(fixture.RefreshRequest, default));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Busy' AND lease_token IS NULL;"));
        Assert.True(await fixture.FinishAsync(owner, RunOutcome.Success));
    }

    [OutagePostgresFact]
    public async Task DeletedCheckpointCannotRegressAndRollsBackLiveCheckpointAndAudit()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        var claim = (await fixture.ClaimAsync("owner"))!;
        var invalid = new RunResultModel(RunOutcome.Success, 0,
            CompletedCheckpointUpdatedUtc: DatabaseFixture.InitialDate.AddDays(1),
            CompletedCheckpointId: "live-next",
            CompletedDeletedCheckpointUpdatedUtc: DatabaseFixture.InitialDate.AddDays(-1),
            CompletedDeletedCheckpointId: "deleted-old");

        Assert.False(await fixture.Repository.TryFinalizeAsync(new(claim, invalid), default));
        var current = await new PostgresExtractionCursorRepository(fixture.ConnectionString)
            .GetCurrentAsync(claim.Lease.WindfarmId, "Outage", default);
        Assert.NotNull(current);
        Assert.Equal(claim.Lease.LastUpdatedUtc, current.LastUpdatedUtc);
        Assert.Equal(claim.Lease.DeletedLastUpdatedUtc, current.DeletedLastUpdatedUtc);
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run WHERE status = 'Running';"));
        Assert.True(await fixture.FinishAsync(claim, RunOutcome.Success));
    }

    [OutagePostgresFact]
    public async Task DirectCompletionSeedsDeletedCheckpointUsingLeaseFallbackAndIsAtomic()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET deleted_last_committed_updated_at = NULL, deleted_last_committed_id = NULL;");
        var cursors = new PostgresExtractionCursorRepository(fixture.ConnectionString);
        var lease = (await cursors.TryAcquireLeaseAsync(1, "Outage", "direct-owner", TimeSpan.FromMinutes(5), default))!;
        Assert.Equal("farm-1", lease.WindfarmName);
        Assert.Equal(lease.LastUpdatedUtc, lease.DeletedLastUpdatedUtc);
        var run = await cursors.StartRunAsync(1, "Outage", lease.LeaseToken,
            lease.LastUpdatedUtc, lease.LastCommittedId, null, default);
        Assert.NotNull(run);
        Assert.Equal("direct-owner", await fixture.ScalarAsync("SELECT created_by FROM extraction_run;"));

        // An absent run must roll back both checkpoint updates, not just its own audit write.
        Assert.False(await cursors.TryCompleteRunAsync(-1, 1, "Outage", lease.LeaseToken,
            lease.LastUpdatedUtc.AddDays(1), "live-next", default,
            expectedLastUpdatedUtc: lease.LastUpdatedUtc,
            newDeletedLastUpdatedUtc: lease.DeletedLastUpdatedUtc.AddDays(2), newDeletedLastCommittedId: "deleted-next",
            expectedDeletedLastUpdatedUtc: lease.DeletedLastUpdatedUtc));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_cursor WHERE windfarm_id = 1 AND last_committed_updated_at = '2026-09-01Z' AND deleted_last_committed_updated_at IS NULL;"));

        Assert.True(await cursors.TryCompleteRunAsync(run.RunId, 1, "Outage", lease.LeaseToken,
            lease.LastUpdatedUtc.AddDays(1), "live-next", default,
            expectedLastUpdatedUtc: lease.LastUpdatedUtc,
            newDeletedLastUpdatedUtc: lease.DeletedLastUpdatedUtc.AddDays(2), newDeletedLastCommittedId: "deleted-next",
            expectedDeletedLastUpdatedUtc: lease.DeletedLastUpdatedUtc));
        var completed = (await cursors.GetCurrentAsync(1, "Outage", default))!;
        Assert.Equal(lease.LastUpdatedUtc.AddDays(1), completed.LastUpdatedUtc);
        Assert.Equal(lease.DeletedLastUpdatedUtc.AddDays(2), completed.DeletedLastUpdatedUtc);
        Assert.Equal("live-next", completed.LastCommittedId);
        Assert.Equal("deleted-next", completed.DeletedLastCommittedId);
    }

    [OutagePostgresFact]
    public async Task ExpiredDirectLeaseCannotStartRunOrReleaseLease()
    {
        await using var fixture = await DatabaseFixture.CreateAsync();
        var cursors = new PostgresExtractionCursorRepository(fixture.ConnectionString);
        var lease = (await cursors.TryAcquireLeaseAsync(1, "Outage", "expired-owner", TimeSpan.FromMinutes(5), default))!;
        await fixture.ExecuteAsync("UPDATE extraction_cursor SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE windfarm_id = 1;");
        Assert.Null(await cursors.StartRunAsync(1, "Outage", lease.LeaseToken,
            lease.LastUpdatedUtc, lease.LastCommittedId, null, default));
        await cursors.ReleaseLeaseAsync(1, "Outage", lease.LeaseToken, default);
        Assert.Equal("expired-owner", await fixture.ScalarAsync("SELECT lease_owner FROM extraction_cursor WHERE windfarm_id = 1;"));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT count(*) FROM extraction_run;"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DirectCompletionRejectsPartialDeletedCheckpointBeforeOpeningDatabase(bool timestampOnly)
    {
        var cursors = new PostgresExtractionCursorRepository("not a connection string");
        await Assert.ThrowsAsync<ArgumentException>(() => cursors.TryCompleteRunAsync(1, 1, "Outage", 1,
            DatabaseFixture.InitialDate, "live", default,
            newDeletedLastUpdatedUtc: timestampOnly ? DatabaseFixture.InitialDate : null,
            newDeletedLastCommittedId: timestampOnly ? null : "deleted"));
    }

    [Fact]
    public async Task FailureAuditDoesNotMaskOriginalFailureWhenConnectionSetupFails()
    {
        var cursors = new PostgresExtractionCursorRepository("not a connection string");
        await cursors.FailRunAsync(1, 1, "Outage", 1, null, null, "original failure", default);
    }

    private sealed class DatabaseFixture : IAsyncDisposable
    {
        public static readonly DateTime InitialDate = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        public const string IncrementalStateSql = """
            SELECT jsonb_build_array(last_committed_updated_at, last_committed_id, last_committed_utc,
                deleted_last_committed_updated_at, deleted_last_committed_id,
                last_run_started_utc, last_run_completed_utc, last_run_outcome, consecutive_failures, last_error)::text
            FROM extraction_cursor WHERE windfarm_id = 1 AND source_type = 'Outage';
            """;
        private readonly string _schema;
        public string ConnectionString { get; }
        public PostgresExtractionSchedulingRepository Repository { get; }
        public ExtractionClaimRequestModel Request { get; } = new(new("Outage"), "owner", new IngestionOptions());
        public ExtractionClaimRequestModel RefreshRequest => Request with
        {
            Refresh = new(1, "untrusted-request-name", "Outage", IngestionMode.Refresh, "refresh-1"),
            RequestedBy = "operator",
            Reason = "historical replay",
            JobName = "outage-refresh-job"
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
            var schema = $"outage_test_{Guid.NewGuid():N}";
            settings.SearchPath = schema;
            var fixture = new DatabaseFixture(settings.ConnectionString, schema);
            await fixture.ExecuteAsync($"CREATE SCHEMA {schema};");
            try
            {
                await fixture.ExecuteAsync(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Schema", "cursors.sql")));
                await fixture.ExecuteAsync(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Schema", "extraction_run_query_bounds.sql")));
                await fixture.ExecuteAsync(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Schema", "deleted_feed_checkpoint.sql")));
                await fixture.ExecuteAsync("""
                    CREATE TABLE windfarm (id INTEGER PRIMARY KEY, windfarm_name TEXT NOT NULL);
                    INSERT INTO windfarm(id, windfarm_name) VALUES (1, 'farm-1'), (2, 'farm-2');
                    INSERT INTO extraction_cursor(windfarm_id, source_type, is_enabled, last_committed_updated_at,
                        deleted_last_committed_updated_at, deleted_last_committed_id)
                    VALUES (1, 'Outage', TRUE, '2026-09-01Z', '2026-09-01Z', '0'),
                           (2, 'Outage', TRUE, '2026-09-01Z', '2026-09-01Z', '0');
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
            => Repository.TryFinalizeAsync(new(claim, new(outcome, 0,
                CompletedCheckpointUpdatedUtc: outcome == RunOutcome.Success ? InitialDate.AddDays(1) : null,
                CompletedCheckpointId: outcome == RunOutcome.Success ? "next-id" : null,
                CompletedDeletedCheckpointUpdatedUtc: outcome == RunOutcome.Success ? InitialDate.AddDays(2) : null,
                CompletedDeletedCheckpointId: outcome == RunOutcome.Success ? "deleted-next-id" : null)), default);
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

public sealed class OutagePostgresFactAttribute : FactAttribute
{
    public OutagePostgresFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("OUTAGE_POSTGRES_INTEGRATION") != "1")
            Skip = "Set OUTAGE_POSTGRES_INTEGRATION=1 and start the existing raw-fanout PostgreSQL test service.";
    }
}
