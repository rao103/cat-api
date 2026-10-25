using Npgsql;
using OutageIngestor.Enums;
using OutageIngestor.Factories;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;

namespace OutageIngestor.Repositories;

/// <summary>
/// Postgres-backed, fenced implementation of <see cref="IExtractionCursorRepository"/> for
/// Outage, against the shared `extraction_cursor`/`extraction_run` tables
/// (src/db/migrations/0002_create_extraction_schema.sql) keyed by (windfarm_id, source_type).
/// `last_committed_id` carries the source `OutageClassificationID` as TEXT; there is no
/// separate turbine/item tie-break column.
/// </summary>
/// <summary>Thrown by fenced writes whose WHERE-fenced statement affected zero rows, i.e. the
/// caller no longer holds the lease it thinks it holds.</summary>
public sealed class LeaseFencingException : InvalidOperationException
{
    public LeaseFencingException(string message) : base(message) { }
}

public sealed class PostgresExtractionCursorRepository : IExtractionCursorRepository
{
    private readonly Func<NpgsqlConnection> _createConnection;

    public PostgresExtractionCursorRepository(string connectionString)
    {
        _createConnection = () => new NpgsqlConnection(connectionString);
    }

    public PostgresExtractionCursorRepository(PostgresConnectionFactory factory)
    {
        _createConnection = factory.CreateConnection;
    }

    private static void AddWindfarmIdParameter(NpgsqlParameterCollection parameters, int windfarmId)
        => parameters.Add("windfarmId", NpgsqlTypes.NpgsqlDbType.Integer).Value = windfarmId;

    public async Task<ExtractionLeaseModel?> GetCurrentAsync(int windfarmId, string sourceType, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT cursor.lease_token, cursor.last_committed_updated_at, cursor.last_committed_id, cursor.lease_expires_at,
                   cursor.deleted_last_committed_updated_at, cursor.deleted_last_committed_id, w.windfarm_name
              FROM extraction_cursor cursor
              JOIN windfarm w ON w.id = cursor.windfarm_id
             WHERE cursor.windfarm_id = @windfarmId
               AND cursor.source_type = @sourceType;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        AddWindfarmIdParameter(command.Parameters, windfarmId);
        command.Parameters.AddWithValue("sourceType", sourceType);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ExtractionLeaseModel
        {
            WindfarmId = windfarmId,
            WindfarmName = reader.GetString(6),
            SourceType = sourceType,
            LeaseToken = reader.GetInt64(0),
            LastUpdatedUtc = reader.GetDateTime(1),
            LastCommittedId = reader.GetString(2),
            LeaseExpiresAtUtc = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
            DeletedLastUpdatedUtc = reader.IsDBNull(4) ? reader.GetDateTime(1) : reader.GetDateTime(4),
            DeletedLastCommittedId = reader.IsDBNull(5) ? reader.GetString(2) : reader.GetString(5),
        };
    }

    public async Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(
        int windfarmId, string sourceType, string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        // A cursor with no matching windfarm row is a configuration error (INNER JOIN), so a
        // malformed registration fails fast here rather than silently inventing a display name.
        const string sql = """
            UPDATE extraction_cursor c
               SET lease_owner = @owner,
                   lease_token = c.lease_token + 1,
                   lease_expires_at = clock_timestamp() + (@leaseSeconds || ' seconds')::interval,
                   last_run_started_utc = clock_timestamp()
              FROM windfarm w
             WHERE w.id = c.windfarm_id
               AND c.windfarm_id = @windfarmId
               AND c.source_type = @sourceType
               AND (c.lease_expires_at IS NULL OR c.lease_expires_at < clock_timestamp())
            RETURNING c.lease_token, c.last_committed_updated_at, c.last_committed_id, c.lease_expires_at,
                      c.deleted_last_committed_updated_at, c.deleted_last_committed_id, w.windfarm_name;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue("leaseSeconds", leaseDuration.TotalSeconds);
        AddWindfarmIdParameter(command.Parameters, windfarmId);
        command.Parameters.AddWithValue("sourceType", sourceType);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            if (await GetCurrentAsync(windfarmId, sourceType, cancellationToken) is null)
                throw new InvalidOperationException("The extraction cursor must be initialized before ingestion can run.");
            return null;
        }

        return new ExtractionLeaseModel
        {
            WindfarmId = windfarmId,
            WindfarmName = reader.GetString(6),
            SourceType = sourceType,
            LeaseToken = reader.GetInt64(0),
            LastUpdatedUtc = reader.GetDateTime(1),
            LastCommittedId = reader.GetString(2),
            LeaseExpiresAtUtc = reader.GetDateTime(3),
            // NULL means "not yet seeded"; fall back to the live/changed feed's committed
            // position so a not-yet-backfilled row behaves like a fresh registration.
            DeletedLastUpdatedUtc = reader.IsDBNull(4) ? reader.GetDateTime(1) : reader.GetDateTime(4),
            DeletedLastCommittedId = reader.IsDBNull(5) ? reader.GetString(2) : reader.GetString(5),
        };
    }

    public async Task<bool> TryRenewLeaseAsync(
        int windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET lease_expires_at = clock_timestamp() + (@leaseSeconds || ' seconds')::interval
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND lease_owner = @owner
               AND lease_token = @leaseToken
               AND lease_expires_at > clock_timestamp()
            RETURNING lease_token;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("leaseSeconds", leaseDuration.TotalSeconds);
        AddWindfarmIdParameter(command.Parameters, windfarmId);
        command.Parameters.AddWithValue("sourceType", sourceType);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue("leaseToken", leaseToken);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }

    public async Task<bool> TryCommitAsync(
        int windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedUtc, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedUtc = null)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET last_committed_updated_at = @newLastUpdatedUtc,
                   last_committed_id = @newLastCommittedId,
                   last_committed_utc = clock_timestamp()
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND lease_token = @leaseToken
               AND lease_expires_at > clock_timestamp()
               AND last_committed_updated_at <= @newLastUpdatedUtc
               AND (@expectedLastUpdatedUtc IS NULL OR last_committed_updated_at = @expectedLastUpdatedUtc)
            RETURNING lease_token;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("newLastUpdatedUtc", newLastUpdatedUtc);
        command.Parameters.AddWithValue("newLastCommittedId", newLastCommittedId);
        command.Parameters.Add("expectedLastUpdatedUtc", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
            expectedLastUpdatedUtc is { } expected ? expected : DBNull.Value;
        AddWindfarmIdParameter(command.Parameters, windfarmId);
        command.Parameters.AddWithValue("sourceType", sourceType);
        command.Parameters.AddWithValue("leaseToken", leaseToken);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is not null;
    }

    public async Task ReleaseLeaseAsync(int windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET lease_owner = NULL,
                   lease_expires_at = NULL
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
                             AND lease_token = @leaseToken
                             AND lease_expires_at > clock_timestamp();
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        AddWindfarmIdParameter(command.Parameters, windfarmId);
        command.Parameters.AddWithValue("sourceType", sourceType);
        command.Parameters.AddWithValue("leaseToken", leaseToken);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ExtractionRunModel?> StartRunAsync(
        int windfarmId, string sourceType, long leaseToken,
        DateTime fromUpdatedAtUtc, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        // Fenced insert: only creates the run row if this lease token is still the current,
        // unexpired one on extraction_cursor — otherwise a lost/stolen lease could still start
        // (and later complete) a run against a cursor another owner now controls.
        const string sql = """
            INSERT INTO extraction_run
                (windfarm_id, source_type, status, from_updated_at, from_committed_id, output_path, operation_id, created_by)
            SELECT @windfarmId, @sourceType, 'Running', @fromUpdatedAtUtc, @fromCommittedId, @outputPath, control.resolve_operation(@operationId, @sourceType),
                   COALESCE((SELECT lease_owner FROM extraction_cursor
                       WHERE windfarm_id = @windfarmId AND source_type = @sourceType AND lease_token = @leaseToken), current_user)
             WHERE EXISTS (
                SELECT 1 FROM extraction_cursor
                 WHERE windfarm_id = @windfarmId
                   AND source_type = @sourceType
                   AND lease_token = @leaseToken
                   AND lease_expires_at > clock_timestamp()
             )
            RETURNING id, started_at, operation_id;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add("operationId", NpgsqlTypes.NpgsqlDbType.Text).Value = (object?)operationId ?? DBNull.Value;
        AddWindfarmIdParameter(command.Parameters, windfarmId);
        command.Parameters.AddWithValue("sourceType", sourceType);
        command.Parameters.AddWithValue("fromUpdatedAtUtc", fromUpdatedAtUtc);
        command.Parameters.AddWithValue("fromCommittedId", fromCommittedId);
        command.Parameters.Add("outputPath", NpgsqlTypes.NpgsqlDbType.Text).Value = (object?)outputPath ?? DBNull.Value;
        command.Parameters.AddWithValue("leaseToken", leaseToken);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ExtractionRunModel(
            RunId: reader.GetInt64(0),
            WindfarmId: windfarmId,
            SourceType: sourceType,
            Status: ExtractionRunStatus.Running,
            StartedAtUtc: reader.GetDateTime(1),
            CompletedAtUtc: null,
            FromUpdatedAtUtc: fromUpdatedAtUtc,
            FromCommittedId: fromCommittedId,
            ToUpdatedAtUtc: null,
            ToCommittedId: null,
            OutputPath: outputPath,
            ErrorMessage: null,
            OperationId: reader.GetString(2));
    }

    public async Task<bool> TryCompleteRunAsync(
        long runId, int windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedUtc, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedUtc = null,
        DateTime? newDeletedLastUpdatedUtc = null, string? newDeletedLastCommittedId = null,
        DateTime? expectedDeletedLastUpdatedUtc = null)
    {
        if (newDeletedLastUpdatedUtc.HasValue != (newDeletedLastCommittedId is not null))
            throw new ArgumentException("The deletion-feed checkpoint timestamp and ID must be supplied together.");

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        // The deletion-feed pair only advances when both new values are supplied; COALESCE keeps
        // the existing value otherwise, so sources with no delete feed are never touched here.
        const string cursorSql = """
            UPDATE extraction_cursor
               SET last_committed_updated_at = @newLastUpdatedUtc,
                   last_committed_id = @newLastCommittedId,
                   deleted_last_committed_updated_at = COALESCE(@newDeletedLastUpdatedUtc, deleted_last_committed_updated_at),
                   deleted_last_committed_id = COALESCE(@newDeletedLastCommittedId, deleted_last_committed_id),
                   last_committed_utc = clock_timestamp()
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND lease_token = @leaseToken
               AND lease_expires_at > clock_timestamp()
               AND last_committed_updated_at <= @newLastUpdatedUtc
               AND (@expectedLastUpdatedUtc IS NULL OR last_committed_updated_at = @expectedLastUpdatedUtc)
               AND (@newDeletedLastUpdatedUtc IS NULL OR deleted_last_committed_updated_at IS NULL
                    OR deleted_last_committed_updated_at <= @newDeletedLastUpdatedUtc)
            AND (@expectedDeletedLastUpdatedUtc IS NULL
                OR COALESCE(deleted_last_committed_updated_at, last_committed_updated_at) = @expectedDeletedLastUpdatedUtc)
            RETURNING lease_token;
            """;

        await using (var cursorCommand = new NpgsqlCommand(cursorSql, connection, transaction))
        {
            cursorCommand.Parameters.AddWithValue("newLastUpdatedUtc", newLastUpdatedUtc);
            cursorCommand.Parameters.AddWithValue("newLastCommittedId", newLastCommittedId);
            cursorCommand.Parameters.Add("newDeletedLastUpdatedUtc", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                newDeletedLastUpdatedUtc is { } newDeleted ? newDeleted : DBNull.Value;
            cursorCommand.Parameters.Add("newDeletedLastCommittedId", NpgsqlTypes.NpgsqlDbType.Text).Value =
                (object?)newDeletedLastCommittedId ?? DBNull.Value;
            cursorCommand.Parameters.Add("expectedDeletedLastUpdatedUtc", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                expectedDeletedLastUpdatedUtc is { } expectedDeleted ? expectedDeleted : DBNull.Value;
            cursorCommand.Parameters.Add("expectedLastUpdatedUtc", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                expectedLastUpdatedUtc is { } expected ? expected : DBNull.Value;
            AddWindfarmIdParameter(cursorCommand.Parameters, windfarmId);
            cursorCommand.Parameters.AddWithValue("sourceType", sourceType);
            cursorCommand.Parameters.AddWithValue("leaseToken", leaseToken);

            // ExecuteScalarAsync leaves no reader open, so RollbackAsync below never races an
            // open DataReader on the same connection (NpgsqlDataReader must be closed first).
            var cursorResult = await cursorCommand.ExecuteScalarAsync(cancellationToken);
            if (cursorResult is null)
            {
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }
        }

        const string runSql = """
            UPDATE extraction_run
               SET status = 'Completed',
                   completed_at = clock_timestamp(),
                   to_updated_at = @newLastUpdatedUtc,
                   to_committed_id = @newLastCommittedId
             WHERE id = @runId
               AND windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND status = 'Running'
            RETURNING id;
            """;

        await using (var runCommand = new NpgsqlCommand(runSql, connection, transaction))
        {
            runCommand.Parameters.AddWithValue("runId", runId);
            runCommand.Parameters.AddWithValue("newLastUpdatedUtc", newLastUpdatedUtc);
            runCommand.Parameters.AddWithValue("newLastCommittedId", newLastCommittedId);
            AddWindfarmIdParameter(runCommand.Parameters, windfarmId);
            runCommand.Parameters.AddWithValue("sourceType", sourceType);

            var runResult = await runCommand.ExecuteScalarAsync(cancellationToken);
            if (runResult is null)
            {
                await transaction.RollbackAsync(cancellationToken);
                return false;
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    public async Task FailRunAsync(
        long runId, int windfarmId, string sourceType, long leaseToken,
        DateTime? partialUpdatedAtUtc, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = _createConnection();
            await connection.OpenAsync(cancellationToken);

            const string sql = """
                UPDATE extraction_run
                   SET status = 'Failed',
                       completed_at = clock_timestamp(),
                       to_updated_at = @partialUpdatedAtUtc,
                       to_committed_id = @partialCommittedId,
                       error_message = @errorMessage
                 WHERE id = @runId
                   AND windfarm_id = @windfarmId
                   AND source_type = @sourceType
                   AND status = 'Running';
                """;

            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue("runId", runId);
            command.Parameters.Add("partialUpdatedAtUtc", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                partialUpdatedAtUtc is { } updated ? updated : DBNull.Value;
            command.Parameters.Add("partialCommittedId", NpgsqlTypes.NpgsqlDbType.Text).Value =
                (object?)partialCommittedId ?? DBNull.Value;
            command.Parameters.AddWithValue("errorMessage", errorMessage);
            AddWindfarmIdParameter(command.Parameters, windfarmId);
            command.Parameters.AddWithValue("sourceType", sourceType);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch
        {
            // Best-effort audit write, including connection open/setup: never mask the original failure.
        }
    }
}
