using System.Globalization;
using Npgsql;
using ProductionIngestor.Enums;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.Repositories;

/// <summary>Postgres-backed, fenced implementation of <see cref="IExtractionCursorRepository"/>.</summary>
public sealed class PostgresExtractionCursorRepository : IExtractionCursorRepository
{
    private readonly Func<NpgsqlConnection> _createConnection;

    public PostgresExtractionCursorRepository(string connectionString)
    {
        _createConnection = () => new NpgsqlConnection(connectionString);
    }

    public PostgresExtractionCursorRepository(PostgresConnectionFactory connectionFactory)
    {
        _createConnection = connectionFactory.CreateConnection;
    }

    /// <summary>`windfarm_id` is INTEGER in the schema; parse the public string id here rather than
    /// widening the column, so a malformed/negative id fails fast instead of being silently coerced.</summary>
    //private static int ParseWindfarmId(string windfarmId)
    //{
    //    if (!int.TryParse(windfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
    //        throw new ArgumentException($"Windfarm identifier '{windfarmId}' must be a positive integer.", nameof(windfarmId));
    //    return parsed;
    //}

    private void AddWindfarmIdParameter(NpgsqlParameterCollection parameters, int windfarmId)
        => parameters.Add("windfarmId", NpgsqlTypes.NpgsqlDbType.Integer).Value = windfarmId;

    public async Task<ExtractionLeaseModel?> GetCurrentAsync(int windfarmId, string sourceType, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT cursor.lease_token, cursor.last_committed_updated_at, cursor.last_committed_id, cursor.lease_expires_at, w.windfarm_name
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
            WindfarmName = reader.GetString(4),
            SourceType = sourceType,
            LeaseToken = reader.GetInt64(0),
            LastUpdatedUtc = reader.GetDateTime(1),
            LastCommittedId = reader.GetString(2),
            LeaseExpiresAtUtc = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
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
                   last_run_started_utc = now()
              FROM windfarm w
             WHERE w.id = c.windfarm_id
               AND c.windfarm_id = @windfarmId
               AND c.source_type = @sourceType
               AND (c.lease_expires_at IS NULL OR c.lease_expires_at < clock_timestamp())
            RETURNING c.lease_token, c.last_committed_updated_at, c.last_committed_id, c.lease_expires_at, w.windfarm_name;
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
            WindfarmName = reader.GetString(4),
            SourceType = sourceType,
            LeaseToken = reader.GetInt64(0),
            LastUpdatedUtc = reader.GetDateTime(1),
            LastCommittedId = reader.GetString(2),
            LeaseExpiresAtUtc = reader.GetDateTime(3),
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
                   last_committed_utc = now()
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

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
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

        // Fenced on the cursor's current lease token/expiry: a stale owner (lease reassigned or
        // expired since acquisition) never creates an orphaned Running run row.
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
                   AND lease_expires_at > clock_timestamp())
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
        DateTime? queryStartInclusiveUtc = null,
        DateTime? queryEndExclusiveUtc = null)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        const string cursorSql = """
            UPDATE extraction_cursor
               SET last_committed_updated_at = @newLastUpdatedUtc,
                   last_committed_id = @newLastCommittedId,
                   last_committed_utc = now()
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND lease_token = @leaseToken
               AND lease_expires_at > clock_timestamp()
               AND last_committed_updated_at <= @newLastUpdatedUtc
               AND (@expectedLastUpdatedUtc IS NULL OR last_committed_updated_at = @expectedLastUpdatedUtc)
            RETURNING lease_token;
            """;

        await using (var cursorCommand = new NpgsqlCommand(cursorSql, connection, transaction))
        {
            cursorCommand.Parameters.AddWithValue("newLastUpdatedUtc", newLastUpdatedUtc);
            cursorCommand.Parameters.AddWithValue("newLastCommittedId", newLastCommittedId);
            cursorCommand.Parameters.Add("expectedLastUpdatedUtc", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                expectedLastUpdatedUtc is { } expected ? expected : DBNull.Value;
            AddWindfarmIdParameter(cursorCommand.Parameters, windfarmId);
            cursorCommand.Parameters.AddWithValue("sourceType", sourceType);
            cursorCommand.Parameters.AddWithValue("leaseToken", leaseToken);

            // ExecuteScalarAsync (not ExecuteReaderAsync) so no open reader survives past this
            // block — a RollbackAsync while a reader from this same connection/transaction is
            // still open throws NpgsqlOperationInProgress instead of actually rolling back.
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
                   completed_at = now(),
                   to_updated_at = @newLastUpdatedUtc,
                   to_committed_id = @newLastCommittedId,
                   query_start_inclusive_utc = @queryStart,
                   query_end_exclusive_utc = @queryEnd
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
            runCommand.Parameters.Add("queryStart", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                queryStartInclusiveUtc is { } start ? start : DBNull.Value;
            runCommand.Parameters.Add("queryEnd", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                queryEndExclusiveUtc is { } end ? end : DBNull.Value;
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
        DateTime? partialUpdatedAtUtc, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken,
        DateTime? queryStartInclusiveUtc = null,
        DateTime? queryEndExclusiveUtc = null)
    {
        try
        {
            await using var connection = _createConnection();
            await connection.OpenAsync(cancellationToken);

            const string sql = """
                UPDATE extraction_run
                   SET status = 'Failed',
                       completed_at = now(),
                       to_updated_at = @partialUpdatedAtUtc,
                       to_committed_id = @partialCommittedId,
                       error_message = @errorMessage,
                       query_start_inclusive_utc = @queryStart,
                       query_end_exclusive_utc = @queryEnd
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
            command.Parameters.Add("queryStart", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                queryStartInclusiveUtc is { } start ? start : DBNull.Value;
            command.Parameters.Add("queryEnd", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                queryEndExclusiveUtc is { } end ? end : DBNull.Value;
            AddWindfarmIdParameter(command.Parameters, windfarmId);
            command.Parameters.AddWithValue("sourceType", sourceType);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch
        {
            // Best-effort audit write, including connection open/setup: a failure here must
            // never mask or throw over the original run failure the caller is already handling.
        }
    }
}
