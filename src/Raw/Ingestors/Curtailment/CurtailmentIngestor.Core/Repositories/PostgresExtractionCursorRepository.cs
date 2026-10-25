using CAT.Messaging.Contracts;
using System.Globalization;
using Npgsql;
using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Interfaces;
using CurtailmentIngestor.Models;

namespace CurtailmentIngestor.Repositories;

/// <summary>
/// Postgres-backed, fenced implementation of <see cref="IExtractionCursorRepository"/> for
/// Curtailment. Uses the shared `extraction_cursor`/`extraction_run` control-plane tables
/// (source_type = "Curtailment"), keyed by (windfarm_id, source_type) alongside the other
/// ingestors — see src/db/migrations/0002_create_extraction_schema.sql.
/// </summary>
/// <summary>Thrown when a fenced write is rejected because the caller's lease token is no
/// longer the current, unexpired lease (a zombie caller).</summary>
public sealed class LeaseFencingException : InvalidOperationException
{
    public LeaseFencingException(string message) : base(message) { }
}

public sealed class PostgresExtractionCursorRepository : IExtractionCursorRepository
{
    private readonly string _connectionString;

    public PostgresExtractionCursorRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>`windfarm_id` is INTEGER in the shared schema; parse the public string id here
    /// rather than widening the column, so a malformed/negative id fails fast instead of being
    /// silently coerced.</summary>
    private static int ParseWindfarmId(string windfarmId)
    {
        if (!int.TryParse(windfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
            throw new ArgumentException($"Windfarm identifier '{windfarmId}' must be a positive integer.", nameof(windfarmId));
        return parsed;
    }

    private static void AddWindfarmIdParameter(NpgsqlParameterCollection parameters, string windfarmId)
        => parameters.Add("windfarmId", NpgsqlTypes.NpgsqlDbType.Integer).Value = ParseWindfarmId(windfarmId);

    public async Task<ExtractionLeaseModel?> GetCurrentAsync(string windfarmId, string sourceType, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            SELECT lease_token, last_committed_updated_at, last_committed_id, lease_expires_at
              FROM extraction_cursor
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ExtractionLeaseModel
        {
            WindfarmId = windfarmId,
            SourceType = sourceType,
            LeaseToken = reader.GetInt64(0),
            LastUpdatedDate = AsUtc(reader.GetDateTime(1)),
            LastCommittedId = reader.GetString(2),
            LeaseExpiresAt = reader.IsDBNull(3) ? null : AsUtc(reader.GetDateTime(3)),
        };
    }

    /// <summary>Cursor timestamps are UTC by contract (Section 6.4.2); normalise so callers can assert Kind.</summary>
    private static DateTime AsUtc(DateTime value)
        => value.Kind == DateTimeKind.Utc ? value : DateTime.SpecifyKind(value.ToUniversalTime(), DateTimeKind.Utc);

    public async Task<ExtractionLeaseModel?> TryAcquireLeaseAsync(
        string windfarmId, string sourceType, string owner, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET lease_owner = @owner,
                   lease_token = lease_token + 1,
                   lease_expires_at = clock_timestamp() + (@leaseSeconds || ' seconds')::interval,
                   last_run_started_utc = clock_timestamp()
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND (lease_expires_at IS NULL OR lease_expires_at < clock_timestamp())
            RETURNING lease_token, last_committed_updated_at, last_committed_id, lease_expires_at;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("owner", owner);
        cmd.Parameters.AddWithValue("leaseSeconds", (int)leaseDuration.TotalSeconds);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            if (await GetCurrentAsync(windfarmId, sourceType, cancellationToken) is null)
                throw new InvalidOperationException("The extraction cursor must be initialized before ingestion can run.");
            return null;
        }

        return new ExtractionLeaseModel
        {
            WindfarmId = windfarmId,
            SourceType = sourceType,
            LeaseToken = reader.GetInt64(0),
            LastUpdatedDate = AsUtc(reader.GetDateTime(1)),
            LastCommittedId = reader.GetString(2),
            LeaseExpiresAt = AsUtc(reader.GetDateTime(3)),
        };
    }

    public async Task<bool> TryRenewLeaseAsync(
        string windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

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

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("leaseSeconds", (int)leaseDuration.TotalSeconds);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);
        cmd.Parameters.AddWithValue("owner", owner);
        cmd.Parameters.AddWithValue("leaseToken", leaseToken);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }

    public async Task<bool> TryCommitAsync(
        string windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedDate, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedDate = null)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET last_committed_updated_at = @newLastUpdatedDate,
                   last_committed_id = @newLastCommittedId,
                   last_committed_utc = clock_timestamp()
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND lease_token = @leaseToken
               AND lease_expires_at > clock_timestamp()
               AND last_committed_updated_at <= @newLastUpdatedDate
               AND (@expectedLastUpdatedDate IS NULL OR last_committed_updated_at = @expectedLastUpdatedDate)
            RETURNING lease_token;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("newLastUpdatedDate", newLastUpdatedDate);
        cmd.Parameters.AddWithValue("newLastCommittedId", newLastCommittedId);
        cmd.Parameters.Add("expectedLastUpdatedDate", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
            expectedLastUpdatedDate is { } expected ? expected : DBNull.Value;
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);
        cmd.Parameters.AddWithValue("leaseToken", leaseToken);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }

    public async Task ReleaseLeaseAsync(string windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET lease_owner = NULL,
                   lease_expires_at = NULL
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND lease_token = @leaseToken;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);
        cmd.Parameters.AddWithValue("leaseToken", leaseToken);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ExtractionRunModel> StartRunAsync(
        string windfarmId, string sourceType, long leaseToken,
        DateTime fromUpdatedAt, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null)
    {
        OperationCorrelation.Validate(operationId);
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Fenced on the cursor's current lease token/expiry: a stale owner (lease reassigned or
        // expired since acquisition) never creates an orphaned Running run row.
        const string sql = """
            INSERT INTO extraction_run
                (windfarm_id, source_type, status, from_updated_at, from_committed_id, output_path, operation_id)
            SELECT @windfarmId, @sourceType, 'Running', @fromUpdatedAt, @fromCommittedId, @outputPath,
                   control.resolve_operation(@operationId, 'curtailment-ingestor-worker')
             WHERE EXISTS (
                SELECT 1 FROM extraction_cursor
                 WHERE windfarm_id = @windfarmId
                   AND source_type = @sourceType
                   AND lease_token = @leaseToken
                   AND lease_expires_at > clock_timestamp())
            RETURNING id, started_at, operation_id::text;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.Add("operationId", NpgsqlTypes.NpgsqlDbType.Text).Value = (object?)operationId ?? DBNull.Value;
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);
        cmd.Parameters.AddWithValue("fromUpdatedAt", fromUpdatedAt);
        cmd.Parameters.AddWithValue("fromCommittedId", fromCommittedId);
        cmd.Parameters.Add("outputPath", NpgsqlTypes.NpgsqlDbType.Text).Value = (object?)outputPath ?? DBNull.Value;
        cmd.Parameters.AddWithValue("leaseToken", leaseToken);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new LeaseFencingException(
                $"StartRunAsync rejected: lease token {leaseToken} for windfarm {windfarmId}/{sourceType} is no longer current or has expired.");
        }

        return new ExtractionRunModel(
            RunId: reader.GetInt64(0),
            WindfarmId: windfarmId,
            SourceType: sourceType,
            Status: ExtractionRunStatus.Running,
            StartedAtUtc: reader.GetDateTime(1),
            CompletedAtUtc: null,
            FromUpdatedAtUtc: fromUpdatedAt,
            FromCommittedId: fromCommittedId,
            ToUpdatedAtUtc: null,
            ToCommittedId: null,
            OutputPath: outputPath,
            ErrorMessage: null,
            OperationId: reader.GetString(2));
    }

    public async Task<bool> TryCompleteRunAsync(
        long runId, string windfarmId, string sourceType, long leaseToken,
        DateTime newLastUpdatedDate, string newLastCommittedId, CancellationToken cancellationToken,
        DateTime? expectedLastUpdatedDate = null)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var transaction = await conn.BeginTransactionAsync(cancellationToken);

        const string cursorSql = """
            UPDATE extraction_cursor
               SET last_committed_updated_at = @newLastUpdatedDate,
                   last_committed_id = @newLastCommittedId,
                   last_committed_utc = clock_timestamp()
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND lease_token = @leaseToken
               AND lease_expires_at > clock_timestamp()
               AND last_committed_updated_at <= @newLastUpdatedDate
               AND (@expectedLastUpdatedDate IS NULL OR last_committed_updated_at = @expectedLastUpdatedDate)
            RETURNING lease_token;
            """;

        await using (var cursorCommand = new NpgsqlCommand(cursorSql, conn, transaction))
        {
            cursorCommand.Parameters.AddWithValue("newLastUpdatedDate", newLastUpdatedDate);
            cursorCommand.Parameters.AddWithValue("newLastCommittedId", newLastCommittedId);
            cursorCommand.Parameters.Add("expectedLastUpdatedDate", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
                expectedLastUpdatedDate is { } expected ? expected : DBNull.Value;
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
                   completed_at = clock_timestamp(),
                   to_updated_at = @newLastUpdatedDate,
                   to_committed_id = @newLastCommittedId
             WHERE id = @runId
               AND windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND status = 'Running'
            RETURNING id;
            """;

        await using (var runCommand = new NpgsqlCommand(runSql, conn, transaction))
        {
            runCommand.Parameters.AddWithValue("runId", runId);
            runCommand.Parameters.AddWithValue("newLastUpdatedDate", newLastUpdatedDate);
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
        long runId, string windfarmId, string sourceType, long leaseToken,
        DateTime? partialUpdatedAt, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken)
    {
        ParseWindfarmId(windfarmId);
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_run
               SET status = 'Failed',
                   completed_at = clock_timestamp(),
                   to_updated_at = @partialUpdatedAt,
                   to_committed_id = @partialCommittedId,
                   error_message = @errorMessage
             WHERE id = @runId
               AND windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND status = 'Running';
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("runId", runId);
        cmd.Parameters.Add("partialUpdatedAt", NpgsqlTypes.NpgsqlDbType.TimestampTz).Value =
            partialUpdatedAt is { } updated ? updated : DBNull.Value;
        cmd.Parameters.Add("partialCommittedId", NpgsqlTypes.NpgsqlDbType.Text).Value =
            (object?)partialCommittedId ?? DBNull.Value;
        cmd.Parameters.AddWithValue("errorMessage", errorMessage);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch
        {
            // Best-effort audit write; a failure here must never mask the original run failure.
        }
    }
}
