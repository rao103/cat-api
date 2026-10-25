using CAT.Messaging.Contracts;
using System.Globalization;
using Npgsql;
using NpgsqlTypes;
using AlarmIngestor.Enums;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;

namespace AlarmIngestor.Repositories;

/// <summary>
/// Postgres-backed, fenced implementation of <see cref="IExtractionCursorRepository"/> against
/// the shared `extraction_cursor`/`extraction_run` schema (src/db/migrations) plus this
/// ingestor's own `alarm_known_open_alarm` table. `windfarm_id` is INTEGER in the schema; the
/// public string id is parsed here at the DB boundary rather than widening the column. The
/// committed AlarmId checkpoint (`last_committed_id`) is TEXT and global per windfarm — never
/// keyed by turbine. `last_committed_updated_at` is never written: Alarm's source has no
/// LastUpdatedUtc support, so the column stays at its '-infinity' sentinel default forever
/// rather than being fabricated.
/// </summary>
public sealed class PostgresExtractionCursorRepository : IExtractionCursorRepository
{
    private readonly string _connectionString;

    public PostgresExtractionCursorRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>`windfarm_id` is INTEGER in the schema; parse the public string id here rather
    /// than widening the column, so a malformed/negative id fails fast instead of being silently
    /// coerced. `internal` (not `private`) so a unit test can verify this without a live DB —
    /// this is the only place a source farm string is forced numeric; adapters/paths/requests
    /// keep using the source's own (possibly non-numeric) farm code untouched.</summary>
    internal static int ParseWindfarmId(string windfarmId)
    {
        if (!int.TryParse(windfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) || parsed <= 0)
            throw new ArgumentException($"Windfarm identifier '{windfarmId}' must be a positive integer.", nameof(windfarmId));
        return parsed;
    }

    private static void AddWindfarmIdParameter(NpgsqlParameterCollection parameters, string windfarmId)
        => parameters.Add("windfarmId", NpgsqlDbType.Integer).Value = ParseWindfarmId(windfarmId);

    public async Task<ExtractionLeaseModel?> GetCurrentAsync(string windfarmId, string sourceType, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            SELECT lease_token, last_committed_id, lease_expires_at
              FROM extraction_cursor
             WHERE windfarm_id = @windfarmId AND source_type = @sourceType;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ExtractionLeaseModel(
            windfarmId, sourceType, reader.GetInt64(0), reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetDateTime(2));
    }

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
             WHERE windfarm_id = @windfarmId AND source_type = @sourceType
               AND (lease_expires_at IS NULL OR lease_expires_at < clock_timestamp())
            RETURNING lease_token, last_committed_id, lease_expires_at;
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
                throw new InvalidOperationException(
                    $"No extraction_cursor row for windfarm '{windfarmId}'/'{sourceType}'; the row must be manually initialized before ingestion can run.");
            return null;
        }

        return new ExtractionLeaseModel(
            windfarmId, sourceType, reader.GetInt64(0), reader.GetString(1), reader.GetDateTime(2));
    }

    public async Task<bool> TryRenewLeaseAsync(
        string windfarmId, string sourceType, string owner, long leaseToken, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET lease_expires_at = clock_timestamp() + (@leaseSeconds || ' seconds')::interval
             WHERE windfarm_id = @windfarmId AND source_type = @sourceType
               AND lease_owner = @owner AND lease_token = @leaseToken
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

    public async Task ReleaseLeaseAsync(string windfarmId, string sourceType, long leaseToken, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_cursor
               SET lease_owner = NULL, lease_expires_at = NULL
             WHERE windfarm_id = @windfarmId AND source_type = @sourceType AND lease_token = @leaseToken;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);
        cmd.Parameters.AddWithValue("leaseToken", leaseToken);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ExtractionRunModel> StartRunAsync(
        string windfarmId, string sourceType, long leaseToken, string fromCommittedId, string? outputPath, CancellationToken cancellationToken, string? operationId = null)
    {
        OperationCorrelation.Validate(operationId);
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // from_updated_at is NOT NULL in the shared schema but has no meaning for Alarm (no
        // LastUpdatedUtc support); '-infinity' preserves the same "unsupported" sentinel used by
        // the cursor row instead of fabricating a real timestamp.
        const string sql = """
            INSERT INTO extraction_run (windfarm_id, source_type, status, from_updated_at, from_committed_id, output_path, operation_id)
            SELECT @windfarmId, @sourceType, 'Running', '-infinity'::timestamptz, @fromCommittedId, @outputPath,
                   control.resolve_operation(@operationId, 'alarm-ingestor-worker')
             WHERE EXISTS (
                SELECT 1 FROM extraction_cursor
                 WHERE windfarm_id = @windfarmId AND source_type = @sourceType
                   AND lease_token = @leaseToken AND lease_expires_at > clock_timestamp())
            RETURNING id, started_at, operation_id::text;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.Add("operationId", NpgsqlTypes.NpgsqlDbType.Text).Value = (object?)operationId ?? DBNull.Value;
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("sourceType", sourceType);
        cmd.Parameters.AddWithValue("leaseToken", leaseToken);
        cmd.Parameters.AddWithValue("fromCommittedId", fromCommittedId);
        cmd.Parameters.Add("outputPath", NpgsqlDbType.Text).Value = (object?)outputPath ?? DBNull.Value;

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            throw new InvalidOperationException("Cannot start an Alarm run without its current cursor lease.");

        return new ExtractionRunModel(
            RunId: reader.GetInt64(0),
            WindfarmId: windfarmId,
            SourceType: sourceType,
            Status: ExtractionRunStatus.Running,
            StartedAtUtc: reader.GetDateTime(1),
            CompletedAtUtc: null,
            FromCommittedId: fromCommittedId,
            ToCommittedId: null,
            OutputPath: outputPath,
            ErrorMessage: null,
            OperationId: reader.GetString(2));
    }

    public async Task<bool> TryCompleteRunAsync(
        long runId, string windfarmId, string sourceType, long leaseToken, string newCommittedId, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        const string cursorSql = """
            UPDATE extraction_cursor
               SET last_committed_id = @newCommittedId,
                   last_committed_utc = clock_timestamp()
             WHERE windfarm_id = @windfarmId AND source_type = @sourceType
               AND lease_token = @leaseToken AND lease_expires_at > clock_timestamp()
            RETURNING lease_token;
            """;

        await using (var cursorCmd = new NpgsqlCommand(cursorSql, conn, tx))
        {
            cursorCmd.Parameters.AddWithValue("newCommittedId", newCommittedId);
            AddWindfarmIdParameter(cursorCmd.Parameters, windfarmId);
            cursorCmd.Parameters.AddWithValue("sourceType", sourceType);
            cursorCmd.Parameters.AddWithValue("leaseToken", leaseToken);

            bool cursorUpdated;
            await using (var cursorReader = await cursorCmd.ExecuteReaderAsync(cancellationToken))
            {
                cursorUpdated = await cursorReader.ReadAsync(cancellationToken);
            }
            // Reader must be disposed (closed above) before RollbackAsync — Npgsql cannot roll
            // back a transaction while a reader from it is still open.
            if (!cursorUpdated)
            {
                await tx.RollbackAsync(cancellationToken);
                return false;
            }
        }

        const string runSql = """
            UPDATE extraction_run
               SET status = 'Completed',
                   completed_at = clock_timestamp(),
                   to_updated_at = '-infinity'::timestamptz,
                   to_committed_id = @newCommittedId
             WHERE id = @runId AND windfarm_id = @windfarmId AND source_type = @sourceType AND status = 'Running'
            RETURNING id;
            """;

        await using (var runCmd = new NpgsqlCommand(runSql, conn, tx))
        {
            runCmd.Parameters.AddWithValue("runId", runId);
            runCmd.Parameters.AddWithValue("newCommittedId", newCommittedId);
            AddWindfarmIdParameter(runCmd.Parameters, windfarmId);
            runCmd.Parameters.AddWithValue("sourceType", sourceType);

            bool runUpdated;
            await using (var runReader = await runCmd.ExecuteReaderAsync(cancellationToken))
            {
                runUpdated = await runReader.ReadAsync(cancellationToken);
            }
            if (!runUpdated)
            {
                await tx.RollbackAsync(cancellationToken);
                return false;
            }
        }

        await tx.CommitAsync(cancellationToken);
        return true;
    }

    public async Task FailRunAsync(
        long runId, string windfarmId, string sourceType, long leaseToken, string? partialCommittedId, string errorMessage, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        const string sql = """
            UPDATE extraction_run
               SET status = 'Failed',
                   completed_at = clock_timestamp(),
                   to_updated_at = '-infinity'::timestamptz,
                   to_committed_id = @partialCommittedId,
                   error_message = @errorMessage
             WHERE id = @runId AND windfarm_id = @windfarmId AND source_type = @sourceType AND status = 'Running';
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("runId", runId);
        cmd.Parameters.Add("partialCommittedId", NpgsqlDbType.Text).Value = (object?)partialCommittedId ?? DBNull.Value;
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

    public async Task<IReadOnlyDictionary<string, IReadOnlyList<long>>> GetKnownOpenAlarmsAsync(
        string windfarmId, IReadOnlyList<string> turbineRdspps, int maxIdsPerTurbine, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        // Bounded from the database: at most maxIdsPerTurbine+1 rows per turbine are fetched, so an
        // overflowing turbine can be detected and failed closed rather than silently truncated.
        const string sql = """
            SELECT turbine_rdspp, alarm_id FROM (
                SELECT turbine_rdspp, alarm_id,
                       ROW_NUMBER() OVER (PARTITION BY turbine_rdspp ORDER BY alarm_id) AS rn
                  FROM alarm_known_open_alarm
                 WHERE windfarm_id = @windfarmId AND turbine_rdspp = ANY(@turbines)
            ) ranked
             WHERE rn <= @capPlusOne;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.Add(new NpgsqlParameter("turbines", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = turbineRdspps.ToArray() });
        cmd.Parameters.AddWithValue("capPlusOne", maxIdsPerTurbine + 1);

        var mutable = turbineRdspps.ToDictionary(t => t, _ => new List<long>());

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            mutable[reader.GetString(0)].Add(reader.GetInt64(1));
        }

        var overflowing = mutable.Where(kv => kv.Value.Count > maxIdsPerTurbine).Select(kv => kv.Key).ToList();
        if (overflowing.Count > 0)
        {
            throw new InvalidOperationException(
                $"Windfarm '{windfarmId}' turbine(s) [{string.Join(", ", overflowing)}] exceed the configured " +
                $"MaxOpenAlarmsPerTurbine ({maxIdsPerTurbine}); failing closed rather than silently abandoning open alarm IDs.");
        }

        return mutable.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<long>)kv.Value);
    }

    public async Task<bool> TryEnrollOpenAlarmsAsync(
        string windfarmId, long leaseToken, IReadOnlyList<(string TurbineRdspp, long AlarmId)> newOpenAlarms, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        if (!await LeaseIsCurrentAsync(conn, tx, windfarmId, leaseToken, cancellationToken))
        {
            await tx.RollbackAsync(cancellationToken);
            return false;
        }

        foreach (var (turbineRdspp, alarmId) in newOpenAlarms)
        {
            const string insert = """
                INSERT INTO alarm_known_open_alarm (windfarm_id, turbine_rdspp, alarm_id, first_seen_utc)
                VALUES (@windfarmId, @turbineRdspp, @alarmId, clock_timestamp())
                ON CONFLICT (windfarm_id, turbine_rdspp, alarm_id) DO NOTHING;
                """;
            await using var cmd = new NpgsqlCommand(insert, conn, tx);
            AddWindfarmIdParameter(cmd.Parameters, windfarmId);
            cmd.Parameters.AddWithValue("turbineRdspp", turbineRdspp);
            cmd.Parameters.AddWithValue("alarmId", alarmId);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
        return true;
    }

    public async Task<bool> TryRemoveResolvedOpenAlarmsAsync(
        string windfarmId, long leaseToken, IReadOnlyList<(string TurbineRdspp, long AlarmId)> resolvedAlarmIds, CancellationToken cancellationToken)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        if (!await LeaseIsCurrentAsync(conn, tx, windfarmId, leaseToken, cancellationToken))
        {
            await tx.RollbackAsync(cancellationToken);
            return false;
        }

        foreach (var (turbineRdspp, alarmId) in resolvedAlarmIds)
        {
            const string delete = """
                DELETE FROM alarm_known_open_alarm
                 WHERE windfarm_id = @windfarmId AND turbine_rdspp = @turbineRdspp AND alarm_id = @alarmId;
                """;
            await using var cmd = new NpgsqlCommand(delete, conn, tx);
            AddWindfarmIdParameter(cmd.Parameters, windfarmId);
            cmd.Parameters.AddWithValue("turbineRdspp", turbineRdspp);
            cmd.Parameters.AddWithValue("alarmId", alarmId);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Fences known-open writes against the SHARED extraction_cursor lease row (the alarm-only
    /// alarm_extraction_lease table was retired): a stale/expired owner's writes never land, but
    /// this check never touches last_committed_id itself.
    /// </summary>
    private static async Task<bool> LeaseIsCurrentAsync(
        NpgsqlConnection conn, NpgsqlTransaction tx, string windfarmId, long leaseToken, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT 1 FROM extraction_cursor
             WHERE windfarm_id = @windfarmId AND source_type = 'Alarm' AND lease_token = @leaseToken
               AND lease_expires_at > clock_timestamp()
             FOR UPDATE;
            """;
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        AddWindfarmIdParameter(cmd.Parameters, windfarmId);
        cmd.Parameters.AddWithValue("leaseToken", leaseToken);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }
}
