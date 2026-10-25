using Centralize.Core.Interfaces;
using Centralize.Core.Models;
using Centralize.Core.Validators;
using Npgsql;

namespace Centralize.Core.Repositories;

public sealed class PostgresOperationRepository(Func<NpgsqlConnection> createConnection) : IOperationRepository
{
    public PostgresOperationRepository(NpgsqlDataSource dataSource) : this(dataSource.CreateConnection) { }

    public async Task<IReadOnlyList<RefreshSourceModel>> GetRefreshSourcesAsync(OperationRequestModel request, CancellationToken cancellationToken)
    {
        request = OperationRequestValidator.Normalize(request);
        cancellationToken.ThrowIfCancellationRequested();
        await using var connection = createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var select = new NpgsqlCommand("""
            SELECT cursor.windfarm_id, farm.windfarm_name, cursor.source_type
            FROM public.extraction_cursor cursor
            JOIN public.windfarm farm ON farm.id = cursor.windfarm_id
            WHERE cursor.windfarm_id = @farm AND cursor.is_enabled
              AND (cursor.source_type = 'Alarm' OR isfinite(cursor.last_committed_updated_at))
              AND cursor.source_type = ANY(@sources)
            ORDER BY cursor.source_type;
            """, connection);
        select.Parameters.AddWithValue("farm", request.WindfarmId);
        select.Parameters.AddWithValue("sources", request.SourceType is null
            ? OperationRequestValidator.SupportedSources.ToArray() : new[] { request.SourceType });
        var sources = new List<RefreshSourceModel>();
        await using var reader = await select.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            sources.Add(new(reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
        if (sources.Count == 0)
            throw new KeyNotFoundException("No enabled, refresh-supported source is configured for this windfarm.");
        return sources;
    }

    public async Task<OperationModel> CreateAsync(string requestedBy, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestedBy);
        if (requestedBy.Length > 256) throw new ArgumentException("Requester must not exceed 256 characters.");
        cancellationToken.ThrowIfCancellationRequested();
        await using var connection = createConnection();
        await connection.OpenAsync(cancellationToken);
        var operationId = Guid.NewGuid();
        await using var insert = new NpgsqlCommand("""
            INSERT INTO control.operation(operation_id, requested_by) VALUES (@id, @creator)
            RETURNING created_date_utc;
            """, connection);
        insert.Parameters.AddWithValue("id", operationId);
        insert.Parameters.AddWithValue("creator", requestedBy.Trim());
        var created = new DateTimeOffset((DateTime)(await insert.ExecuteScalarAsync(cancellationToken))!);
        return new(operationId, created, requestedBy.Trim(), [], []);
    }

    public async Task<OperationModel?> GetAsync(Guid operationId, string requestedBy, CancellationToken cancellationToken)
    {
        if (operationId == Guid.Empty) throw new ArgumentException("OperationId is required.");
        ArgumentException.ThrowIfNullOrWhiteSpace(requestedBy);
        await using var connection = createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        DateTimeOffset created;
        string creator;
        await using (var select = new NpgsqlCommand("""
            SELECT created_date_utc, requested_by FROM control.operation
            WHERE operation_id = @id AND requested_by = @creator;
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("id", operationId);
            select.Parameters.AddWithValue("creator", requestedBy);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) return null;
            created = new(reader.GetDateTime(0));
            creator = reader.GetString(1);
        }
        var runs = new List<ExtractionStatusModel>();
        await using (var select = new NpgsqlCommand("""
            SELECT id, windfarm_id, source_type, execution_mode, status, requested_dates,
                started_at, completed_at, error_message FROM public.extraction_run
            WHERE operation_id = @id ORDER BY id;
            """, connection, transaction))
        {
            select.Parameters.AddWithValue("id", operationId);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                runs.Add(new(reader.GetInt64(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3),
                    reader.GetString(4), reader.IsDBNull(5) ? [] : reader.GetFieldValue<DateOnly[]>(5),
                    reader.IsDBNull(6) ? null : new DateTimeOffset(reader.GetDateTime(6)),
                    reader.IsDBNull(7) ? null : new DateTimeOffset(reader.GetDateTime(7)),
                    reader.IsDBNull(8) ? null : reader.GetString(8)));
        }
        var stages = new List<StageStatusModel>();
        (string Stage, string Table, string Sql)[] stageQueries =
        [
            ("RawFanOut", "control.raw_artifact_dispatch", """
                SELECT count(*), count(*) FILTER (WHERE fan_out_completed_at_utc IS NOT NULL), 0::bigint
                FROM control.raw_artifact_dispatch WHERE original_envelope->>'OperationId' = @id;
                """),
            ("Harmonizer", "control.harmonizer_batch", """
                SELECT count(*), count(*) FILTER (WHERE completed_at_utc IS NOT NULL),
                    count(*) FILTER (WHERE last_error_code IS NOT NULL AND completed_at_utc IS NULL)
                FROM control.harmonizer_batch WHERE operation_id = @id;
                """),
            ("HarmonizerHandoff", "control.harmonizer_outbox", """
                SELECT count(*), count(*) FILTER (WHERE sent_at_utc IS NOT NULL), 0::bigint
                FROM control.harmonizer_outbox WHERE operation_id = @id;
                """)
        ];
        foreach (var stage in stageQueries)
        {
            await using var exists = new NpgsqlCommand("SELECT to_regclass(@table) IS NOT NULL;", connection, transaction);
            exists.Parameters.AddWithValue("table", stage.Table);
            if (!(bool)(await exists.ExecuteScalarAsync(cancellationToken))!)
            {
                stages.Add(new(stage.Stage, "Unavailable", 0, 0, 0));
                continue;
            }

            await using var select = new NpgsqlCommand(stage.Sql, connection, transaction);
            select.Parameters.AddWithValue("id", operationId.ToString("D"));
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            var total = reader.GetInt64(0);
            var completed = reader.GetInt64(1);
            var failed = reader.GetInt64(2);
            stages.Add(new(stage.Stage, total == 0 ? "NotObserved" :
                failed > 0 ? "Failed" : completed == total ? "Completed" : "Pending", total, completed, failed));
        }
        await transaction.CommitAsync(cancellationToken);
        return new(operationId, created, creator, runs, stages);
    }
    public async Task<IReadOnlyList<RefreshAttempt>> GetRefreshAttemptsAsync(string requestId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        await using var connection = createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT run.id, run.request_id, run.windfarm_id, run.execution_mode, run.status,
                run.started_at, run.completed_at, run.job_name, run.error_message,
                run.requested_dates, cursor.lease_expires_at
            FROM public.extraction_run run LEFT JOIN public.extraction_cursor cursor ON cursor.active_run_id = run.id
                AND cursor.windfarm_id = run.windfarm_id AND cursor.source_type = run.source_type
            WHERE run.request_id = @id AND run.source_type = 'Production' AND run.execution_mode = 'Refresh'
            ORDER BY run.id DESC LIMIT 100;
            """, connection);
        command.Parameters.AddWithValue("id", requestId);
        var attempts = new List<RefreshAttempt>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            attempts.Add(new(reader.GetInt64(0), reader.GetString(1),
                reader.GetInt32(2).ToString(System.Globalization.CultureInfo.InvariantCulture),
                reader.GetString(3), reader.GetString(4),
                reader.IsDBNull(5) ? null : new DateTimeOffset(reader.GetDateTime(5)),
                reader.IsDBNull(6) ? null : new DateTimeOffset(reader.GetDateTime(6)),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? [] : reader.GetFieldValue<DateOnly[]>(9),
                reader.IsDBNull(10) ? null : new DateTimeOffset(reader.GetDateTime(10))));
        return attempts;
    }
}
