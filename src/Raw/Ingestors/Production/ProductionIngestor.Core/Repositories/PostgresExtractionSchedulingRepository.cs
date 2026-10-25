using System.Globalization;
using Npgsql;
using NpgsqlTypes;
using ProductionIngestor.Enums;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Raw;

namespace ProductionIngestor.Repositories;

public sealed class PostgresExtractionSchedulingRepository : IExtractionSchedulingRepository
{
    private readonly Func<NpgsqlConnection> _createConnection;

    private sealed record CursorClaimCandidate(ExtractionLeaseModel Lease, bool IsBusy);

    private sealed record InsertedRunAudit(long RunId, DateTime StartedAtUtc, string OperationId);

    public PostgresExtractionSchedulingRepository(PostgresConnectionFactory factory)
        => _createConnection = factory.CreateConnection;

    public PostgresExtractionSchedulingRepository(string connectionString)
        => _createConnection = () => new NpgsqlConnection(connectionString);

    private const string DueTimeSql = """
        CASE
          WHEN active.execution_mode = 'Incremental' AND active.status = 'Running'
            THEN '-infinity'::timestamptz
                    WHEN cursor.last_run_started_utc > cursor.last_run_completed_utc
                        THEN '-infinity'::timestamptz
          WHEN cursor.last_run_completed_utc IS NULL THEN '-infinity'::timestamptz
          WHEN cursor.last_run_outcome = 'Failed' THEN cursor.last_run_completed_utc + @backoff
          ELSE cursor.last_run_completed_utc + @interval
        END
        """;

    public async Task<bool> IsRegisteredAsync(ExtractionLookupModel lookup, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT EXISTS (SELECT 1 FROM extraction_cursor
                WHERE source_type = @sourceType
                  AND windfarm_id = @windfarmId AND is_enabled AND isfinite(last_committed_updated_at));
            """, connection);
        command.Parameters.AddWithValue("sourceType", lookup.Scope.SourceType);
        command.Parameters.AddWithValue("windfarmId", lookup.WindfarmId);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    public async Task<ExtractionSelectionStatusModel> GetSelectionStatusAsync(
        ExtractionClaimRequestModel request, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand($"""
            SELECT count(*)::integer,
                count(*) FILTER (WHERE cursor.lease_expires_at > statement_timestamp())::integer,
                count(*) FILTER (WHERE (cursor.lease_expires_at IS NULL OR cursor.lease_expires_at <= statement_timestamp())
                    AND ({DueTimeSql}) <= statement_timestamp())::integer
            FROM extraction_cursor cursor LEFT JOIN extraction_run active ON active.id = cursor.active_run_id
            WHERE cursor.source_type = @sourceType
              AND cursor.is_enabled AND isfinite(cursor.last_committed_updated_at);
            """, connection);
        AddSelectionParameters(command, request);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new(reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
    }

    // Paste these members inside PostgresExtractionSchedulingRepository. They use the
    // updated int WindfarmId and WindfarmName models shown in the supplied method.
    // Keep the class's existing DueTimeSql, AddSelectionParameters and
    // AddIdentityParameters helpers. Rename the interface/call sites to the public
    // method name below (or retain TryClaimAsync as a delegating compatibility shim).

    public async Task<ExtractionClaimModel?> TryClaimCursorAndStartExtractionRunAsync(
        ExtractionClaimRequestModel request, CancellationToken cancellationToken)
    {
        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = (NpgsqlTransaction)await connection.BeginTransactionAsync(cancellationToken);

        var candidate = await SelectCursorForClaimAsync(connection, transaction, request, cancellationToken);
        if (candidate is null)
            return null;

        var ingestion = CreateIngestionRequest(request, candidate.Lease);
        if (request.OperationId is not null && ingestion.OperationId is not null && request.OperationId != ingestion.OperationId)
            throw new InvalidDataException("Request OperationId values disagree.");
        ingestion = ingestion with { OperationId = ingestion.OperationId ?? request.OperationId };
        
        var outputPath = candidate.IsBusy
            ? null
            : RawPathBuilder.BuildWindowExtractionPrefix(candidate.Lease.WindfarmName, candidate.Lease.LeaseToken,
                request.Refresh is not null ? IngestionMode.Refresh : IngestionMode.Incremental);

        if (!candidate.IsBusy)
            await FailPreviousRunningAttemptsAsync(connection, transaction, candidate.Lease, cancellationToken);

        var audit = await InsertExtractionRunAuditAsync(
            connection, transaction, request, candidate, ingestion, outputPath, cancellationToken);

        if (candidate.IsBusy)
        {
            // Persist the Busy audit row, without claiming or replacing the existing lease.
            await transaction.CommitAsync(cancellationToken);
            return null;
        }

        var lease = await AttachRunAndAcquireLeaseAsync(
            connection, transaction, request, candidate.Lease, audit.RunId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);

		var run = new ExtractionRunModel(
		    audit.RunId, lease.WindfarmId, lease.SourceType,
		    ExtractionRunStatus.Running,
		    audit.StartedAtUtc, null,
		    lease.LastUpdatedUtc, lease.LastCommittedId,
		    null, null, outputPath, null,
		    OperationId: audit.OperationId);

		ingestion = ingestion with { OperationId = audit.OperationId };

        return new ExtractionClaimModel(ingestion, lease, run, request.OwnerId);
    }

    private async Task<CursorClaimCandidate?> SelectCursorForClaimAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ExtractionClaimRequestModel request,
        CancellationToken cancellationToken)
    {
        var isRefresh = request.Refresh is not null;
        var selectionSql = $"""
        SELECT cursor.windfarm_id, cursor.lease_token, cursor.last_committed_updated_at,
            cursor.last_committed_id, cursor.lease_expires_at > clock_timestamp(), w.windfarm_name
        FROM extraction_cursor cursor
            LEFT JOIN extraction_run active ON active.id = cursor.active_run_id
            JOIN windfarm w ON w.id = cursor.windfarm_id
        WHERE cursor.source_type = @sourceType
          AND cursor.is_enabled AND isfinite(cursor.last_committed_updated_at)
          AND {(isRefresh
                  ? "cursor.windfarm_id = @windfarmId"
                  : $"(cursor.lease_expires_at IS NULL OR cursor.lease_expires_at <= statement_timestamp()) AND ({DueTimeSql}) <= statement_timestamp()")}
        ORDER BY ({DueTimeSql}), cursor.source_type, cursor.windfarm_id
        LIMIT 1 FOR UPDATE OF cursor {(isRefresh ? "" : "SKIP LOCKED")};
        """;

        await using var command = new NpgsqlCommand(selectionSql, connection, transaction);
        AddSelectionParameters(command, request);
        if (isRefresh)
            command.Parameters.AddWithValue("windfarmId", request.Refresh!.WindfarmId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        var isBusy = !reader.IsDBNull(4) && reader.GetBoolean(4);
        var lease = new ExtractionLeaseModel
        {
            WindfarmId = reader.GetInt32(0),
            WindfarmName = reader.GetString(5),
            SourceType = request.Scope.SourceType,
            LeaseToken = reader.GetInt64(1) + (isBusy ? 0 : 1),
            LastUpdatedUtc = reader.GetDateTime(2),
            LastCommittedId = reader.GetString(3),
            LeaseExpiresAtUtc = null
        };
        return new CursorClaimCandidate(lease, isBusy);
    }

    private IngestionRequestModel CreateIngestionRequest(
        ExtractionClaimRequestModel request, ExtractionLeaseModel lease)
        => request.Refresh ?? new IngestionRequestModel(
            lease.WindfarmId, lease.WindfarmName, lease.SourceType,
            IngestionMode.Incremental, RunId: Guid.NewGuid().ToString("N"));

    private async Task FailPreviousRunningAttemptsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ExtractionLeaseModel lease,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
        UPDATE extraction_run SET status = 'Failed', completed_at = clock_timestamp(),
            to_updated_at = @checkpoint, to_committed_id = @committedId,
            error_message = 'LeaseExpired'
        WHERE windfarm_id = @windfarmId AND source_type = @sourceType AND status = 'Running';
        """, connection, transaction);
        AddIdentityParameters(command, lease);
        command.Parameters.AddWithValue("checkpoint", lease.LastUpdatedUtc);
        command.Parameters.AddWithValue("committedId", lease.LastCommittedId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<InsertedRunAudit> InsertExtractionRunAuditAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ExtractionClaimRequestModel request,
        CursorClaimCandidate candidate,
        IngestionRequestModel ingestion,
        string? outputPath,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
        INSERT INTO extraction_run(windfarm_id, source_type, status, execution_mode,
            created_by, request_id, lease_token, lease_owner, job_name, requested_by, reason, requested_dates,
            from_updated_at, from_committed_id, output_path, operation_id, completed_at, error_message)
        VALUES (@windfarmId, @sourceType, @status, @mode, @createdBy, @requestId, @token, @owner,
            @job, @requestedBy, @reason, @dates, @checkpoint, @committedId, @path, control.resolve_operation(@operationId, @createdBy),
            CASE WHEN @busy THEN clock_timestamp() ELSE NULL END,
            CASE WHEN @busy THEN 'LeaseHeldByOther' ELSE NULL END)
        RETURNING id, started_at, operation_id;
        """, connection, transaction);

        var lease = candidate.Lease;
        AddIdentityParameters(command, lease);
        command.Parameters.AddWithValue("status", candidate.IsBusy ? "Busy" : "Running");
        command.Parameters.AddWithValue("mode", request.Refresh is not null ? "Refresh" : "Incremental");
        command.Parameters.Add("createdBy", NpgsqlDbType.Text).Value =
            string.IsNullOrWhiteSpace(request.RequestedBy) ? request.OwnerId : request.RequestedBy;
        command.Parameters.Add("operationId", NpgsqlDbType.Text).Value = (object?)ingestion.OperationId ?? DBNull.Value;
        command.Parameters.AddWithValue("requestId", ingestion.RunId ?? Guid.NewGuid().ToString("N"));
        command.Parameters.Add("token", NpgsqlDbType.Bigint).Value = candidate.IsBusy ? DBNull.Value : lease.LeaseToken;
        command.Parameters.AddWithValue("owner", request.OwnerId);
        command.Parameters.Add("job", NpgsqlDbType.Text).Value = (object?)request.JobName ?? DBNull.Value;
        command.Parameters.Add("requestedBy", NpgsqlDbType.Text).Value = (object?)request.RequestedBy ?? DBNull.Value;
        command.Parameters.Add("reason", NpgsqlDbType.Text).Value = (object?)request.Reason ?? DBNull.Value;
        command.Parameters.Add("dates", NpgsqlDbType.Array | NpgsqlDbType.Date).Value =
            (object?)ingestion.ExplicitDates?.ToArray() ?? DBNull.Value;
        command.Parameters.AddWithValue("checkpoint", lease.LastUpdatedUtc);
        command.Parameters.AddWithValue("committedId", lease.LastCommittedId);
        command.Parameters.Add("path", NpgsqlDbType.Text).Value = (object?)outputPath ?? DBNull.Value;
        command.Parameters.AddWithValue("busy", candidate.IsBusy);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            throw new InvalidOperationException("The extraction_run insert did not return an audit ID.");
        return new InsertedRunAudit(reader.GetInt64(0), reader.GetDateTime(1), reader.GetString(2));
    }

    private async Task<ExtractionLeaseModel> AttachRunAndAcquireLeaseAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ExtractionClaimRequestModel request,
        ExtractionLeaseModel lease,
        long runId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("""
        UPDATE extraction_cursor SET lease_token = @token, lease_owner = @owner,
            lease_expires_at = clock_timestamp() + @duration, active_run_id = @runId,
            last_run_started_utc = CASE WHEN @refresh THEN last_run_started_utc ELSE clock_timestamp() END
        WHERE windfarm_id = @windfarmId AND source_type = @sourceType
        RETURNING lease_expires_at;
        """, connection, transaction);

        AddIdentityParameters(command, lease);
        command.Parameters.AddWithValue("token", lease.LeaseToken);
        command.Parameters.AddWithValue("owner", request.OwnerId);
        command.Parameters.AddWithValue("duration", request.Policy.LeaseDuration);
        command.Parameters.AddWithValue("runId", runId);
        command.Parameters.AddWithValue("refresh", request.Refresh is not null);

        var expiresAt = await command.ExecuteScalarAsync(cancellationToken);
        if (expiresAt is not DateTime expiry)
            throw new InvalidOperationException("The cursor lease update did not return its expiry.");

        return new ExtractionLeaseModel
        {
            WindfarmId = lease.WindfarmId,
            WindfarmName = lease.WindfarmName,
            SourceType = lease.SourceType,
            LeaseToken = lease.LeaseToken,
            LastUpdatedUtc = lease.LastUpdatedUtc,
            LastCommittedId = lease.LastCommittedId,
            LeaseExpiresAtUtc = expiry
        };
    }

    public async Task<bool> TryFinalizeAsync(ExtractionAttemptResultModel attempt, CancellationToken cancellationToken)
    {
        var claim = attempt.Claim;
        var success = attempt.Result.Outcome == RunOutcome.Success;
        var advancesCheckpoint = claim.Request.Mode == IngestionMode.Incremental && success;
        if (advancesCheckpoint &&
            (attempt.Result.CompletedCheckpointUpdatedUtc is null || attempt.Result.CompletedCheckpointId is null))
        {
            throw new InvalidOperationException("A successful incremental attempt must include its completed checkpoint.");
        }

        await using var connection = _createConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            WITH owned AS (
                SELECT cursor.* FROM extraction_cursor cursor
                WHERE windfarm_id = @windfarmId AND source_type = @sourceType
                  AND lease_token = @token AND lease_owner = @owner
                  AND lease_expires_at > clock_timestamp() AND active_run_id = @runId
                                    AND (NOT @advancesCheckpoint OR last_committed_updated_at <= @checkpointUpdatedAt)
                FOR UPDATE
            ), finished AS (
                UPDATE extraction_run run SET status = @status, completed_at = clock_timestamp(),
                    to_updated_at = CASE WHEN run.execution_mode = 'Incremental' AND @success
                        THEN @checkpointUpdatedAt ELSE owned.last_committed_updated_at END,
                    to_committed_id = CASE WHEN run.execution_mode = 'Incremental' AND @success
                        THEN @checkpointId ELSE owned.last_committed_id END,
                    error_message = @error,
                    query_start_inclusive_utc = @queryStart, query_end_exclusive_utc = @queryEnd
                FROM owned WHERE run.id = @runId AND run.status = 'Running'
                    AND run.lease_token = @token AND run.lease_owner = @owner
                RETURNING run.id, run.execution_mode, run.to_updated_at, run.to_committed_id
            )
            UPDATE extraction_cursor cursor SET
                last_committed_updated_at = CASE WHEN finished.execution_mode = 'Incremental' AND @success
                    THEN finished.to_updated_at ELSE cursor.last_committed_updated_at END,
                last_committed_id = CASE WHEN finished.execution_mode = 'Incremental' AND @success
                    THEN finished.to_committed_id ELSE cursor.last_committed_id END,
                last_committed_utc = CASE WHEN finished.execution_mode = 'Incremental' AND @success
                    THEN clock_timestamp() ELSE cursor.last_committed_utc END,
                last_run_completed_utc = CASE WHEN finished.execution_mode = 'Incremental' THEN clock_timestamp() ELSE cursor.last_run_completed_utc END,
                last_run_outcome = CASE WHEN finished.execution_mode = 'Incremental' THEN @incrementalOutcome ELSE cursor.last_run_outcome END,
                consecutive_failures = CASE WHEN finished.execution_mode <> 'Incremental' THEN cursor.consecutive_failures
                    WHEN @success THEN 0 ELSE cursor.consecutive_failures + 1 END,
                last_error = CASE WHEN finished.execution_mode = 'Incremental' THEN @error ELSE cursor.last_error END,
                lease_owner = NULL, lease_expires_at = NULL, active_run_id = NULL
            FROM finished WHERE cursor.windfarm_id = @windfarmId AND cursor.source_type = @sourceType
            RETURNING finished.id;
            """, connection, transaction);
        AddIdentityParameters(command, claim.Lease);
        command.Parameters.AddWithValue("token", claim.Lease.LeaseToken);
        command.Parameters.AddWithValue("owner", claim.OwnerId);
        command.Parameters.AddWithValue("runId", claim.Run.RunId);
        command.Parameters.AddWithValue("status", success ? "Completed" : attempt.Result.Outcome == RunOutcome.Cancelled ? "Cancelled" : "Failed");
        command.Parameters.AddWithValue("incrementalOutcome", success ? "Completed" : "Failed");
        command.Parameters.AddWithValue("success", success);
        command.Parameters.AddWithValue("advancesCheckpoint", advancesCheckpoint);
        command.Parameters.Add("checkpointUpdatedAt", NpgsqlDbType.TimestampTz).Value =
            (object?)attempt.Result.CompletedCheckpointUpdatedUtc ?? DBNull.Value;
        command.Parameters.Add("checkpointId", NpgsqlDbType.Text).Value =
            (object?)attempt.Result.CompletedCheckpointId ?? DBNull.Value;
        command.Parameters.Add("error", NpgsqlDbType.Text).Value = success ? DBNull.Value : attempt.Result.Error ?? attempt.Result.Outcome.ToString();
        command.Parameters.Add("queryStart", NpgsqlDbType.TimestampTz).Value =
            attempt.Result.QueryStartInclusiveUtc is { } queryStart ? queryStart : DBNull.Value;
        command.Parameters.Add("queryEnd", NpgsqlDbType.TimestampTz).Value =
            attempt.Result.QueryEndExclusiveUtc is { } queryEnd ? queryEnd : DBNull.Value;
        var completed = await command.ExecuteScalarAsync(cancellationToken) is not null;
        await transaction.CommitAsync(cancellationToken);
        return completed;
    }

    private void AddSelectionParameters(NpgsqlCommand command, ExtractionClaimRequestModel request)
    {
        command.Parameters.AddWithValue("sourceType", request.Scope.SourceType);
        command.Parameters.AddWithValue("interval", request.Policy.MinimumIngestionInterval);
        command.Parameters.AddWithValue("backoff", request.Policy.FailureRetryBackoff);
    }

    private void AddIdentityParameters(NpgsqlCommand command, ExtractionLeaseModel lease)
    {
        command.Parameters.AddWithValue("windfarmId", lease.WindfarmId);
        command.Parameters.AddWithValue("sourceType", lease.SourceType);
    }
}