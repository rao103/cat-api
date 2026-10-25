using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using Npgsql;
using NpgsqlTypes;
namespace CAT.RawFanOut.Repositories;

public sealed class PostgresDeadLetterStore(IPostgresConnectionFactory connectionFactory) : IDeadLetterStore
{
    public async Task<DeadLetterArchiveResult> RecordAsync(DeadLetterArchiveRequest request, CancellationToken ct)
    {
        var delivery = request.Delivery;
        var artifact = request.Artifact;
        await using var connection = await connectionFactory.OpenConnectionAsync(ct);
        // The conflict branch only fires for byte-identical bodies; a different body returns no row.
        await using var command = new NpgsqlCommand("""
            INSERT INTO control.raw_fan_out_dead_letter
              (incident_id, queue_url, message_id, original_body, attributes, artifact_paths, inserted_by,
               last_error, error_type, source_type, windfarm_id, extraction_token, run_id, trace_id, artifact_id, data_date)
            VALUES (@id, @queue, @message, @body, @attributes, @paths, @actor,
                    @lastError, @errorType, @source, @farm, @token, @run, @trace, @artifact, @date)
            ON CONFLICT (queue_url, message_id) DO UPDATE
              SET last_observed_at_utc = clock_timestamp(), updated_by = @actor
              WHERE control.raw_fan_out_dead_letter.original_body = EXCLUDED.original_body
            RETURNING incident_id, (xmax = 0) AS created;
            """, connection);
        command.Parameters.AddWithValue("id", Guid.NewGuid());
        command.Parameters.AddWithValue("queue", request.QueueUrl);
        command.Parameters.AddWithValue("message", delivery.MessageId);
        command.Parameters.AddWithValue("body", delivery.Body);
        command.Parameters.AddWithValue("attributes", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(delivery.Attributes));
        command.Parameters.AddWithValue("paths", NpgsqlDbType.Array | NpgsqlDbType.Text, artifact?.Paths ?? []);
        command.Parameters.AddWithValue("actor", ServiceActors.DeadLetterWorker);
        command.Parameters.AddWithValue("lastError", NpgsqlDbType.Text, (object?)request.LastError ?? DBNull.Value);
        command.Parameters.AddWithValue("errorType", NpgsqlDbType.Text, (object?)request.ErrorType ?? DBNull.Value);
        command.Parameters.AddWithValue("source", NpgsqlDbType.Varchar, (object?)artifact?.SourceType ?? DBNull.Value);
        command.Parameters.AddWithValue("farm", NpgsqlDbType.Integer, artifact is null ? DBNull.Value : artifact.WindfarmId);
        command.Parameters.AddWithValue("token", NpgsqlDbType.Bigint, artifact is null ? DBNull.Value : artifact.ExtractionToken);
        command.Parameters.AddWithValue("run", NpgsqlDbType.Varchar, (object?)artifact?.RunId ?? DBNull.Value);
        command.Parameters.AddWithValue("trace", NpgsqlDbType.Text, (object?)artifact?.TraceId ?? DBNull.Value);
        command.Parameters.AddWithValue("artifact", NpgsqlDbType.Text, (object?)artifact?.ArtifactId ?? DBNull.Value);
        command.Parameters.AddWithValue("date", NpgsqlDbType.Date, artifact?.DataDate is { } d ? d : DBNull.Value);

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            throw new DeadLetterIntegrityException(request.QueueUrl, delivery.MessageId);
        return new DeadLetterArchiveResult(reader.GetGuid(0), reader.GetBoolean(1));
    }

    public async Task<DeadLetterIncident> LoadAsync(Guid id, CancellationToken ct)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT original_body, resolved_at_utc IS NOT NULL, artifact_id, source_type, windfarm_id,
                   extraction_token, run_id, trace_id
            FROM control.raw_fan_out_dead_letter WHERE incident_id = @id;
            """;
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) throw new KeyNotFoundException("DLQ incident does not exist.");
        return new DeadLetterIncident(id, reader.GetString(0), reader.GetBoolean(1))
        {
            ArtifactId = reader.IsDBNull(2) ? null : reader.GetString(2),
            SourceType = reader.IsDBNull(3) ? null : reader.GetString(3),
            WindfarmId = reader.IsDBNull(4) ? null : reader.GetInt32(4),
            ExtractionToken = reader.IsDBNull(5) ? null : reader.GetInt64(5),
            RunId = reader.IsDBNull(6) ? null : reader.GetString(6),
            TraceId = reader.IsDBNull(7) ? null : reader.GetString(7)
        };
    }

    public async Task MarkReplayRequestedAsync(Guid id, CancellationToken ct)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand("""
            UPDATE control.raw_fan_out_dead_letter
            SET replay_requested_at_utc = clock_timestamp(), updated_by = @actor
            WHERE incident_id = @id AND resolved_at_utc IS NULL;
            """, connection);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("actor", ServiceActors.DeadLetterWorker);
        if (await command.ExecuteNonQueryAsync(ct) == 0) await EnsureExistsAsync(connection, id, ct);
    }

    public async Task ResolveAsync(ResolveIncidentRequest request, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.Reason))
            throw new ArgumentException("A resolution reason is required.", nameof(request));
        var actor = ServiceActors.Operator(request.Operator);
        await using var connection = await connectionFactory.OpenConnectionAsync(ct);
        // Closing an already-resolved incident is a no-op so the original resolution stays stable.
        await using var command = new NpgsqlCommand("""
            UPDATE control.raw_fan_out_dead_letter
            SET resolved_at_utc = clock_timestamp(), resolution_reason = @reason, resolved_by = @actor, updated_by = @actor
            WHERE incident_id = @id AND resolved_at_utc IS NULL;
            """, connection);
        command.Parameters.AddWithValue("id", request.IncidentId);
        command.Parameters.AddWithValue("reason", request.Reason.Trim());
        command.Parameters.AddWithValue("actor", actor);
        if (await command.ExecuteNonQueryAsync(ct) == 0) await EnsureExistsAsync(connection, request.IncidentId, ct);
    }

    private static async Task EnsureExistsAsync(NpgsqlConnection connection, Guid id, CancellationToken ct)
    {
        await using var exists = new NpgsqlCommand(
            "SELECT 1 FROM control.raw_fan_out_dead_letter WHERE incident_id = @id", connection);
        exists.Parameters.AddWithValue("id", id);
        if (await exists.ExecuteScalarAsync(ct) is null) throw new KeyNotFoundException("DLQ incident does not exist.");
    }
}
