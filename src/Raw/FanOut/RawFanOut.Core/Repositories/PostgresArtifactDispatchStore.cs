using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using Npgsql;
using NpgsqlTypes;

namespace CAT.RawFanOut.Repositories;

public sealed class PostgresArtifactDispatchStore : IArtifactDispatchStore
{
    private readonly IPostgresConnectionFactory connectionFactory;

    public PostgresArtifactDispatchStore(IPostgresConnectionFactory connectionFactory)
    {
        this.connectionFactory = connectionFactory;
    }

    public async Task<IReadOnlyList<OutgoingMessage>?> LoadPlanAsync(RawEnvelope artifact, CancellationToken cancellationToken)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);

        int expectedBatchCount;
        await using (var parentCommand = connection.CreateCommand())
        {
            parentCommand.CommandText = """
                SELECT original_envelope = @envelope::jsonb, expected_batch_count
                FROM control.raw_artifact_dispatch WHERE artifact_id = @id
                """;
            parentCommand.Parameters.AddWithValue("id", artifact.ArtifactId);
            parentCommand.Parameters.AddWithValue("envelope", NpgsqlDbType.Jsonb, artifact.OriginalBody);
            await using var parentReader = await parentCommand.ExecuteReaderAsync(cancellationToken);
            // A missing parent row means there is no saved plan yet; the caller will plan and save one.
            if (!await parentReader.ReadAsync(cancellationToken)) return null;
            if (!parentReader.GetBoolean(0)) throw new InvalidDataException("Immutable artifact envelope changed.");
            expectedBatchCount = parentReader.GetInt32(1);
        }

        var plan = new List<OutgoingMessage>(expectedBatchCount);
        await using (var childCommand = connection.CreateCommand())
        {
            childCommand.CommandText = """
                SELECT batch_id, message_body FROM control.raw_artifact_batch
                WHERE artifact_id = @id ORDER BY batch_ordinal
                """;
            childCommand.Parameters.AddWithValue("id", artifact.ArtifactId);
            await using var childReader = await childCommand.ExecuteReaderAsync(cancellationToken);
            while (await childReader.ReadAsync(cancellationToken))
                plan.Add(new OutgoingMessage(childReader.GetString(0), childReader.GetString(1)));
        }

        // An existing parent with an inconsistent child count must fail validation, never silently replan.
        if (plan.Count != expectedBatchCount)
            throw new InvalidDataException(
                $"Persisted artifact plan is inconsistent: expected {expectedBatchCount} batches, found {plan.Count}.");

        return plan;
    }

    public async Task<IReadOnlyList<OutgoingMessage>> SavePlanAsync(
        RawEnvelope artifact,
        IReadOnlyList<OutgoingMessage> proposed,
        CancellationToken cancellationToken)
    {
        // The SQL function locks the artifact row and commits parent plus children atomically.
        // A duplicate call returns the previously committed child IDs/bodies, never a new plan.
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT batch_id, message_body FROM control.raw_artifact_save_plan(@id,@source,@farm,@run,@token,@envelope,@plan,@actor)
            ORDER BY batch_ordinal
            """;
        command.Parameters.AddWithValue("id", artifact.ArtifactId);
        command.Parameters.AddWithValue("source", artifact.SourceType);
        command.Parameters.AddWithValue("farm", artifact.WindfarmId);
        command.Parameters.AddWithValue("run", artifact.RunId);
        command.Parameters.AddWithValue("token", artifact.ExtractionToken);
        command.Parameters.AddWithValue("envelope", NpgsqlDbType.Jsonb, artifact.OriginalBody);
        command.Parameters.AddWithValue("plan", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(proposed));
        command.Parameters.AddWithValue("actor", ServiceActors.FanOutWorker);

        var plan = new List<OutgoingMessage>(proposed.Count);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            plan.Add(new OutgoingMessage(reader.GetString(0), reader.GetString(1)));

        if (plan.Count == 0)
            throw new InvalidDataException("Database did not return a persisted artifact plan.");

        return plan;
    }

    public async Task MarkDispatchedAsync(string artifactId, CancellationToken cancellationToken)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT control.raw_artifact_mark_dispatched(@id,@actor)";
        command.Parameters.AddWithValue("id", artifactId);
        command.Parameters.AddWithValue("actor", ServiceActors.FanOutWorker);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
