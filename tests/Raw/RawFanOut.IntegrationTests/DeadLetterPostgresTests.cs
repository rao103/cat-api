using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.DeadLetters;
using CAT.RawFanOut.Repositories;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
namespace CAT.RawFanOut.IntegrationTests;

[Collection("Postgres")]
public sealed class DeadLetterPostgresTests
{
    private static QueueDelivery Delivery(string body, string? id = null) =>
        new(id ?? Guid.NewGuid().ToString(), "receipt", body, new Dictionary<string, string>());

    private static RawEnvelope Envelope(params string[] paths) =>
        RawEnvelope.Parse(ParquetFixtures.Body("Outage", paths), new());

    [ExternalIntegrationFact]
    public async Task ArchiveKeepsTheWholeShardedEnvelopeWithBestEffortMetadataAndActor()
    {
        await using var db = await PostgresTests.OpenAsync();
        var store = new PostgresDeadLetterStore(new TestPostgresConnectionFactory(db));
        var prefix = Guid.NewGuid().ToString("N");
        var artifact = Envelope($"raw/{prefix}-a.parquet", $"raw/{prefix}-b.parquet");
        var delivery = Delivery(artifact.OriginalBody);

        var result = await store.RecordAsync(new DeadLetterArchiveRequest("dlq", delivery, artifact), default);

        Assert.True(result.Created);
        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE incident_id=@i
              AND original_body=@body AND cardinality(artifact_paths)=2 AND artifact_id=@artifact
              AND source_type='Outage' AND windfarm_id=7 AND extraction_token=41 AND run_id='integration-run'
              AND trace_id='integration-trace' AND data_date IS NULL
              AND last_error IS NULL AND error_type IS NULL
              AND inserted_by='raw-fanout-dlq-worker' AND last_observed_at_utc=first_observed_at_utc
            """, ("i", result.IncidentId), ("body", artifact.OriginalBody), ("artifact", artifact.ArtifactId)));
    }

    [ExternalIntegrationFact]
    public async Task MalformedJsonIsArchivedVerbatimWithoutInventedIdentifiers()
    {
        await using var db = await PostgresTests.OpenAsync();
        var store = new PostgresDeadLetterStore(new TestPostgresConnectionFactory(db));
        var message = Delivery("{broken");

        await new DeadLetterProcessor(store, new()).ProcessAsync("dlq", message, default);

        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE message_id=@m AND original_body='{broken'
              AND artifact_id IS NULL AND source_type IS NULL AND windfarm_id IS NULL AND extraction_token IS NULL
              AND run_id IS NULL AND trace_id IS NULL AND cardinality(artifact_paths)=0
              AND error_type LIKE '%Json%' AND last_error IS NOT NULL
            """, ("m", message.MessageId)));
    }

    [ExternalIntegrationFact]
    public async Task DuplicateDeliveryIsIdempotentAndNeverReopensAResolvedIncident()
    {
        await using var db = await PostgresTests.OpenAsync();
        var store = new PostgresDeadLetterStore(new TestPostgresConnectionFactory(db));
        var artifact = Envelope($"raw/{Guid.NewGuid():N}.parquet");
        var delivery = Delivery(artifact.OriginalBody);
        var request = new DeadLetterArchiveRequest("dlq", delivery, artifact);

        var first = await store.RecordAsync(request, default);
        var second = await store.RecordAsync(request, default);

        Assert.True(first.Created);
        Assert.False(second.Created);
        Assert.Equal(first.IncidentId, second.IncidentId);
        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, "SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE message_id=@m", ("m", delivery.MessageId)));
        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE incident_id=@i
              AND last_observed_at_utc > first_observed_at_utc AND original_body=@body
            """, ("i", first.IncidentId), ("body", artifact.OriginalBody)));

        await store.ResolveAsync(new ResolveIncidentRequest(first.IncidentId, "verified downstream", "alice"), default);
        await store.RecordAsync(request, default);
        Assert.True((await store.LoadAsync(first.IncidentId, default)).Resolved);
    }

    [ExternalIntegrationFact]
    public async Task ConflictingPayloadForTheSameMessageIdentityIsAnIntegrityError()
    {
        await using var db = await PostgresTests.OpenAsync();
        var store = new PostgresDeadLetterStore(new TestPostgresConnectionFactory(db));
        var id = Guid.NewGuid().ToString();
        var original = Envelope($"raw/{Guid.NewGuid():N}.parquet");
        await store.RecordAsync(new DeadLetterArchiveRequest("dlq", Delivery(original.OriginalBody, id), original), default);

        var other = Envelope($"raw/{Guid.NewGuid():N}.parquet");
        await Assert.ThrowsAsync<DeadLetterIntegrityException>(() =>
            store.RecordAsync(new DeadLetterArchiveRequest("dlq", Delivery(other.OriginalBody, id), other), default));

        Assert.Equal(original.OriginalBody, await Sql.ScalarAsync<string>(db,
            "SELECT original_body FROM control.raw_fan_out_dead_letter WHERE message_id=@m", ("m", id)));
    }

    [ExternalIntegrationFact]
    public async Task ReplayRequestDoesNotResolveAndClosureNeedsAReasonAndStaysStable()
    {
        await using var db = await PostgresTests.OpenAsync();
        var store = new PostgresDeadLetterStore(new TestPostgresConnectionFactory(db));
        var artifact = Envelope($"raw/{Guid.NewGuid():N}.parquet");
        var incident = (await store.RecordAsync(new DeadLetterArchiveRequest("dlq", Delivery(artifact.OriginalBody), artifact), default)).IncidentId;

        await store.MarkReplayRequestedAsync(incident, default);
        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE incident_id=@i
              AND replay_requested_at_utc IS NOT NULL AND resolved_at_utc IS NULL
            """, ("i", incident)));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.ResolveAsync(new ResolveIncidentRequest(incident, "  ", "alice"), default));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.ResolveAsync(new ResolveIncidentRequest(incident, "done", " "), default));
        await store.ResolveAsync(new ResolveIncidentRequest(incident, "confirmed with owner", "alice"), default);
        var resolvedAt = await Sql.ScalarAsync<DateTime>(db,
            "SELECT resolved_at_utc FROM control.raw_fan_out_dead_letter WHERE incident_id=@i", ("i", incident));
        await store.ResolveAsync(new ResolveIncidentRequest(incident, "second closure", "bob"), default);

        Assert.Equal(1L, await Sql.ScalarAsync<long>(db, """
            SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE incident_id=@i
              AND resolution_reason='confirmed with owner' AND resolved_by='operator:alice' AND updated_by='operator:alice'
              AND resolved_at_utc=@t
            """, ("i", incident), ("t", resolvedAt)));
        await Assert.ThrowsAsync<KeyNotFoundException>(() =>
            store.ResolveAsync(new ResolveIncidentRequest(Guid.NewGuid(), "x", "alice"), default));
    }

    [ExternalIntegrationFact]
    public async Task DatabaseFailureWhileArchivingSurfacesAndLeavesNoRow()
    {
        await using var db = await PostgresTests.OpenAsync();
        var store = new PostgresDeadLetterStore(new TestPostgresConnectionFactory(db));
        var valid = Envelope($"raw/{Guid.NewGuid():N}.parquet");
        var invalid = valid with { SourceType = new string('x', 33) }; // violates varchar(32)
        var delivery = Delivery(valid.OriginalBody);

        await Assert.ThrowsAsync<PostgresException>(() =>
            store.RecordAsync(new DeadLetterArchiveRequest("dlq", delivery, invalid), default));

        Assert.Equal(0L, await Sql.ScalarAsync<long>(db, "SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE message_id=@m", ("m", delivery.MessageId)));
    }

    [ExternalIntegrationFact]
    public async Task ResolvedIncidentRetentionDeletesOnlyOldResolvedIncidentsInBoundedBatches()
    {
        await using var db = await PostgresTests.OpenAsync();
        var retention = new PostgresDeadLetterRetentionStore(new TestPostgresConnectionFactory(db));
        async Task Seed(string message, string? resolvedAt)
        {
            await Sql.ExecAsync(db, """
                INSERT INTO control.raw_fan_out_dead_letter(incident_id,queue_url,message_id,original_body,attributes,inserted_by,resolved_at_utc)
                VALUES (gen_random_uuid(),'q',@m,'x','{}','test',@r::text::timestamptz)
                """, ("m", message), ("r", (object?)resolvedAt ?? DBNull.Value));
        }
        await Seed("old-unresolved", null);
        await Seed("old-resolved-1", "2025-01-01T00:00:00Z");
        await Seed("old-resolved-2", "2025-01-02T00:00:00Z");
        await Seed("old-resolved-3", "2025-01-03T00:00:00Z");
        await Seed("recent-resolved", DateTime.UtcNow.AddDays(-1).ToString("O"));
        await Sql.ExecAsync(db, "UPDATE control.raw_fan_out_dead_letter SET first_observed_at_utc='2024-01-01T00:00:00Z', updated_by='test' WHERE message_id='old-unresolved'");
        var cutoff = DateTimeOffset.UtcNow.AddMonths(-6);

        Assert.Equal(2, await retention.DeleteResolvedBatchAsync(cutoff, 2, default));
        Assert.Equal(1, await retention.DeleteResolvedBatchAsync(cutoff, 2, default));
        Assert.Equal(0, await retention.DeleteResolvedBatchAsync(cutoff, 2, default));

        Assert.Equal(2L, await Sql.ScalarAsync<long>(db, "SELECT count(*) FROM control.raw_fan_out_dead_letter"));
        Assert.Equal(2L, await Sql.ScalarAsync<long>(db,
            "SELECT count(*) FROM control.raw_fan_out_dead_letter WHERE message_id IN ('old-unresolved','recent-resolved')"));
    }
}
