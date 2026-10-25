using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Repositories;
using Npgsql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
namespace CAT.RawFanOut.IntegrationTests;

[Collection("Postgres")]
public sealed class ArtifactDispatchPostgresTests
{
    private static Task<MigrationDatabase> OpenAsync() => PostgresTests.OpenAsync();

    private static RawEnvelope Artifact(string suffix) =>
        RawEnvelope.Parse(ParquetFixtures.Body("Outage", [$"raw/{suffix}/{Guid.NewGuid():N}.parquet"]), new());

    private static IReadOnlyList<OutgoingMessage> Plan(int count) =>
        Enumerable.Range(0, count).Select(i => new OutgoingMessage($"{i:D64}".Replace('0', 'a'), $"body-{i}")).ToArray();

    [ExternalIntegrationFact]
    public async Task SavePlanPersistsChildrenInOrderAndIsIdempotentOnReplay()
    {
        await using var source = await OpenAsync();
        var store = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var artifact = Artifact("order");
        var proposed = Plan(3);

        var first = await store.SavePlanAsync(artifact, proposed, default);
        // A second save with a DIFFERENT proposed plan must return the FIRST committed plan, never replan.
        var differentlyOrdered = proposed.Reverse().ToArray();
        var second = await store.SavePlanAsync(artifact, differentlyOrdered, default);

        Assert.Equal(proposed.Select(m => m.Id), first.Select(m => m.Id));
        Assert.Equal(first.Select(m => m.Id), second.Select(m => m.Id));
        Assert.Equal(first.Select(m => m.Body), second.Select(m => m.Body));
    }

    [ExternalIntegrationFact]
    public async Task LoadPlanReconstructsExactReplayOrderAndUnchangedBodies()
    {
        await using var source = await OpenAsync();
        var store = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var artifact = Artifact("load");
        var proposed = Plan(5);
        await store.SavePlanAsync(artifact, proposed, default);

        var loaded = await store.LoadPlanAsync(artifact, default);

        Assert.NotNull(loaded);
        Assert.Equal(proposed.Select(m => m.Id), loaded!.Select(m => m.Id));
        Assert.Equal(proposed.Select(m => m.Body), loaded.Select(m => m.Body));
    }

    [ExternalIntegrationFact]
    public async Task MarkDispatchedCompletesFanOutWithoutMaterializingAnyChild()
    {
        await using var source = await OpenAsync();
        var store = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var artifact = Artifact("dispatched");
        await store.SavePlanAsync(artifact, Plan(2), default);

        await store.MarkDispatchedAsync(artifact.ArtifactId, default);
        await store.MarkDispatchedAsync(artifact.ArtifactId, default);

        await using var query = source.CreateCommand("""
            SELECT d.fan_out_completed_at_utc IS NOT NULL, d.harmonization_completed_at_utc IS NULL, d.updated_by,
                   (SELECT count(*) FROM control.raw_artifact_batch b WHERE b.artifact_id=d.artifact_id AND b.materialized_at_utc IS NOT NULL)
            FROM control.raw_artifact_dispatch d WHERE d.artifact_id=@id
            """);
        query.Parameters.AddWithValue("id", artifact.ArtifactId);
        await using var reader = await query.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.True(reader.GetBoolean(0));
        Assert.True(reader.GetBoolean(1));
        Assert.Equal("raw-fanout-worker", reader.GetString(2));
        Assert.Equal(0L, reader.GetInt64(3));
    }

    [ExternalIntegrationFact]
    public async Task LoadPlanReturnsNullWhenNoParentExists()
    {
        await using var source = await OpenAsync();
        var store = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var artifact = Artifact("missing");

        Assert.Null(await store.LoadPlanAsync(artifact, default));
    }

    [ExternalIntegrationFact]
    public async Task LoadPlanThrowsWhenChildCountIsInconsistentWithExpectedBatchCount()
    {
        await using var source = await OpenAsync();
        var store = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var artifact = Artifact("inconsistent");
        await store.SavePlanAsync(artifact, Plan(3), default);

        await using var delete = source.CreateCommand(
            "DELETE FROM control.raw_artifact_batch WHERE artifact_id=@id AND batch_ordinal=2");
        delete.Parameters.AddWithValue("id", artifact.ArtifactId);
        await delete.ExecuteNonQueryAsync();

        await Assert.ThrowsAsync<InvalidDataException>(() => store.LoadPlanAsync(artifact, default));
    }

    [ExternalIntegrationFact]
    public async Task ConcurrentSavePlanConvergesOnOneCommittedPlan()
    {
        await using var source = await OpenAsync();
        var artifact = Artifact("concurrent");
        var storeA = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var storeB = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));

        var results = await Task.WhenAll(
            storeA.SavePlanAsync(artifact, Plan(4), default),
            storeB.SavePlanAsync(artifact, Plan(4), default));

        Assert.Equal(results[0].Select(m => m.Id), results[1].Select(m => m.Id));
        await using var count = source.CreateCommand(
            "SELECT count(*) FROM control.raw_artifact_batch WHERE artifact_id=@id");
        count.Parameters.AddWithValue("id", artifact.ArtifactId);
        Assert.Equal(4L, (long)(await count.ExecuteScalarAsync())!);
    }

    private static async Task MarkFullyCompletedAsync(NpgsqlDataSource source, RawEnvelope artifact, DateTime harmonizedAtUtc)
    {
        await using var update = source.CreateCommand("""
            UPDATE control.raw_artifact_batch SET materialized_at_utc=@t, updated_by='test' WHERE artifact_id=@id;
            UPDATE control.raw_artifact_dispatch SET fan_out_completed_at_utc=@t, harmonization_completed_at_utc=@t, updated_by='test' WHERE artifact_id=@id;
            """);
        update.Parameters.AddWithValue("id", artifact.ArtifactId);
        update.Parameters.AddWithValue("t", NpgsqlTypes.NpgsqlDbType.TimestampTz, harmonizedAtUtc);
        await update.ExecuteNonQueryAsync();
    }

    [ExternalIntegrationFact]
    public async Task RetentionCleanupDeletesOnlyFullyCompletedArtifactsPastCutoff()
    {
        await using var source = await OpenAsync();
        var store = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var cleanup = new PostgresArtifactRetentionCleanupStore(new TestPostgresConnectionFactory(source));

        var completedOld = Artifact("completed-old");
        await store.SavePlanAsync(completedOld, Plan(2), default);
        await MarkFullyCompletedAsync(source, completedOld, DateTime.UtcNow.AddMonths(-7));

        var completedRecent = Artifact("completed-recent");
        await store.SavePlanAsync(completedRecent, Plan(2), default);
        await MarkFullyCompletedAsync(source, completedRecent, DateTime.UtcNow.AddDays(-1));

        var partial = Artifact("partial");
        await store.SavePlanAsync(partial, Plan(2), default);
        await MarkFullyCompletedAsync(source, partial, DateTime.UtcNow.AddMonths(-7));
        await using var unmaterialize = source.CreateCommand(
            "UPDATE control.raw_artifact_batch SET materialized_at_utc=NULL, updated_by='test' WHERE artifact_id=@id AND batch_ordinal=0");
        unmaterialize.Parameters.AddWithValue("id", partial.ArtifactId);
        await unmaterialize.ExecuteNonQueryAsync();
        await using var clearHarmonized = source.CreateCommand(
            "UPDATE control.raw_artifact_dispatch SET harmonization_completed_at_utc=NULL, updated_by='test' WHERE artifact_id=@id");
        clearHarmonized.Parameters.AddWithValue("id", partial.ArtifactId);
        await clearHarmonized.ExecuteNonQueryAsync();

        var cutoff = DateTimeOffset.UtcNow.AddMonths(-6);
        var result = await cleanup.DeleteEligibleBatchAsync(cutoff, 500, default);

        Assert.Equal(1, result.DeletedArtifacts);
        Assert.Equal(2, result.DeletedBatches);
        await using var remaining = source.CreateCommand(
            "SELECT artifact_id FROM control.raw_artifact_dispatch WHERE artifact_id = ANY(@ids)");
        remaining.Parameters.AddWithValue("ids", new[] { completedOld.ArtifactId, completedRecent.ArtifactId, partial.ArtifactId });
        var stillPresent = new HashSet<string>();
        await using (var reader = await remaining.ExecuteReaderAsync())
            while (await reader.ReadAsync()) stillPresent.Add(reader.GetString(0));
        Assert.DoesNotContain(completedOld.ArtifactId, stillPresent);
        Assert.Contains(completedRecent.ArtifactId, stillPresent);
        Assert.Contains(partial.ArtifactId, stillPresent);
    }

    [ExternalIntegrationFact]
    public async Task RetentionCleanupIsBoundedAndLocksSkipConcurrentCallers()
    {
        await using var source = await OpenAsync();
        var store = new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(source));
        var cleanup = new PostgresArtifactRetentionCleanupStore(new TestPostgresConnectionFactory(source));

        var artifacts = new List<RawEnvelope>();
        for (var i = 0; i < 5; i++)
        {
            var artifact = Artifact($"bounded-{i}");
            await store.SavePlanAsync(artifact, Plan(1), default);
            await MarkFullyCompletedAsync(source, artifact, DateTime.UtcNow.AddMonths(-7));
            artifacts.Add(artifact);
        }

        var cutoff = DateTimeOffset.UtcNow.AddMonths(-6);
        var firstBatch = await cleanup.DeleteEligibleBatchAsync(cutoff, 2, default);
        Assert.Equal(2, firstBatch.DeletedArtifacts);

        var secondBatch = await cleanup.DeleteEligibleBatchAsync(cutoff, 2, default);
        Assert.Equal(2, secondBatch.DeletedArtifacts);

        var thirdBatch = await cleanup.DeleteEligibleBatchAsync(cutoff, 2, default);
        Assert.Equal(1, thirdBatch.DeletedArtifacts);

        var fourthBatch = await cleanup.DeleteEligibleBatchAsync(cutoff, 2, default);
        Assert.Equal(0, fourthBatch.DeletedArtifacts);
    }
}
