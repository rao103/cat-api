using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using Npgsql;
namespace CAT.RawFanOut.Repositories;

public sealed class PostgresArtifactRetentionCleanupStore : IArtifactRetentionCleanupStore
{
    private readonly IPostgresConnectionFactory connectionFactory;

    public PostgresArtifactRetentionCleanupStore(IPostgresConnectionFactory connectionFactory)
    {
        this.connectionFactory = connectionFactory;
    }

    public async Task<RetentionCleanupBatchResult> DeleteEligibleBatchAsync(
        System.DateTimeOffset cutoffUtc, int maxParentsPerBatch, CancellationToken cancellationToken)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT deleted_artifacts, deleted_batches FROM control.raw_artifact_cleanup_batch(@cutoff,@maxParents)";
        command.Parameters.AddWithValue("cutoff", NpgsqlTypes.NpgsqlDbType.TimestampTz, cutoffUtc.UtcDateTime);
        command.Parameters.AddWithValue("maxParents", maxParentsPerBatch);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new RetentionCleanupBatchResult(reader.GetInt32(0), reader.GetInt32(1));
    }
}
