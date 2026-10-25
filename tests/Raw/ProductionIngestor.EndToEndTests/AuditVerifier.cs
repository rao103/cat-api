using Npgsql;
using ProductionIngestor.Factories;

namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// Correlates the durable PostgreSQL state for one refresh run: a newly completed
/// <c>extraction_run</c> row matching the run-owned output prefix, a released lease, and an
/// incremental checkpoint that a refresh must not have advanced. Reuses the Worker's own
/// <see cref="PostgresConnectionFactory"/> so AWS IAM token authentication resolves identically.
/// </summary>
public sealed class AuditVerifier
{
    private readonly PostgresConnectionFactory _connectionFactory;

    public AuditVerifier(PostgresConnectionFactory connectionFactory) => _connectionFactory = connectionFactory;

    public async Task<CursorSnapshot?> ReadCursorSnapshotAsync(string windfarmId, string sourceType, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT last_committed_updated_at, last_committed_id, lease_owner, lease_expires_at
              FROM extraction_cursor
             WHERE windfarm_id = @windfarmId AND source_type = @sourceType;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("windfarmId", int.Parse(windfarmId));
        command.Parameters.AddWithValue("sourceType", sourceType);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new CursorSnapshot(
            reader.GetDateTime(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetDateTime(3));
    }

    /// <summary>Captures the current maximum <c>extraction_run.id</c> for this farm/source so a
    /// later lookup can require <c>id &gt; baseline</c> instead of comparing wall-clock timestamps,
    /// which are vulnerable to clock skew between the test host and the database server.</summary>
    public async Task<long> ReadMaxRunIdAsync(string windfarmId, string sourceType, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT COALESCE(MAX(id), 0)
              FROM extraction_run
             WHERE windfarm_id = @windfarmId AND source_type = @sourceType;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("windfarmId", int.Parse(windfarmId));
        command.Parameters.AddWithValue("sourceType", sourceType);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is long maxId ? maxId : 0;
    }

    /// <summary>Finds the most recent Completed run for this windfarm/source, newer than
    /// <paramref name="baselineRunId"/>, whose output path is prefixed by the run-owned prefix
    /// built from the observed extraction token.</summary>
    public async Task<CompletedRunRow?> FindCompletedRunAsync(
        string windfarmId, string sourceType, long extractionToken, long baselineRunId, CancellationToken cancellationToken)
    {
        var outputPrefix = ProductionIngestor.Raw.RawPathBuilder.BuildWindowExtractionPrefix(windfarmId, extractionToken);

        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        const string sql = """
            SELECT id, status, completed_at, output_path, from_updated_at, to_updated_at
              FROM extraction_run
             WHERE windfarm_id = @windfarmId
               AND source_type = @sourceType
               AND output_path LIKE @outputPrefix || '%'
               AND id > @baselineRunId
             ORDER BY id DESC
             LIMIT 1;
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("windfarmId", int.Parse(windfarmId));
        command.Parameters.AddWithValue("sourceType", sourceType);
        command.Parameters.AddWithValue("outputPrefix", outputPrefix);
        command.Parameters.AddWithValue("baselineRunId", baselineRunId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;

        return new CompletedRunRow(
            reader.GetInt64(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.GetDateTime(4),
            reader.IsDBNull(5) ? null : reader.GetDateTime(5));
    }
}

public sealed record CursorSnapshot(DateTime LastCommittedUpdatedUtc, string LastCommittedId, string? LeaseOwner, DateTime? LeaseExpiresAtUtc);

public sealed record CompletedRunRow(long RunId, string Status, DateTime? CompletedAtUtc, string? OutputPath, DateTime FromUpdatedAtUtc, DateTime? ToUpdatedAtUtc);
