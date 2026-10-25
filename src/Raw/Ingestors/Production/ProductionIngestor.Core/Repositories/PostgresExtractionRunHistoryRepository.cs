using Npgsql;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.Repositories;

public sealed class PostgresExtractionRunHistoryRepository(string connectionString) : IExtractionRunHistoryRepository
{
    public async Task<IReadOnlyList<ExtractionRunHistoryModel>> GetAttemptsAsync(
        ExtractionHistoryQueryModel query, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            SELECT run.id, run.request_id, run.windfarm_id, run.execution_mode, run.status,
                run.started_at, run.completed_at, run.job_name, run.error_message,
                run.requested_dates, cursor.lease_expires_at
            FROM extraction_run run LEFT JOIN extraction_cursor cursor ON cursor.active_run_id = run.id
                AND cursor.windfarm_id = run.windfarm_id AND cursor.source_type = run.source_type
            WHERE run.request_id = @requestId AND run.source_type = 'Production' AND run.execution_mode = 'Refresh'
            ORDER BY run.id DESC LIMIT 100;
            """, connection);
        command.Parameters.AddWithValue("requestId", query.RequestId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var attempts = new List<ExtractionRunHistoryModel>();
        while (await reader.ReadAsync(cancellationToken))
        {
            attempts.Add(new(reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2).ToString(System.Globalization.CultureInfo.InvariantCulture),
                reader.GetString(3), reader.GetString(4), new(reader.GetDateTime(5)),
                reader.IsDBNull(6) ? null : new DateTimeOffset(reader.GetDateTime(6)),
                reader.IsDBNull(7) ? null : reader.GetString(7), reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? [] : reader.GetFieldValue<DateOnly[]>(9),
                reader.IsDBNull(10) ? null : new DateTimeOffset(reader.GetDateTime(10))));
        }
        return attempts;
    }
}