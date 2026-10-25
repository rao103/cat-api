using System;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Interfaces;
using Npgsql;
using NpgsqlTypes;
namespace CAT.RawFanOut.Repositories;

public sealed class PostgresDeadLetterRetentionStore(IPostgresConnectionFactory connectionFactory) : IDeadLetterRetentionStore
{
    public async Task<int> DeleteResolvedBatchAsync(DateTimeOffset cutoffUtc, int maxIncidents, CancellationToken cancellationToken)
    {
        await using var connection = await connectionFactory.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            "SELECT control.raw_fan_out_dead_letter_cleanup_batch(@cutoff,@max)", connection);
        command.Parameters.AddWithValue("cutoff", NpgsqlDbType.TimestampTz, cutoffUtc.UtcDateTime);
        command.Parameters.AddWithValue("max", maxIncidents);
        return (int)(await command.ExecuteScalarAsync(cancellationToken))!;
    }
}
