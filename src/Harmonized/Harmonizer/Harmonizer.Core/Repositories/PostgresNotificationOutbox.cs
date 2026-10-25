using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Npgsql;

namespace Harmonizer.Core.Repositories;

/// <summary>Reads and marks durable notification intents independently of source processing.</summary>
public sealed class PostgresNotificationOutbox(NpgsqlDataSource dataSource,
    ITelemetry<PostgresNotificationOutbox> telemetry) : INotificationOutbox
{
    public async Task<IReadOnlyList<Notification>> ReadPendingAsync(int limit, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("SELECT id,message_body,operation_id,execution_id FROM control.harmonizer_outbox WHERE sent_at_utc IS NULL ORDER BY id LIMIT @limit;");
        command.Parameters.AddWithValue("limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var items = new List<Notification>();
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new(reader.GetInt64(0), reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        if (items.Count > 0)
        {
            telemetry.Debug("Pending notification intents read. Count={Count}", items.Count);
        }

        return items;
    }

    public async Task MarkSentAsync(long id, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("UPDATE control.harmonizer_outbox SET sent_at_utc=clock_timestamp() WHERE id=@id AND sent_at_utc IS NULL;");
        command.Parameters.AddWithValue("id", id);
        var updated = await command.ExecuteNonQueryAsync(cancellationToken);
        telemetry.Debug("Notification send marker applied. OutboxId={OutboxId} Updated={Updated}", id, updated == 1);
    }
}
