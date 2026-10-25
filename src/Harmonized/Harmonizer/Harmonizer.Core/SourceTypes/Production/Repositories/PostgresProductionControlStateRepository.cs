using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Harmonizer.Core.SourceTypes.Production.Models;
using Harmonizer.Core.SourceTypes.Production.Operations;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
namespace Harmonizer.Core.SourceTypes.Production.Repositories;

public sealed class PostgresProductionControlStateRepository(NpgsqlDataSource dataSource, IOptions<HarmonizerOptions> options,
    ITelemetry<PostgresProductionControlStateRepository> telemetry, ProductionNotificationFactory notifications)
    : IProductionControlStateRepository
{
    public async Task RegisterBatchAsync(ProductionWork work, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var members = work.Turbines.Select(turbine => turbine.WtgId).Order().ToArray();
        await using var existing = new NpgsqlCommand("""
            SELECT message_body::jsonb = @body::jsonb AND expected_wtgids = @members
            FROM control.harmonizer_batch
            WHERE artifact_id=@artifact AND batch_id=@batch;
            """, connection, transaction);
        Batch(existing, work.Batch);
        existing.Parameters.AddWithValue("body", work.WireBody);
        existing.Parameters.AddWithValue("members", members);
        var alreadyRegistered = await existing.ExecuteScalarAsync(cancellationToken);
        if (alreadyRegistered is bool matches)
        {
            if (!matches)
            {
                throw new WorkValidationException("A registered batch body or resolved turbine membership changed.");
            }

            await using var correlate = new NpgsqlCommand("""
                UPDATE control.harmonizer_batch
                SET operation_id=coalesce(operation_id,@operation),execution_id=coalesce(execution_id,@execution)
                WHERE artifact_id=@artifact AND batch_id=@batch
                  AND (operation_id IS NULL OR operation_id IS NOT DISTINCT FROM @operation)
                  AND (execution_id IS NULL OR execution_id IS NOT DISTINCT FROM @execution);
                """, connection, transaction);
            Batch(correlate, work.Batch);
            Operation(correlate, work.Operation);
            if (await correlate.ExecuteNonQueryAsync(cancellationToken) != 1)
            {
                throw new WorkValidationException("A registered batch operation identity changed.");
            }

            await transaction.CommitAsync(cancellationToken);
            telemetry.Debug("Production batch registration replayed. BatchId={BatchId}", work.Batch.BatchId);
            return;
        }
        await using var command = new NpgsqlCommand("""
            INSERT INTO control.harmonizer_batch(artifact_id,batch_id,expected_wtgids,message_body,operation_id,execution_id)
            SELECT artifact_id,batch_id,@members,@body,@operation,@execution FROM control.raw_artifact_batch
            WHERE artifact_id=@artifact AND batch_id=@batch AND message_body::jsonb=@body::jsonb
            ON CONFLICT(artifact_id,batch_id) DO UPDATE
            SET message_body=control.harmonizer_batch.message_body,
                operation_id=coalesce(control.harmonizer_batch.operation_id,excluded.operation_id),
                execution_id=coalesce(control.harmonizer_batch.execution_id,excluded.execution_id)
            WHERE control.harmonizer_batch.message_body::jsonb=excluded.message_body::jsonb
                AND control.harmonizer_batch.expected_wtgids=excluded.expected_wtgids
                AND (control.harmonizer_batch.operation_id IS NULL OR control.harmonizer_batch.operation_id IS NOT DISTINCT FROM excluded.operation_id)
                AND (control.harmonizer_batch.execution_id IS NULL OR control.harmonizer_batch.execution_id IS NOT DISTINCT FROM excluded.execution_id);
            """, connection, transaction);
        Batch(command, work.Batch);
        command.Parameters.AddWithValue("members", members);
        command.Parameters.AddWithValue("body", work.WireBody);
        Operation(command, work.Operation);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            throw new WorkValidationException("Unknown Fan-Out batch or body differs from its frozen dispatch plan.");
        }

        await transaction.CommitAsync(cancellationToken);
        telemetry.Debug("Production batch registered. BatchId={BatchId} TurbineCount={TurbineCount}",
            work.Batch.BatchId, work.Turbines.Count);
    }

    public async Task<bool> IsItemAppliedAsync(BatchIdentity batch, int wtgId, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT EXISTS(SELECT 1 FROM control.harmonizer_batch_item
            WHERE artifact_id=@artifact AND batch_id=@batch AND wtgid=@wtgid);
            """);
        Batch(command, batch);
        command.Parameters.AddWithValue("wtgid", wtgId);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken) ?? false);
    }

    public async Task<ComponentLease> ClaimAsync(WtgDayKey key, BatchIdentity batch, Guid ownerId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var insert = new NpgsqlCommand("""
            INSERT INTO control.wtg_day_input_state(windfarm_id,wtgid,data_date,input_type,inserted_by)
            VALUES(@farm,@wtgid,@date,@type,'harmonizer') ON CONFLICT DO NOTHING;
            """, connection, transaction);
        Key(insert, key);
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            UPDATE control.wtg_day_input_state SET lease_token=lease_token+1,lease_owner=@owner,
                lease_expires_at_utc=clock_timestamp()+make_interval(secs=>@seconds),updated_by='harmonizer'
            WHERE windfarm_id=@farm AND wtgid=@wtgid AND data_date=@date AND input_type=@type
              AND (lease_expires_at_utc IS NULL OR lease_expires_at_utc<=clock_timestamp())
              AND (first_failed_batch_id IS NULL OR (first_failed_batch_id=@batch AND first_failed_artifact_id=@artifact))
            RETURNING lease_token,component_revision,canonical_hash,snapshot_path,parquet_path,manifest_path,status;
            """, connection, transaction);
        Key(command, key);
        Batch(command, batch);
        command.Parameters.AddWithValue("owner", ownerId);
        command.Parameters.AddWithValue("seconds", options.Value.LeaseSeconds);
        ComponentLease lease;
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new LeaseUnavailableException("Component is leased or pinned to an earlier unresolved batch.");
            }

            lease = new(key, batch, reader.GetInt64(0), reader.GetInt64(1), Text(reader, 2), Text(reader, 3),
                Text(reader, 4), Text(reader, 5), reader.GetString(6), ownerId);
        }
        await transaction.CommitAsync(cancellationToken);
        telemetry.Debug("Production component lease claimed. WindfarmId={WindfarmId} WtgId={WtgId} DataDate={DataDate} FenceToken={FenceToken}",
            key.WindfarmId, key.WtgId, key.DataDate, lease.FenceToken);
        return lease;
    }

    public async Task RenewAsync(ComponentLease lease, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE control.wtg_day_input_state SET lease_expires_at_utc=clock_timestamp()+make_interval(secs=>@seconds),updated_by='harmonizer'
            WHERE windfarm_id=@farm AND wtgid=@wtgid AND data_date=@date AND input_type=@type
              AND lease_token=@token AND lease_owner=@owner AND lease_expires_at_utc>clock_timestamp();
            """);
        Fence(command, lease);
        command.Parameters.AddWithValue("seconds", options.Value.LeaseSeconds);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            telemetry.Warning("Production lease renewal rejected. WindfarmId={WindfarmId} WtgId={WtgId} DataDate={DataDate} FenceToken={FenceToken}",
                lease.Key.WindfarmId, lease.Key.WtgId, lease.Key.DataDate, lease.FenceToken);
            throw new LeaseLostException("Component lease renewal rejected.");
        }
    }

    public async Task CommitAsync(ComponentCommit commit, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        // Serialize receipt/completion decisions for this batch, before touching its individual state row.
        await using var batchLock = new NpgsqlCommand("""
            SELECT expected_wtgids FROM control.harmonizer_batch
            WHERE artifact_id=@artifact AND batch_id=@batch
              AND operation_id IS NOT DISTINCT FROM @operation
              AND execution_id IS NOT DISTINCT FROM @execution FOR UPDATE;
            """, connection, transaction);
        Batch(batchLock, commit.Lease.Batch);
        Operation(batchLock, commit.Operation);
        var members = (int[]?)await batchLock.ExecuteScalarAsync(cancellationToken)
            ?? throw new WorkValidationException("Batch is not registered with this operation identity.");
        if (!members.Contains(commit.Lease.Key.WtgId))
        {
            throw new WorkValidationException("WTG is outside the frozen batch.");
        }

        await using var state = new NpgsqlCommand("""
            UPDATE control.wtg_day_input_state SET component_revision=@revision,
                committed_attempt=CASE WHEN @changed THEN @token ELSE committed_attempt END,
                canonical_hash=@hash,record_count=@count,parquet_path=@parquet,snapshot_path=@snapshot,manifest_path=@manifest,
                status=@status,lease_owner=NULL,lease_expires_at_utc=NULL,
                first_failed_artifact_id=NULL,first_failed_batch_id=NULL,first_failed_raw_paths=NULL,last_error_code=NULL,
                last_operation_id=@operation,last_execution_id=@execution,updated_by='harmonizer'
            WHERE windfarm_id=@farm AND wtgid=@wtgid AND data_date=@date AND input_type=@type
              AND lease_token=@token AND lease_owner=@owner AND lease_expires_at_utc>clock_timestamp()
              AND component_revision=@previous_revision;
            """, connection, transaction);
        Fence(state, commit.Lease);
        Operation(state, commit.Operation);
        state.Parameters.AddWithValue("revision", commit.Revision);
        state.Parameters.AddWithValue("previous_revision", commit.Lease.Revision);
        state.Parameters.AddWithValue("changed", commit.Changed);
        state.Parameters.AddWithValue("hash", commit.CanonicalHash);
        state.Parameters.AddWithValue("count", commit.RecordCount);
        NullableText(state, "parquet", commit.Candidate.ParquetPath);
        state.Parameters.AddWithValue("snapshot", commit.Candidate.SnapshotPath);
        state.Parameters.AddWithValue("manifest", commit.Candidate.ManifestPath);
        state.Parameters.AddWithValue("status", commit.Complete ? "Complete" : "Partial");
        if (await state.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            throw new LeaseLostException("Fenced component commit rejected.");
        }

        foreach (var notification in notifications.Create(commit))
        {
            await using var outbox = new NpgsqlCommand("""
                INSERT INTO control.harmonizer_outbox(deduplication_key,message_body,operation_id,execution_id)
                VALUES(@id,@body,@operation,@execution) ON CONFLICT DO NOTHING;
                """, connection, transaction);
            outbox.Parameters.AddWithValue("id", notification.DeduplicationKey);
            outbox.Parameters.AddWithValue("body", notification.Body);
            Operation(outbox, notification.Operation);
            await outbox.ExecuteNonQueryAsync(cancellationToken);
        }
        await using var receipt = new NpgsqlCommand("""
            INSERT INTO control.harmonizer_batch_item(artifact_id,batch_id,wtgid,component_revision)
            VALUES(@artifact,@batch,@wtgid,@revision) ON CONFLICT DO NOTHING;
            """, connection, transaction);
        Batch(receipt, commit.Lease.Batch);
        receipt.Parameters.AddWithValue("wtgid", commit.Lease.Key.WtgId);
        receipt.Parameters.AddWithValue("revision", commit.Revision);
        await receipt.ExecuteNonQueryAsync(cancellationToken);
        await using var complete = new NpgsqlCommand("""
            UPDATE control.harmonizer_batch b SET completed_at_utc=clock_timestamp()
            WHERE artifact_id=@artifact AND batch_id=@batch AND completed_at_utc IS NULL
              AND NOT EXISTS(SELECT 1 FROM unnest(b.expected_wtgids) AS expected(wtgid)
                  WHERE NOT EXISTS(SELECT 1 FROM control.harmonizer_batch_item i
                      WHERE i.artifact_id=b.artifact_id AND i.batch_id=b.batch_id AND i.wtgid=expected.wtgid));
            """, connection, transaction);
        Batch(complete, commit.Lease.Batch);
        var batchCompleted = await complete.ExecuteNonQueryAsync(cancellationToken) == 1;
        if (batchCompleted)
        {
            await using var acknowledge = new NpgsqlCommand("SELECT control.raw_artifact_ack(@artifact,@batch,'harmonizer');", connection, transaction);
            Batch(acknowledge, commit.Lease.Batch);
            await acknowledge.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        telemetry.Information("Production component committed with receipt and outbox. WindfarmId={WindfarmId} WtgId={WtgId} DataDate={DataDate} Revision={Revision} BatchCompleted={BatchCompleted}",
            commit.Lease.Key.WindfarmId, commit.Lease.Key.WtgId, commit.Lease.Key.DataDate, commit.Revision, batchCompleted);
    }

    public async Task FailAsync(ComponentLease lease, ProductionWork work, string errorCode, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE control.wtg_day_input_state SET status='Failed',lease_owner=NULL,lease_expires_at_utc=NULL,
                first_failed_artifact_id=coalesce(first_failed_artifact_id,@artifact),
                first_failed_batch_id=coalesce(first_failed_batch_id,@batch),
                first_failed_raw_paths=coalesce(first_failed_raw_paths,@paths),last_error_code=@error,
                last_operation_id=@operation,last_execution_id=@execution,updated_by='harmonizer'
            WHERE windfarm_id=@farm AND wtgid=@wtgid AND data_date=@date AND input_type=@type
              AND lease_token=@token AND lease_owner=@owner AND lease_expires_at_utc>clock_timestamp();
            """);
        Fence(command, lease);
        Batch(command, work.Batch);
        Operation(command, work.Operation);
        command.Parameters.AddWithValue("paths", work.RawPaths.ToArray());
        command.Parameters.AddWithValue("error", errorCode);
        var updated = await command.ExecuteNonQueryAsync(cancellationToken);
        if (updated == 1)
        {
            telemetry.Warning("Production component marked failed. WindfarmId={WindfarmId} WtgId={WtgId} DataDate={DataDate} ErrorCode={ErrorCode}",
                lease.Key.WindfarmId, lease.Key.WtgId, lease.Key.DataDate, errorCode);
        }
        else
        {
            telemetry.Debug("Stale failure marker ignored by lease fence. WindfarmId={WindfarmId} WtgId={WtgId} FenceToken={FenceToken}",
                lease.Key.WindfarmId, lease.Key.WtgId, lease.FenceToken);
        }
    }

    private static string? Text(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    private static void NullableText(NpgsqlCommand command, string name, string? value) =>
        command.Parameters.Add(name, NpgsqlDbType.Text).Value = (object?)value ?? DBNull.Value;
    private static void Operation(NpgsqlCommand command, OperationIdentity operation)
    {
        NullableText(command, "operation", operation.OperationId);
        NullableText(command, "execution", operation.ExecutionId);
    }

    private static void Key(NpgsqlCommand command, WtgDayKey key)
    {
        command.Parameters.AddWithValue("farm", key.WindfarmId);
        command.Parameters.AddWithValue("wtgid", key.WtgId);
        command.Parameters.AddWithValue("date", key.DataDate);
        command.Parameters.AddWithValue("type", key.InputType);
    }
    private static void Batch(NpgsqlCommand command, BatchIdentity batch)
    {
        command.Parameters.AddWithValue("artifact", batch.ArtifactId);
        command.Parameters.AddWithValue("batch", batch.BatchId);
    }
    private static void Fence(NpgsqlCommand command, ComponentLease lease)
    {
        Key(command, lease.Key);
        command.Parameters.AddWithValue("token", lease.FenceToken);
        command.Parameters.AddWithValue("owner", lease.OwnerId);
    }
}
