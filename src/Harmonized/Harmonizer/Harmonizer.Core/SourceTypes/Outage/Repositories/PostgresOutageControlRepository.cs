using Harmonizer.Core.SourceTypes.Outage.Serialization;
using System.Text.Json;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace Harmonizer.Core.SourceTypes.Outage.Repositories;

/// <summary>One farm/source publication lease guards canonical pointers and daily progress across pods.</summary>
public sealed class PostgresOutageControlRepository(NpgsqlDataSource dataSource,
    IOptions<HarmonizerOptions> options, ITelemetry<PostgresOutageControlRepository> telemetry) : IOutageControlRepository
{
    public async Task<bool> RegisterAsync(OutageWork work, CancellationToken cancellationToken)
    {
        var members = work.IsReconciliation || work.IsObservation ? new[] { OutageWorkKeys.Scope(work) } : work.Message.Members.Select(member => member.MemberKey).Order(StringComparer.Ordinal).ToArray();
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var insert = Command(connection, transaction, """
            INSERT INTO control.harmonizer_batch(artifact_id,batch_id,expected_wtgids,message_body,
                work_kind,windfarm_id,source_type,expected_member_keys,expected_event_batch_ids,operation_id,execution_id)
            SELECT artifact_id,batch_id,'{}',@body,@kind,@farm,'Outage',@members,@expected,@operation,@execution
            FROM control.raw_artifact_batch WHERE artifact_id=@artifact AND batch_id=@batch AND message_body::jsonb=@body::jsonb
            ON CONFLICT(artifact_id,batch_id) DO NOTHING;
            """, Batch(work.Batch).Concat([
                ("body", (object?)work.WireBody), ("kind", work.Message.WorkKind), ("farm", work.Windfarm.WindfarmId),
                ("members", members), ("expected", work.Message.ExpectedEventBatchIds),
                ("operation", work.Operation.OperationId), ("execution", work.Operation.ExecutionId)]).ToArray());
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await using var check = Command(connection, transaction, """
            SELECT message_body::jsonb=@body::jsonb AND expected_member_keys=@members
                AND expected_event_batch_ids=@expected AND source_type='Outage' AND windfarm_id=@farm
                AND operation_id IS NOT DISTINCT FROM @operation AND execution_id IS NOT DISTINCT FROM @execution AS matches_frozen_work,
                completed_at_utc IS NOT NULL AS already_completed
            FROM control.harmonizer_batch WHERE artifact_id=@artifact AND batch_id=@batch FOR UPDATE;
            """, Batch(work.Batch).Concat([
                ("body", (object?)work.WireBody), ("members", members), ("expected", work.Message.ExpectedEventBatchIds),
                ("farm", work.Windfarm.WindfarmId), ("operation", work.Operation.OperationId), ("execution", work.Operation.ExecutionId)]).ToArray());
        bool complete;
        await using (var reader = await check.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken) || !reader.GetBoolean(0))
                throw new WorkValidationException("Event batch is unknown or differs from its frozen Fan-Out plan/correlation.");
            complete = reader.GetBoolean(1);
        }
        await transaction.CommitAsync(cancellationToken);
        telemetry.Debug("Outage batch registered. BatchId={BatchId} Completed={Completed}", work.Message.BatchId, complete);
        return complete;
    }

    public async Task RecordSourceFailureAsync(OutageWork work, string errorCode, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE control.harmonizer_batch SET last_error_code=@error
            WHERE artifact_id=@artifact AND batch_id=@batch AND source_type='Outage' AND message_body::jsonb=@body::jsonb
              AND completed_at_utc IS NULL;
            """);
        Add(command, Batch(work.Batch).Concat([("error", (object?)errorCode), ("body", work.WireBody)]).ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RecordObservationAsync(OutageWork work, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = Command(connection, transaction, """
            INSERT INTO control.harmonizer_batch_item(artifact_id,batch_id,component_revision,member_key,member_kind,materialized_at_utc)
            VALUES(@artifact,@batch,0,@member,'ArtifactObservation',clock_timestamp()) ON CONFLICT DO NOTHING;
            UPDATE control.harmonizer_batch SET canonical_applied_at_utc=coalesce(canonical_applied_at_utc,clock_timestamp()),
                completed_at_utc=coalesce(completed_at_utc,clock_timestamp()),last_error_code=NULL WHERE artifact_id=@artifact AND batch_id=@batch;
            SELECT control.raw_artifact_ack(@artifact,@batch,'harmonizer');
            """, Batch(work.Batch).Concat([("member", (object?)OutageWorkKeys.Scope(work))]).ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        telemetry.Information("Empty incremental Outage artifact acknowledged without changing canonical state. BatchId={BatchId}", work.Batch.BatchId);
    }

    public async Task<bool> HasCanonicalReceiptAsync(BatchIdentity batch, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT EXISTS(SELECT 1 FROM control.harmonizer_batch
                WHERE artifact_id=@artifact AND batch_id=@batch AND canonical_applied_at_utc IS NOT NULL);
            """);
        Add(command, Batch(batch));
        return (bool)(await command.ExecuteScalarAsync(cancellationToken) ?? false);
    }

    public async Task<bool> HasExpectedReceiptsAsync(OutageWork work, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT NOT EXISTS(SELECT 1 FROM unnest(@expected) AS e(batch_id)
                WHERE NOT EXISTS(SELECT 1 FROM control.harmonizer_batch b
                    WHERE b.artifact_id=@artifact AND b.batch_id=e.batch_id AND b.windfarm_id=@farm
                      AND b.work_kind='CanonicalEvents' AND b.source_type='Outage' AND b.canonical_applied_at_utc IS NOT NULL));
            """);
        Add(command, ("expected", work.Message.ExpectedEventBatchIds), ("artifact", work.Message.ArtifactId), ("farm", work.Windfarm.WindfarmId));
        return (bool)(await command.ExecuteScalarAsync(cancellationToken) ?? false);
    }

    public async Task<OutageCatalogLease> ClaimAsync(WindfarmIdentity farm, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var insert = Command(connection, transaction, """
            INSERT INTO control.event_partition_state(windfarm_id,windfarm_name,source_type,partition_key,inserted_by)
            VALUES(@farm,@name,'Outage','_catalog','harmonizer') ON CONFLICT DO NOTHING;
            """, ("farm", farm.WindfarmId), ("name", farm.WindfarmName));
        await insert.ExecuteNonQueryAsync(cancellationToken);
        var owner = Guid.NewGuid();
        await using var claim = Command(connection, transaction, """
            UPDATE control.event_partition_state SET lease_token=lease_token+1,lease_owner=@owner,
                lease_expires_at_utc=clock_timestamp()+make_interval(secs=>@seconds),updated_by='harmonizer'
            WHERE windfarm_id=@farm AND windfarm_name=@name AND source_type='Outage' AND partition_key='_catalog'
              AND (lease_expires_at_utc IS NULL OR lease_expires_at_utc<=clock_timestamp())
            RETURNING lease_token,revision,snapshot_path;
            """, ("farm", farm.WindfarmId), ("name", farm.WindfarmName), ("owner", owner), ("seconds", options.Value.LeaseSeconds));
        OutageCatalogLease lease;
        await using (var reader = await claim.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken)) throw new LeaseUnavailableException("Outage farm/source publication lease is busy.");
            lease = new(farm, reader.GetInt64(0), owner, reader.GetInt64(1), Text(reader, 2));
        }
        await transaction.CommitAsync(cancellationToken);
        telemetry.Debug("Outage catalog lease claimed. WindfarmId={WindfarmId} FenceToken={FenceToken}", farm.WindfarmId, lease.Token);
        return lease;
    }

    public async Task RenewAsync(OutageCatalogLease lease, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE control.event_partition_state SET lease_expires_at_utc=clock_timestamp()+make_interval(secs=>@seconds),updated_by='harmonizer'
            WHERE windfarm_id=@farm AND source_type='Outage' AND partition_key='_catalog'
              AND lease_token=@token AND lease_owner=@owner AND lease_expires_at_utc>clock_timestamp();
            """);
        Add(command, Fence(lease).Concat([("seconds", (object?)options.Value.LeaseSeconds)]).ToArray());
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1) throw new LeaseLostException("Outage catalog lease renewal rejected.");
    }

    public async Task ReleaseAsync(OutageCatalogLease lease, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE control.event_partition_state SET lease_owner=NULL,lease_expires_at_utc=NULL,updated_by='harmonizer'
            WHERE windfarm_id=@farm AND source_type='Outage' AND partition_key='_catalog' AND lease_token=@token AND lease_owner=@owner;
            """);
        Add(command, Fence(lease));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<WindfarmIdentity>> GetDueFarmsAsync(int limit, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT p.windfarm_id,p.windfarm_name FROM control.event_partition_state p
            WHERE p.source_type='Outage' AND p.partition_key='_catalog'
              AND (p.lease_expires_at_utc IS NULL OR p.lease_expires_at_utc<=clock_timestamp())
              AND (EXISTS(SELECT 1 FROM control.event_materialization_state s WHERE s.windfarm_id=p.windfarm_id
                AND s.source_type=p.source_type AND s.due_at_utc<=clock_timestamp() AND (s.has_pending OR s.needs_continuation))
                OR EXISTS(SELECT 1 FROM control.harmonizer_batch b WHERE b.windfarm_id=p.windfarm_id
                   AND b.source_type='Outage' AND b.canonical_applied_at_utc IS NOT NULL AND b.completed_at_utc IS NULL
                   AND NOT EXISTS(SELECT 1 FROM unnest(b.expected_member_keys) e(member_key)
                     WHERE NOT EXISTS(SELECT 1 FROM control.harmonizer_batch_item i WHERE i.artifact_id=b.artifact_id
                       AND i.batch_id=b.batch_id AND i.member_key=e.member_key AND i.materialized_at_utc IS NOT NULL))))
            ORDER BY p.updated_date_utc,p.windfarm_id LIMIT @limit;
            """);
        Add(command, ("limit", limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<WindfarmIdentity>();
        while (await reader.ReadAsync(cancellationToken)) result.Add(new(reader.GetInt32(0), reader.GetString(1)));
        return result;
    }

    public async Task<IReadOnlyList<OutageLifecycleState>> ReadLifecycleAsync(int farmId, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT work_id,event_id,partition_key,event_revision,plan_path,needs_continuation,continued_through,due_at_utc
            FROM control.event_materialization_state WHERE windfarm_id=@farm AND source_type='Outage' ORDER BY due_at_utc,work_id LIMIT @maximum;
            """);
        Add(command, ("farm", farmId), ("maximum", options.Value.MaximumRowsPerArtifact + 1));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<OutageLifecycleState>();
        while (await reader.ReadAsync(cancellationToken)) result.Add(new(reader.GetString(0), Text(reader, 1), Text(reader, 2),
            reader.GetInt64(3), reader.GetString(4), reader.GetBoolean(5), reader.IsDBNull(6) ? null : reader.GetFieldValue<DateOnly>(6), reader.GetDateTime(7)));
        if (result.Count > options.Value.MaximumRowsPerArtifact)
            throw new WorkValidationException("Outage lifecycle state exceeds the configured farm bound.");
        return result;
    }

    public async Task PublishAsync(OutageCatalogLease lease, OutageWork work, OutagePublication publication, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await GuardAsync(connection, transaction, lease, cancellationToken);
        foreach (var month in publication.ChangedMonths)
        {
            await using var command = Command(connection, transaction, """
                INSERT INTO control.event_partition_state(windfarm_id,windfarm_name,source_type,partition_key,
                    revision,committed_attempt,parquet_path,snapshot_path,lineage_path,inserted_by)
                VALUES(@farm,@name,'Outage',@partition,@revision,@token,@parquet,@snapshot,@lineage,'harmonizer')
                ON CONFLICT(windfarm_id,source_type,partition_key) DO UPDATE SET
                    revision=excluded.revision,committed_attempt=excluded.committed_attempt,parquet_path=excluded.parquet_path,
                    snapshot_path=excluded.snapshot_path,lineage_path=excluded.lineage_path,updated_by='harmonizer';
                """, ("farm", lease.Windfarm.WindfarmId), ("name", lease.Windfarm.WindfarmName), ("partition", month.PartitionKey),
                ("revision", month.Revision), ("token", lease.Token), ("parquet", month.ParquetPath), ("snapshot", month.SnapshotPath), ("lineage", month.LineagePath));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        foreach (var update in publication.Lifecycle) await SaveStateAsync(connection, transaction, lease, update, cancellationToken);
        await using var catalog = Command(connection, transaction, """
            UPDATE control.event_partition_state SET revision=revision+1,committed_attempt=@token,
                snapshot_path=@path,last_error_code=NULL,updated_by='harmonizer'
            WHERE windfarm_id=@farm AND source_type='Outage' AND partition_key='_catalog' AND revision=@previous;
            """, ("farm", lease.Windfarm.WindfarmId), ("token", lease.Token), ("path", publication.CatalogPath), ("previous", lease.Revision));
        if (await catalog.ExecuteNonQueryAsync(cancellationToken) != 1) throw new LeaseLostException("Outage catalog generation changed.");
        foreach (var memberKey in publication.AppliedMemberKeys)
        {
            var member = work.Message.Members.SingleOrDefault(member => member.MemberKey == memberKey);
            var workId = member is null ? OutageWorkKeys.Scope(work) : OutageWorkKeys.Event(member.EventId);
            var update = publication.Lifecycle.SingleOrDefault(update => update.State.WorkId == workId);
            await using var receipt = Command(connection, transaction, """
                INSERT INTO control.harmonizer_batch_item(artifact_id,batch_id,wtgid,component_revision,member_key,
                    member_kind,event_id,lifecycle_work_id,planning_generation,materialized_at_utc)
                VALUES(@artifact,@batch,@wtg,@revision,@member,@kind,@event,@work,@generation,
                    CASE WHEN @done THEN clock_timestamp() ELSE NULL END) ON CONFLICT DO NOTHING;
                """, Batch(work.Batch).Concat([
                    ("wtg", (object?)member?.WtgId), ("revision", update?.State.EventRevision ?? 0), ("member", memberKey),
                    ("kind", member is null ? "SnapshotScope" : "EventMember"), ("event", member?.EventId), ("work", workId),
                    ("generation", update?.Plan.Generation ?? 0), ("done", update is null || update.Plan.Pending.Count == 0)]).ToArray());
            await receipt.ExecuteNonQueryAsync(cancellationToken);
        }
        await using var applied = Command(connection, transaction, """
            UPDATE control.harmonizer_batch SET canonical_applied_at_utc=coalesce(canonical_applied_at_utc,clock_timestamp()),last_error_code=NULL
            WHERE artifact_id=@artifact AND batch_id=@batch;
            """, Batch(work.Batch));
        await applied.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        telemetry.Information("Canonical Outage pointers, plans, and receipts committed. BatchId={BatchId} CatalogRevision={CatalogRevision} ChangedPartitions={ChangedPartitions}",
            work.Message.BatchId, lease.Revision + 1, publication.ChangedMonths.Count);
    }

    public async Task<long> ReadDayRevisionAsync(WtgDayKey key, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT component_revision FROM control.wtg_day_input_state WHERE windfarm_id=@farm AND wtgid=@wtg AND data_date=@date AND input_type='Outage';
            """);
        Add(command, ("farm", key.WindfarmId), ("wtg", key.WtgId), ("date", key.DataDate));
        return (long?)await command.ExecuteScalarAsync(cancellationToken) ?? 0;
    }

    public async Task AdvanceAsync(OutageCatalogLease lease, OutageLifecycleUpdate update, OutageDailyCandidate? candidate, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await GuardAsync(connection, transaction, lease, cancellationToken);
        if (candidate is not null)
        {
            await using var daily = Command(connection, transaction, """
                INSERT INTO control.wtg_day_input_state(windfarm_id,wtgid,data_date,input_type,status,component_revision,
                    committed_attempt,canonical_hash,record_count,parquet_path,snapshot_path,manifest_path,
                    last_operation_id,last_execution_id,inserted_by)
                VALUES(@farm,@wtg,@date,'Outage','Complete',@revision,@token,@hash,@count,@parquet,@snapshot,@lineage,@operation,@execution,'harmonizer')
                ON CONFLICT(windfarm_id,wtgid,data_date,input_type) DO UPDATE SET status='Complete',
                    component_revision=excluded.component_revision,committed_attempt=excluded.committed_attempt,
                    canonical_hash=excluded.canonical_hash,record_count=excluded.record_count,parquet_path=excluded.parquet_path,
                    snapshot_path=excluded.snapshot_path,manifest_path=excluded.manifest_path,
                    last_operation_id=excluded.last_operation_id,last_execution_id=excluded.last_execution_id,updated_by='harmonizer'
                WHERE control.wtg_day_input_state.component_revision=@previous
                  AND (control.wtg_day_input_state.lease_expires_at_utc IS NULL OR control.wtg_day_input_state.lease_expires_at_utc<=clock_timestamp());
                """, ("farm", candidate.Key.WindfarmId), ("wtg", candidate.Key.WtgId), ("date", candidate.Key.DataDate),
                ("revision", candidate.Revision), ("previous", candidate.PreviousRevision), ("token", lease.Token),
                ("hash", candidate.CanonicalHash), ("count", candidate.RecordCount), ("parquet", candidate.ParquetPath),
                ("snapshot", candidate.SnapshotPath), ("lineage", candidate.LineagePath),
                ("operation", candidate.Operation.OperationId), ("execution", candidate.Operation.ExecutionId));
            if (await daily.ExecuteNonQueryAsync(cancellationToken) != 1) throw new LeaseLostException("Outage daily revision changed.");
            var id = $"PackageCoordinationRequested:Outage:{candidate.Key.WindfarmId}:{candidate.Key.WtgId}:{candidate.Key.DataDate:yyyy-MM-dd}:{candidate.Revision}";
            var body = JsonSerializer.Serialize(new { SchemaVersion = 1, MessageKind = "PackageCoordinationRequested", NotificationId = id,
                candidate.Operation.OperationId, candidate.Operation.ExecutionId, SourceType = "Outage", PipelineStage = "harmonizer",
                WindfarmId = lease.Windfarm.WindfarmId, lease.Windfarm.WindfarmName, WtgId = candidate.Key.WtgId,
                candidate.Key.DataDate, ComponentRevision = candidate.Revision, Status = "Complete", ManifestPath = candidate.LineagePath }, HarmonizerJson.Options);
            await using var outbox = Command(connection, transaction, """
                INSERT INTO control.harmonizer_outbox(deduplication_key,message_body,operation_id,execution_id)
                VALUES(@id,@body,@operation,@execution) ON CONFLICT DO NOTHING;
                """, ("id", id), ("body", body), ("operation", candidate.Operation.OperationId), ("execution", candidate.Operation.ExecutionId));
            await outbox.ExecuteNonQueryAsync(cancellationToken);
        }
        await SaveStateAsync(connection, transaction, lease, update, cancellationToken);
        if (update.Plan.Pending.Count == 0)
        {
            await using var receipts = Command(connection, transaction, """
                UPDATE control.harmonizer_batch_item i SET materialized_at_utc=clock_timestamp()
                FROM control.harmonizer_batch b
                WHERE i.artifact_id=b.artifact_id AND i.batch_id=b.batch_id AND b.windfarm_id=@farm AND b.source_type='Outage'
                  AND i.lifecycle_work_id=@work AND i.planning_generation<=@generation AND i.materialized_at_utc IS NULL;
                """, ("farm", lease.Windfarm.WindfarmId), ("work", update.State.WorkId), ("generation", update.Plan.Generation));
            await receipts.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        telemetry.Debug("Outage lifecycle progress committed. WorkId={WorkId} Generation={Generation} PendingRanges={PendingRanges}",
            update.State.WorkId, update.Plan.Generation, update.Plan.Pending.Count);
    }

    public async Task RecordFailureAsync(OutageCatalogLease lease, string errorCode, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            UPDATE control.event_partition_state SET last_error_code=@error,updated_by='harmonizer'
            WHERE windfarm_id=@farm AND source_type='Outage' AND partition_key='_catalog' AND lease_token=@token AND lease_owner=@owner;
            UPDATE control.event_materialization_state SET last_error_code=@error,
                due_at_utc=clock_timestamp()+make_interval(secs=>@retry),updated_by='harmonizer'
            WHERE windfarm_id=@farm AND source_type='Outage' AND (has_pending OR needs_continuation)
              AND EXISTS(SELECT 1 FROM control.event_partition_state p WHERE p.windfarm_id=@farm AND p.source_type='Outage'
                AND p.partition_key='_catalog' AND p.lease_token=@token AND p.lease_owner=@owner);
            """);
        Add(command, Fence(lease).Concat([("error", (object?)errorCode), ("retry", options.Value.EventLifecycle.RetrySeconds)]).ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task CompleteReadyBatchesAsync(int farmId, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT b.artifact_id,b.batch_id FROM control.harmonizer_batch b
            WHERE b.windfarm_id=@farm AND b.source_type='Outage' AND b.completed_at_utc IS NULL
              AND b.canonical_applied_at_utc IS NOT NULL
              AND NOT EXISTS(SELECT 1 FROM unnest(b.expected_member_keys) e(member_key)
                WHERE NOT EXISTS(SELECT 1 FROM control.harmonizer_batch_item i
                    WHERE i.artifact_id=b.artifact_id AND i.batch_id=b.batch_id AND i.member_key=e.member_key
                      AND i.materialized_at_utc IS NOT NULL))
            ORDER BY b.artifact_id,b.batch_id LIMIT 100;
            """);
        Add(command, ("farm", farmId));
        var batches = new List<BatchIdentity>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken)) batches.Add(new(reader.GetString(0), reader.GetString(1)));
        foreach (var batch in batches) await CompleteBatchAsync(batch, cancellationToken);
    }

    public async Task<bool> CompleteBatchAsync(BatchIdentity batch, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var completion = Command(connection, transaction, """
            UPDATE control.harmonizer_batch b SET completed_at_utc=coalesce(completed_at_utc,clock_timestamp()),last_error_code=NULL
            WHERE artifact_id=@artifact AND batch_id=@batch AND canonical_applied_at_utc IS NOT NULL
              AND NOT EXISTS(SELECT 1 FROM unnest(b.expected_member_keys) e(member_key)
                WHERE NOT EXISTS(SELECT 1 FROM control.harmonizer_batch_item i
                    WHERE i.artifact_id=b.artifact_id AND i.batch_id=b.batch_id AND i.member_key=e.member_key AND i.materialized_at_utc IS NOT NULL))
            RETURNING completed_at_utc;
            """, Batch(batch));
        var complete = await completion.ExecuteScalarAsync(cancellationToken) is not null;
        if (complete)
        {
            await using var ack = Command(connection, transaction, "SELECT control.raw_artifact_ack(@artifact,@batch,'harmonizer');", Batch(batch));
            await ack.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return complete;
    }

    private static async Task GuardAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, OutageCatalogLease lease, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, """
            SELECT 1 FROM control.event_partition_state WHERE windfarm_id=@farm AND source_type='Outage' AND partition_key='_catalog'
              AND lease_token=@token AND lease_owner=@owner AND lease_expires_at_utc>clock_timestamp() FOR UPDATE;
            """, Fence(lease));
        if (await command.ExecuteScalarAsync(cancellationToken) is null) throw new LeaseLostException("Expired Outage publication owner cannot commit.");
    }

    private static async Task SaveStateAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, OutageCatalogLease lease,
        OutageLifecycleUpdate update, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, """
            INSERT INTO control.event_materialization_state(windfarm_id,source_type,work_id,event_id,partition_key,event_revision,
                planning_generation,plan_path,has_pending,pending_ranges,needs_continuation,continued_through,due_at_utc,inserted_by)
            VALUES(@farm,'Outage',@work,@event,@partition,@revision,@generation,@path,@pending,@ranges::jsonb,@open,@through,@due,'harmonizer')
            ON CONFLICT(windfarm_id,source_type,work_id) DO UPDATE SET event_id=excluded.event_id,partition_key=excluded.partition_key,
                event_revision=excluded.event_revision,planning_generation=excluded.planning_generation,plan_path=excluded.plan_path,
                has_pending=excluded.has_pending,pending_ranges=excluded.pending_ranges,needs_continuation=excluded.needs_continuation,
                continued_through=excluded.continued_through,due_at_utc=excluded.due_at_utc,last_error_code=NULL,updated_by='harmonizer';
            """, ("farm", lease.Windfarm.WindfarmId), ("work", update.State.WorkId), ("event", update.State.EventId),
            ("partition", update.State.PartitionKey), ("revision", update.State.EventRevision), ("generation", update.Plan.Generation),
            ("path", update.State.PlanPath), ("pending", update.Plan.Pending.Count > 0), ("ranges", JsonSerializer.Serialize(update.Plan.Pending, HarmonizerJson.Options)),
            ("open", update.State.NeedsContinuation), ("through", update.State.ContinuedThrough), ("due", update.State.DueUtc));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string? Text(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    private static (string, object?)[] Batch(BatchIdentity batch) => [("artifact", batch.ArtifactId), ("batch", batch.BatchId)];
    private static (string, object?)[] Fence(OutageCatalogLease lease) => [("farm", lease.Windfarm.WindfarmId), ("token", lease.Token), ("owner", lease.Owner)];
    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, params (string, object?)[] values)
    {
        var command = new NpgsqlCommand(sql, connection, transaction);
        Add(command, values);
        return command;
    }
    private static void Add(NpgsqlCommand command, params (string Name, object? Value)[] values)
    {
        foreach (var (name, value) in values)
        {
            if (value is not null) command.Parameters.AddWithValue(name, value);
            else command.Parameters.Add(name, name == "wtg" ? NpgsqlDbType.Integer : name == "through" ? NpgsqlDbType.Date : NpgsqlDbType.Text).Value = DBNull.Value;
        }
    }
}
