-- Harmonizer consolidated baseline: base tables, operation correlation and event lifecycle.
-- Apply ONCE to a database where the Harmonizer schema has not yet been installed.
-- Requires the existing Raw Fan-Out control schema, audit functions and raw_artifact_ack.
-- This replaces the previously unexecuted Harmonizer 0004/0005/0006 chain.
-- Do not apply over an existing Harmonizer schema; deployed databases require forward migrations.
-- Run through the migration pipeline. Worker startup never executes DDL.
BEGIN;
CREATE TABLE control.harmonizer_batch (
    artifact_id text NOT NULL,
    batch_id text NOT NULL,
    expected_wtgids integer[] NOT NULL,
    message_body text NOT NULL,
    completed_at_utc timestamptz,
    created_date_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
    operation_id text,
    execution_id text,
    work_kind text NOT NULL DEFAULT 'ProductionWtgDay',
    windfarm_id integer,
    source_type text NOT NULL DEFAULT 'Production',
    canonical_applied_at_utc timestamptz,
    last_error_code text,
    expected_member_keys text[] NOT NULL DEFAULT '{}',
    expected_event_batch_ids text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (artifact_id,batch_id),
    CONSTRAINT harmonizer_batch_operation_valid CHECK
        (operation_id IS NULL OR (length(operation_id) BETWEEN 1 AND 256 AND btrim(operation_id) <> '' AND operation_id !~ '[[:cntrl:]]')),
    CONSTRAINT harmonizer_batch_execution_valid CHECK
        (execution_id IS NULL OR (length(execution_id) BETWEEN 1 AND 256 AND btrim(execution_id) <> '' AND execution_id !~ '[[:cntrl:]]')),
    CHECK
    ((work_kind='ProductionWtgDay' AND cardinality(expected_wtgids)>0) OR
     (work_kind='CanonicalEvents' AND cardinality(expected_member_keys)>0) OR
     (work_kind IN ('EventSnapshotReconciliation','ArtifactObserved') AND cardinality(expected_member_keys)=1))
    -- Deliberately no FK to the short-lived Fan-Out ledger: Harmonizer recovery metadata has its own retention.
);
CREATE TABLE control.wtg_day_input_state (
    windfarm_id integer NOT NULL CHECK (windfarm_id > 0),
    wtgid integer NOT NULL CHECK (wtgid > 0),
    data_date date NOT NULL,
    input_type varchar(32) NOT NULL CHECK (input_type IN ('Production','Outage','Alarm','Curtailment','Adjustment')),
    status varchar(16) NOT NULL DEFAULT 'Partial' CHECK (status IN ('Partial','Complete','Failed')),
    component_revision bigint NOT NULL DEFAULT 0 CHECK (component_revision >= 0),
    committed_attempt bigint,
    canonical_hash text,
    record_count integer NOT NULL DEFAULT 0 CHECK (record_count >= 0),
    parquet_path text,
    snapshot_path text,
    manifest_path text,
    lease_token bigint NOT NULL DEFAULT 0,
    lease_owner uuid,
    lease_expires_at_utc timestamptz,
    first_failed_artifact_id text,
    first_failed_batch_id text,
    first_failed_raw_paths text[],
    last_error_code text,
    created_date_utc timestamptz NOT NULL,
    updated_date_utc timestamptz NOT NULL,
    inserted_by text NOT NULL,
    updated_by text NOT NULL,
    last_operation_id text,
    last_execution_id text,
    PRIMARY KEY (windfarm_id,wtgid,data_date,input_type),
    CONSTRAINT harmonizer_state_operation_valid CHECK
        (last_operation_id IS NULL OR (length(last_operation_id) BETWEEN 1 AND 256 AND btrim(last_operation_id) <> '' AND last_operation_id !~ '[[:cntrl:]]')),
    CONSTRAINT harmonizer_state_execution_valid CHECK
        (last_execution_id IS NULL OR (length(last_execution_id) BETWEEN 1 AND 256 AND btrim(last_execution_id) <> '' AND last_execution_id !~ '[[:cntrl:]]')),
    CHECK (status <> 'Complete' OR (component_revision > 0 AND snapshot_path IS NOT NULL AND manifest_path IS NOT NULL)),
    CHECK (status <> 'Complete' OR input_type <> 'Production' OR record_count = 144),
    CHECK (record_count = 0 OR parquet_path IS NOT NULL),
    CHECK (component_revision = 0 OR (committed_attempt IS NOT NULL AND canonical_hash ~ '^[0-9a-f]{64}$')),
    CHECK (status <> 'Failed' OR first_failed_batch_id IS NOT NULL),
    CHECK ((first_failed_artifact_id IS NULL) = (first_failed_batch_id IS NULL))
);
CREATE TRIGGER trg_harmonizer_state_audit_1_stamp
    BEFORE INSERT OR UPDATE OF updated_by ON control.wtg_day_input_state
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_stamp();
CREATE TRIGGER trg_harmonizer_state_audit_2_guard
    BEFORE UPDATE ON control.wtg_day_input_state
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_guard();
CREATE TABLE control.harmonizer_batch_item (
    artifact_id text NOT NULL,
    batch_id text NOT NULL,
    wtgid integer,
    component_revision bigint NOT NULL,
    applied_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
    member_key text,
    member_kind text NOT NULL DEFAULT 'WtgDay',
    event_id text,
    lifecycle_work_id text,
    planning_generation bigint,
    materialized_at_utc timestamptz,
    FOREIGN KEY (artifact_id,batch_id) REFERENCES control.harmonizer_batch(artifact_id,batch_id),
    CHECK
    ((member_kind='WtgDay' AND wtgid IS NOT NULL AND member_key IS NULL) OR
     (member_kind IN ('EventMember','SnapshotScope','ArtifactObservation') AND member_key IS NOT NULL))
);
CREATE TABLE control.harmonizer_outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    deduplication_key text NOT NULL UNIQUE,
    message_body text NOT NULL,
    created_date_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at_utc timestamptz,
    operation_id text,
    execution_id text,
    CONSTRAINT harmonizer_outbox_operation_valid CHECK
        (operation_id IS NULL OR (length(operation_id) BETWEEN 1 AND 256 AND btrim(operation_id) <> '' AND operation_id !~ '[[:cntrl:]]')),
    CONSTRAINT harmonizer_outbox_execution_valid CHECK
        (execution_id IS NULL OR (length(execution_id) BETWEEN 1 AND 256 AND btrim(execution_id) <> '' AND execution_id !~ '[[:cntrl:]]'))
);
CREATE INDEX harmonizer_outbox_pending ON control.harmonizer_outbox(id) WHERE sent_at_utc IS NULL;
CREATE INDEX harmonizer_state_failed ON control.wtg_day_input_state(updated_date_utc) WHERE status='Failed';

CREATE INDEX harmonizer_batch_operation ON control.harmonizer_batch(operation_id,execution_id)
    WHERE operation_id IS NOT NULL;
CREATE INDEX harmonizer_outbox_operation ON control.harmonizer_outbox(operation_id,execution_id)
    WHERE operation_id IS NOT NULL;

CREATE UNIQUE INDEX harmonizer_batch_item_production_key
    ON control.harmonizer_batch_item(artifact_id,batch_id,wtgid) WHERE member_kind='WtgDay';
CREATE UNIQUE INDEX harmonizer_batch_item_event_key
    ON control.harmonizer_batch_item(artifact_id,batch_id,member_key) WHERE member_kind<>'WtgDay';
CREATE INDEX harmonizer_event_batches_pending ON control.harmonizer_batch(windfarm_id,source_type)
    WHERE completed_at_utc IS NULL AND source_type IN ('Outage','Alarm','Adjustment');
CREATE INDEX harmonizer_event_pending_receipts
    ON control.harmonizer_batch_item(lifecycle_work_id,planning_generation) WHERE materialized_at_utc IS NULL AND member_kind<>'WtgDay';

CREATE TABLE control.event_partition_state (
    windfarm_id integer NOT NULL CHECK(windfarm_id>0),
    windfarm_name text NOT NULL,
    source_type text NOT NULL,
    partition_key text NOT NULL, -- _catalog, yyyy-MM, or _tombstones
    revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
    committed_attempt bigint,
    parquet_path text,
    snapshot_path text,
    lineage_path text,
    lease_token bigint NOT NULL DEFAULT 0,
    lease_owner uuid,
    lease_expires_at_utc timestamptz,
    last_error_code text,
    created_date_utc timestamptz NOT NULL,
    updated_date_utc timestamptz NOT NULL,
    inserted_by text NOT NULL,
    updated_by text NOT NULL,
    PRIMARY KEY(windfarm_id,source_type,partition_key),
    CHECK(revision=0 OR (committed_attempt IS NOT NULL AND snapshot_path IS NOT NULL))
);
CREATE TABLE control.event_materialization_state (
    windfarm_id integer NOT NULL,
    source_type text NOT NULL,
    work_id text NOT NULL, -- EventId or an artifact-scoped snapshot reconciliation identity
    event_id text,
    partition_key text,
    event_revision bigint NOT NULL CHECK(event_revision>=0),
    planning_generation bigint NOT NULL CHECK(planning_generation>0),
    plan_path text NOT NULL,
    has_pending boolean NOT NULL,
    pending_ranges jsonb NOT NULL DEFAULT '[]'::jsonb CHECK(jsonb_typeof(pending_ranges)='array'),
    needs_continuation boolean NOT NULL,
    continued_through date,
    due_at_utc timestamptz NOT NULL,
    last_error_code text,
    created_date_utc timestamptz NOT NULL,
    updated_date_utc timestamptz NOT NULL,
    inserted_by text NOT NULL,
    updated_by text NOT NULL,
    PRIMARY KEY(windfarm_id,source_type,work_id)
);
CREATE INDEX harmonizer_event_due ON control.event_materialization_state(due_at_utc,windfarm_id)
    WHERE has_pending OR needs_continuation;
CREATE INDEX harmonizer_event_pending_farm ON control.event_materialization_state(windfarm_id,source_type) WHERE has_pending;
DO $migration$
DECLARE table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['event_partition_state','event_materialization_state'] LOOP
        EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE OF updated_by ON control.%I FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_stamp()',table_name||'_audit_1_stamp',table_name);
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE ON control.%I FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_guard()',table_name||'_audit_2_guard',table_name);
    END LOOP;
END $migration$;
-- Package Coordinator must read this view (or apply the same predicate), because an
-- older Complete day pointer remains valid history while a newer lifecycle rebuild is pending.
CREATE FUNCTION control.harmonizer_event_day_pending(p_windfarm_id integer, p_wtgid integer, p_data_date date, p_source_type text)
RETURNS boolean LANGUAGE sql STABLE AS $function$
SELECT EXISTS(
    SELECT 1 FROM control.harmonizer_batch b WHERE b.windfarm_id=p_windfarm_id AND b.source_type=p_source_type
      AND b.canonical_applied_at_utc IS NULL AND (b.work_kind='CanonicalEvents' OR b.last_error_code IS NOT NULL)
 ) OR EXISTS(
    SELECT 1 FROM control.harmonizer_batch b
    WHERE b.windfarm_id=p_windfarm_id AND b.source_type=p_source_type
      AND b.work_kind='EventSnapshotReconciliation' AND b.canonical_applied_at_utc IS NULL
      AND b.message_body::jsonb->'SnapshotScope'->'WtgIds' @> to_jsonb(ARRAY[p_wtgid])
      AND (b.message_body::jsonb->'SnapshotScope'->>'StartInclusiveUtc')::timestamptz < ((p_data_date+1)::timestamp AT TIME ZONE 'UTC')
      AND (b.message_body::jsonb->'SnapshotScope'->>'EndExclusiveUtc')::timestamptz > (p_data_date::timestamp AT TIME ZONE 'UTC')
) OR EXISTS(
    SELECT 1 FROM control.event_materialization_state s,
         LATERAL jsonb_array_elements(s.pending_ranges) r
    WHERE s.windfarm_id=p_windfarm_id AND s.source_type=p_source_type AND s.has_pending
      AND (r->>'WtgId')::integer=p_wtgid
      AND p_data_date BETWEEN (r->>'StartInclusive')::date AND (r->>'EndInclusive')::date
);
$function$;
CREATE VIEW control.harmonizer_input_readiness AS
SELECT d.*, CASE WHEN d.input_type IN ('Outage','Alarm','Adjustment') AND
    control.harmonizer_event_day_pending(d.windfarm_id,d.wtgid,d.data_date,d.input_type)
    THEN 'Pending' ELSE d.status END AS readiness_status
FROM control.wtg_day_input_state d;

COMMIT;
