-- Raw Fan-out persistence: the accepted-artifact ledger (parent/child plan), the SQS dead-letter archive,
-- audit triggers and the lifecycle/retention functions. Fresh-database script; run after 0002.
-- Worker startup never runs DDL.
--
-- Table meaning (see also the COMMENTs below):
--   raw_artifact_dispatch      one accepted logical parent artifact/envelope  -> frozen plan identity, parent progress
--   raw_artifact_batch         one outgoing child work batch                  -> stable replay payload, materialization ack
--   raw_fan_out_dead_letter    one archived SQS DLQ message                   -> investigation and recovery
--
-- Audit columns (created_date_utc, updated_date_utc, inserted_by, updated_by) are stamped by triggers.
-- Actors are passed explicitly by callers (raw-fanout-worker, raw-fanout-dlq-worker, harmonizer,
-- operator:<name>), never taken from the DB login or session state.

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE control.raw_artifact_dispatch (
    artifact_id                    text        PRIMARY KEY CHECK (artifact_id ~ '^[0-9a-f]{64}$'),
    source_type                    varchar(32) NOT NULL,
    windfarm_id                    integer     NOT NULL,
    run_id                         varchar(128) NOT NULL,
    extraction_token               bigint      NOT NULL CHECK (extraction_token >= 0),
    original_envelope              jsonb       NOT NULL,
    -- Frozen at planning time; the outgoing plan itself lives only in raw_artifact_batch.
    expected_batch_count           integer     NOT NULL CHECK (expected_batch_count > 0),
    planned_at_utc                 timestamptz NOT NULL DEFAULT clock_timestamp(),
    fan_out_completed_at_utc       timestamptz,
    harmonization_completed_at_utc timestamptz,
    created_date_utc               timestamptz NOT NULL,
    updated_date_utc               timestamptz NOT NULL,
    inserted_by                    text        NOT NULL,
    updated_by                     text        NOT NULL,
    CHECK (harmonization_completed_at_utc IS NULL OR fan_out_completed_at_utc IS NOT NULL)
);

CREATE TABLE control.raw_artifact_batch (
    artifact_id         text        NOT NULL REFERENCES control.raw_artifact_dispatch (artifact_id),
    batch_id            text        NOT NULL CHECK (batch_id ~ '^[0-9a-f]{64}$'),
    batch_ordinal       integer     NOT NULL,
    message_body        text        NOT NULL,
    -- Harmonizer durably applied this batch. NOT the time Fan-out published it.
    materialized_at_utc timestamptz,
    created_date_utc    timestamptz NOT NULL,
    updated_date_utc    timestamptz NOT NULL,
    inserted_by         text        NOT NULL,
    updated_by          text        NOT NULL,
    PRIMARY KEY (artifact_id, batch_id),
    CONSTRAINT raw_artifact_batch_ordinal_unique UNIQUE (artifact_id, batch_ordinal)
);

CREATE INDEX raw_artifact_pending ON control.raw_artifact_dispatch (planned_at_utc)
    WHERE harmonization_completed_at_utc IS NULL;
CREATE INDEX raw_artifact_batch_pending ON control.raw_artifact_batch (artifact_id)
    WHERE materialized_at_utc IS NULL;

CREATE TABLE control.raw_fan_out_dead_letter (
    incident_id             uuid        PRIMARY KEY,
    queue_url               text        NOT NULL,
    message_id              text        NOT NULL,
    -- Original message text, kept verbatim even when it is not valid JSON.
    original_body           text        NOT NULL,
    attributes              jsonb       NOT NULL,
    artifact_paths          text[]      NOT NULL DEFAULT '{}',
    first_observed_at_utc   timestamptz NOT NULL DEFAULT now(),
    -- Last genuine re-archive of this exact message; not a processing-attempt counter.
    last_observed_at_utc    timestamptz NOT NULL DEFAULT now(),
    -- Populated only from evidence (e.g. an envelope validation failure); NULL means unknown.
    last_error              text,
    error_type              text,
    -- Best-effort metadata parsed from original_body; NULL for malformed messages.
    source_type             varchar(32),
    windfarm_id             integer,
    extraction_token        bigint,
    run_id                  varchar(128),
    trace_id                text,
    artifact_id             text,
    data_date               date,
    -- Republication was requested. NOT resolution.
    replay_requested_at_utc timestamptz,
    -- Set only by verified recovery or an explicit operator closure.
    resolved_at_utc         timestamptz,
    resolution_reason       text,
    resolved_by             text,
    created_date_utc        timestamptz NOT NULL,
    updated_date_utc        timestamptz NOT NULL,
    inserted_by             text        NOT NULL,
    updated_by              text        NOT NULL,
    UNIQUE (queue_url, message_id)
);

CREATE INDEX ix_raw_fan_out_dead_letter_open
    ON control.raw_fan_out_dead_letter (first_observed_at_utc)
    WHERE resolved_at_utc IS NULL;
CREATE INDEX ix_raw_fan_out_dead_letter_paths
    ON control.raw_fan_out_dead_letter USING gin (artifact_paths);
CREATE INDEX ix_raw_fan_out_dead_letter_artifact
    ON control.raw_fan_out_dead_letter (artifact_id) WHERE artifact_id IS NOT NULL;

COMMENT ON TABLE control.raw_artifact_dispatch IS
    'One accepted logical parent artifact/envelope (a file, several shards or an extraction-ready set). original_envelope is the parsed incoming JSONB (not byte-identical). Holds frozen plan identity/count and parent progress only; the outgoing plan lives in raw_artifact_batch.';
COMMENT ON TABLE control.raw_artifact_batch IS
    'One outgoing child work batch. message_body is the stable serialized Fan-out to Harmonizer message replayed on retry; batch_ordinal fixes order; materialized_at_utc means Harmonizer durably applied the work, NOT that Fan-out published it.';
COMMENT ON TABLE control.raw_fan_out_dead_letter IS
    'One archived SQS DLQ message identified by queue_url + message_id, kept for investigation and recovery. Business columns are best-effort and nullable.';

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['raw_artifact_dispatch','raw_artifact_batch','raw_fan_out_dead_letter'] LOOP
        EXECUTE format('COMMENT ON COLUMN control.%I.created_date_utc IS %L', t, 'First insertion time (immutable).');
        EXECUTE format('COMMENT ON COLUMN control.%I.updated_date_utc IS %L', t,
            'Most recent meaningful persisted change; not a full modification history.');
        EXECUTE format('COMMENT ON COLUMN control.%I.inserted_by IS %L', t, 'Service/operator that inserted the row (immutable).');
        EXECUTE format('COMMENT ON COLUMN control.%I.updated_by IS %L', t, 'Service/operator responsible for the latest meaningful change.');
    END LOOP;
END $$;

-- Stamp trigger: fires on INSERT and on any UPDATE whose SET list names updated_by, which is how a
-- writer proves it supplied an actor even when re-using the same actor value.
CREATE FUNCTION control.raw_fanout_audit_stamp()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE audit_columns constant text[] := ARRAY['created_date_utc','updated_date_utc','inserted_by','updated_by'];
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.inserted_by IS NULL OR btrim(NEW.inserted_by) = '' THEN
            RAISE EXCEPTION 'inserted_by is required on %', TG_TABLE_NAME USING ERRCODE = '23502';
        END IF;
        NEW.created_date_utc := clock_timestamp();
        NEW.updated_date_utc := NEW.created_date_utc;
        NEW.updated_by := NEW.inserted_by;
    ELSE
        IF NEW.updated_by IS NULL OR btrim(NEW.updated_by) = '' THEN
            RAISE EXCEPTION 'updated_by must not be blank on % update', TG_TABLE_NAME USING ERRCODE = '23502';
        END IF;
        IF (to_jsonb(NEW) - audit_columns) IS NOT DISTINCT FROM (to_jsonb(OLD) - audit_columns) THEN
            -- Duplicate/no-op operations must not rewrite attribution or timestamps.
            NEW.updated_by := OLD.updated_by;
            NEW.updated_date_utc := OLD.updated_date_utc;
        ELSE
            NEW.updated_date_utc := clock_timestamp();
        END IF;
    END IF;
    RETURN NEW;
END $$;

-- Guard trigger (named to fire after the stamp trigger): rejects edits to immutable fields and any
-- business change from a writer that did not supply updated_by. Protects against stale writers; it is
-- not a security boundary.
CREATE FUNCTION control.raw_fanout_audit_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE audit_columns constant text[] := ARRAY['created_date_utc','updated_date_utc','inserted_by','updated_by'];
BEGIN
    IF NEW.created_date_utc IS DISTINCT FROM OLD.created_date_utc OR NEW.inserted_by IS DISTINCT FROM OLD.inserted_by THEN
        RAISE EXCEPTION 'created_date_utc and inserted_by are immutable on %', TG_TABLE_NAME USING ERRCODE = '23514';
    END IF;
    IF (to_jsonb(NEW) - audit_columns) IS DISTINCT FROM (to_jsonb(OLD) - audit_columns)
       AND NEW.updated_date_utc IS NOT DISTINCT FROM OLD.updated_date_utc THEN
        RAISE EXCEPTION 'updated_by must be set explicitly when modifying %', TG_TABLE_NAME USING ERRCODE = '23502';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER trg_raw_artifact_dispatch_audit_1_stamp
    BEFORE INSERT OR UPDATE OF updated_by ON control.raw_artifact_dispatch
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_stamp();
CREATE TRIGGER trg_raw_artifact_dispatch_audit_2_guard
    BEFORE UPDATE ON control.raw_artifact_dispatch
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_guard();

CREATE TRIGGER trg_raw_artifact_batch_audit_1_stamp
    BEFORE INSERT OR UPDATE OF updated_by ON control.raw_artifact_batch
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_stamp();
CREATE TRIGGER trg_raw_artifact_batch_audit_2_guard
    BEFORE UPDATE ON control.raw_artifact_batch
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_guard();

CREATE TRIGGER trg_raw_fan_out_dead_letter_audit_1_stamp
    BEFORE INSERT OR UPDATE OF updated_by ON control.raw_fan_out_dead_letter
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_stamp();
CREATE TRIGGER trg_raw_fan_out_dead_letter_audit_2_guard
    BEFORE UPDATE ON control.raw_fan_out_dead_letter
    FOR EACH ROW EXECUTE FUNCTION control.raw_fanout_audit_guard();

-- Freezes the complete parent/child plan in one transaction. A duplicate call returns the
-- already-saved plan (never a new one) and changes nothing.
CREATE FUNCTION control.raw_artifact_save_plan(a text,s text,f integer,r text,t bigint,e jsonb,p jsonb,actor text)
RETURNS TABLE(batch_id text, message_body text, batch_ordinal integer) LANGUAGE plpgsql AS $$
DECLARE existing control.raw_artifact_dispatch%ROWTYPE; inserted integer; child jsonb; idx integer := 0;
BEGIN
    IF actor IS NULL OR btrim(actor) = '' THEN RAISE EXCEPTION 'actor is required'; END IF;
    IF jsonb_typeof(p) IS DISTINCT FROM 'array' OR jsonb_array_length(p)=0 THEN RAISE EXCEPTION 'Empty artifact plan'; END IF;
    INSERT INTO control.raw_artifact_dispatch(artifact_id,source_type,windfarm_id,run_id,extraction_token,original_envelope,expected_batch_count,inserted_by)
    VALUES(a,s,f,r,t,e,jsonb_array_length(p),actor) ON CONFLICT(artifact_id) DO NOTHING;
    GET DIAGNOSTICS inserted=ROW_COUNT;
    SELECT * INTO STRICT existing FROM control.raw_artifact_dispatch WHERE artifact_id=a FOR UPDATE;
    IF (existing.source_type,existing.windfarm_id,existing.run_id,existing.extraction_token,existing.original_envelope)
        IS DISTINCT FROM (s,f,r,t,e) THEN RAISE EXCEPTION 'Immutable artifact identity or envelope changed'; END IF;
    IF inserted=1 THEN
        FOR child IN SELECT value FROM jsonb_array_elements(p) LOOP
            INSERT INTO control.raw_artifact_batch(artifact_id,batch_id,message_body,batch_ordinal,inserted_by)
            VALUES(a,child->>'Id',child->>'Body',idx,actor);
            idx := idx + 1;
        END LOOP;
    END IF;
    RETURN QUERY SELECT b.batch_id, b.message_body, b.batch_ordinal
        FROM control.raw_artifact_batch b WHERE b.artifact_id = a ORDER BY b.batch_ordinal;
END $$;

-- Dispatch marking and acknowledgement lock the parent first and re-evaluate completion on both paths,
-- so an acknowledgement that arrives before fan-out completion is still honoured. Updates are conditional:
-- a duplicate call neither changes a stable timestamp nor re-attributes the row.
CREATE FUNCTION control.raw_artifact_mark_dispatched(a text, actor text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE expected integer; all_materialized boolean;
BEGIN
    IF actor IS NULL OR btrim(actor) = '' THEN RAISE EXCEPTION 'actor is required'; END IF;
    SELECT expected_batch_count INTO STRICT expected FROM control.raw_artifact_dispatch WHERE artifact_id=a FOR UPDATE;
    IF (SELECT count(*) FROM control.raw_artifact_batch WHERE artifact_id=a)<>expected
    THEN RAISE EXCEPTION 'Incomplete durable plan'; END IF;
    all_materialized := NOT EXISTS(SELECT 1 FROM control.raw_artifact_batch WHERE artifact_id=a AND materialized_at_utc IS NULL);
    UPDATE control.raw_artifact_dispatch
    SET fan_out_completed_at_utc = coalesce(fan_out_completed_at_utc, clock_timestamp()),
        harmonization_completed_at_utc = CASE WHEN all_materialized
            THEN coalesce(harmonization_completed_at_utc, clock_timestamp()) ELSE harmonization_completed_at_utc END,
        updated_by = actor
    WHERE artifact_id=a
      AND (fan_out_completed_at_utc IS NULL OR (all_materialized AND harmonization_completed_at_utc IS NULL));
END $$;

-- Harmonizer integration: call inside the SAME transaction as the last durable state commit for ALL work
-- belonging to this batch, passing the Harmonizer's own service identity. Never ack on receive.
CREATE FUNCTION control.raw_artifact_ack(a text, b text, actor text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE expected integer; all_materialized boolean;
BEGIN
    IF actor IS NULL OR btrim(actor) = '' THEN RAISE EXCEPTION 'actor is required'; END IF;
    SELECT expected_batch_count INTO STRICT expected FROM control.raw_artifact_dispatch WHERE artifact_id=a FOR UPDATE;
    IF (SELECT count(*) FROM control.raw_artifact_batch WHERE artifact_id=a)<>expected
    THEN RAISE EXCEPTION 'Incomplete durable plan'; END IF;
    IF NOT EXISTS(SELECT 1 FROM control.raw_artifact_batch WHERE artifact_id=a AND batch_id=b)
    THEN RAISE EXCEPTION 'Unknown artifact batch'; END IF;
    UPDATE control.raw_artifact_batch
    SET materialized_at_utc = clock_timestamp(), updated_by = actor
    WHERE artifact_id=a AND batch_id=b AND materialized_at_utc IS NULL;
    all_materialized := NOT EXISTS(SELECT 1 FROM control.raw_artifact_batch WHERE artifact_id=a AND materialized_at_utc IS NULL);
    UPDATE control.raw_artifact_dispatch
    SET harmonization_completed_at_utc = clock_timestamp(), updated_by = actor
    WHERE artifact_id=a AND all_materialized
      AND fan_out_completed_at_utc IS NOT NULL AND harmonization_completed_at_utc IS NULL;
END $$;

-- Retention for fully completed artifacts. Deletes children before the parent in one bounded transaction
-- per call; SKIP LOCKED keeps concurrent cleanup runs and Harmonizer acknowledgements from blocking.
CREATE FUNCTION control.raw_artifact_cleanup_batch(cutoff_utc timestamptz, max_parents integer)
RETURNS TABLE(deleted_artifacts integer, deleted_batches integer) LANGUAGE plpgsql AS $$
DECLARE selected text[]; batch_count integer;
BEGIN
    IF max_parents IS NULL OR max_parents < 1 THEN RAISE EXCEPTION 'max_parents must be a positive integer'; END IF;

    -- Eligibility: harmonized, past the retention cutoff, and every expected child materialized.
    -- Never uses planned_at_utc or fan_out_completed_at_utc as the retention clock.
    SELECT array_agg(artifact_id) INTO selected FROM (
        SELECT d.artifact_id
        FROM control.raw_artifact_dispatch d
        WHERE d.harmonization_completed_at_utc IS NOT NULL
          AND d.harmonization_completed_at_utc < cutoff_utc
          AND d.expected_batch_count = (
              SELECT count(*) FROM control.raw_artifact_batch b WHERE b.artifact_id = d.artifact_id
          )
          AND NOT EXISTS (
              SELECT 1 FROM control.raw_artifact_batch b
              WHERE b.artifact_id = d.artifact_id AND b.materialized_at_utc IS NULL
          )
        ORDER BY d.harmonization_completed_at_utc
        LIMIT max_parents
        FOR UPDATE OF d SKIP LOCKED
    ) eligible;

    IF selected IS NULL OR array_length(selected, 1) IS NULL THEN
        RETURN QUERY SELECT 0, 0;
        RETURN;
    END IF;

    DELETE FROM control.raw_artifact_batch WHERE artifact_id = ANY(selected);
    GET DIAGNOSTICS batch_count = ROW_COUNT;
    DELETE FROM control.raw_artifact_dispatch WHERE artifact_id = ANY(selected);

    RETURN QUERY SELECT array_length(selected, 1), batch_count;
END $$;

-- Separate retention rule for the archive: only RESOLVED incidents past the cutoff are deletable, never
-- unresolved ones however old.
CREATE FUNCTION control.raw_fan_out_dead_letter_cleanup_batch(cutoff_utc timestamptz, max_incidents integer)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE deleted integer;
BEGIN
    IF max_incidents IS NULL OR max_incidents < 1 THEN RAISE EXCEPTION 'max_incidents must be a positive integer'; END IF;
    WITH doomed AS (
        SELECT incident_id FROM control.raw_fan_out_dead_letter
        WHERE resolved_at_utc IS NOT NULL AND resolved_at_utc < cutoff_utc
        ORDER BY resolved_at_utc
        LIMIT max_incidents
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM control.raw_fan_out_dead_letter d USING doomed WHERE d.incident_id = doomed.incident_id;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    RETURN deleted;
END $$;
