-- Shared extraction control plane for Production, Curtailment, Outage and Alarm:
-- one cursor per (windfarm_id, source_type), one audit row per execution attempt, and the Alarm
-- ingestor's durable open-alarm polling state.
-- Fresh-database script; run after 0001. No code path in this repo executes DDL.
-- windfarm_id is a positive INTEGER; repositories parse the public string windfarmId at the DB boundary.

CREATE TABLE extraction_cursor (
    windfarm_id                         INTEGER     NOT NULL,
    source_type                         TEXT        NOT NULL,
    is_enabled                          BOOLEAN     NOT NULL DEFAULT FALSE,
    created_by                          TEXT        NOT NULL DEFAULT current_user,
    created_date                        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_date                        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- Committed change-timestamp frontier. '-infinity' means not yet seeded; seed a finite
    -- checkpoint before enabling a registration.
    last_committed_updated_at           TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    -- Committed tie-break identity as TEXT (an Outage GUID, a numeric id rendered as text, ...).
    -- Each ingestor owns its own comparison/parsing of this value.
    last_committed_id                   TEXT        NOT NULL DEFAULT '0',

    lease_token                         BIGINT      NOT NULL DEFAULT 0 CHECK (lease_token >= 0),
    lease_owner                         TEXT,
    lease_expires_at                    TIMESTAMPTZ,

    last_run_started_utc                TIMESTAMPTZ,
    last_committed_utc                  TIMESTAMPTZ,
    active_run_id                       BIGINT,
    last_run_completed_utc              TIMESTAMPTZ,
    last_run_outcome                    TEXT CHECK (last_run_outcome IN ('Completed', 'Failed')),
    consecutive_failures                INTEGER     NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    last_error                          TEXT,
    deleted_last_committed_updated_at   TIMESTAMPTZ,
    deleted_last_committed_id           TEXT,

    PRIMARY KEY (windfarm_id, source_type)
);

-- Partial indexes stay small: only rows holding a lease / enabled registrations.
CREATE INDEX ix_extraction_cursor_lease_expires
    ON extraction_cursor (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;

CREATE INDEX ix_extraction_cursor_enabled
    ON extraction_cursor (source_type, windfarm_id) WHERE is_enabled;

-- One row per execution attempt, not per window/file. from_* is the committed checkpoint at lease
-- acquisition; to_* is this attempt's resulting checkpoint. Failure or cancellation leaves the
-- checkpoint unchanged. Refresh never changes the incremental checkpoint.
CREATE TABLE extraction_run (
    id                  BIGSERIAL   PRIMARY KEY,
    windfarm_id         INTEGER     NOT NULL,
    source_type         TEXT        NOT NULL,

    status              TEXT        NOT NULL CHECK (status IN ('Running', 'Completed', 'Failed', 'Busy', 'Cancelled')),
    execution_mode      TEXT        NOT NULL DEFAULT 'Incremental' CHECK (execution_mode IN ('Incremental', 'Refresh')),
    created_by          TEXT        NOT NULL DEFAULT current_user,
    created_date        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_date        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    request_id          TEXT,
    lease_token         BIGINT,
    lease_owner         TEXT,
    job_name            TEXT,
    requested_by        TEXT,
    reason              TEXT,
    requested_dates     DATE[],

    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,

    from_updated_at     TIMESTAMPTZ NOT NULL,
    from_committed_id   TEXT        NOT NULL,
    to_updated_at       TIMESTAMPTZ,
    to_committed_id     TEXT,

    -- Exact executed query range; distinct from the checkpoint because of the safety overlap and
    -- because an empty result still executes a real range.
    query_start_inclusive_utc TIMESTAMPTZ,
    query_end_exclusive_utc   TIMESTAMPTZ,

    -- Run-owned artifact prefix covering ALL shards/files this run wrote.
    output_path         TEXT,
    error_message       TEXT,

    CONSTRAINT ck_extraction_run_completed_at
        CHECK (completed_at IS NULL OR completed_at >= started_at),
    CONSTRAINT ck_extraction_run_query_bounds
        CHECK (query_start_inclusive_utc IS NULL OR query_end_exclusive_utc IS NULL
               OR query_end_exclusive_utc > query_start_inclusive_utc),
    FOREIGN KEY (windfarm_id, source_type) REFERENCES extraction_cursor (windfarm_id, source_type)
);

CREATE INDEX ix_extraction_run_farm_source_started
    ON extraction_run (windfarm_id, source_type, started_at DESC);
CREATE INDEX ix_extraction_run_status
    ON extraction_run (status);
CREATE INDEX ix_extraction_run_request
    ON extraction_run (request_id, id DESC);
-- The in-flight run for a cursor.
CREATE INDEX ix_extraction_run_running
    ON extraction_run (windfarm_id, source_type)
    WHERE status = 'Running';

CREATE FUNCTION set_extraction_audit_metadata()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.created_by := COALESCE(NULLIF(NEW.created_by, ''), current_user);
        NEW.created_date := COALESCE(NEW.created_date, clock_timestamp());
    ELSE
        NEW.created_by := OLD.created_by;
        NEW.created_date := OLD.created_date;
    END IF;

    NEW.updated_date := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_extraction_cursor_audit_metadata
    BEFORE INSERT OR UPDATE ON extraction_cursor
    FOR EACH ROW EXECUTE FUNCTION set_extraction_audit_metadata();

CREATE TRIGGER trg_extraction_run_audit_metadata
    BEFORE INSERT OR UPDATE ON extraction_run
    FOR EACH ROW EXECUTE FUNCTION set_extraction_audit_metadata();

-- Alarm ingestor's durable open-alarm polling state (operational, not cursor/lease data).
CREATE TABLE alarm_known_open_alarm (
    windfarm_id     INTEGER     NOT NULL,
    turbine_rdspp   TEXT        NOT NULL,
    alarm_id        BIGINT      NOT NULL,
    first_seen_utc  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (windfarm_id, turbine_rdspp, alarm_id)
);
