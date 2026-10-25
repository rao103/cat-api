# PostgreSQL schema

Deploy to a fresh database by running the three scripts in `src/db/migrations/` in order, then the seed files in `src/db/PostDeployment/`:

| Script | Creates |
|---|---|
| `0001_create_reference_schema.sql` | `windfarm`, `wind_turbine` |
| `0002_create_extraction_schema.sql` | `extraction_cursor`, `extraction_run`, `alarm_known_open_alarm`, audit trigger |
| `0003_create_raw_fanout_schema.sql` | schema `control`: `raw_artifact_dispatch`, `raw_artifact_batch`, `raw_fan_out_dead_letter`, audit triggers, lifecycle and retention functions |

The scripts use plain `CREATE` (no `IF NOT EXISTS`), so they fail loudly instead of silently skipping a table that already exists in another shape. They are not an upgrade path for an existing database. Worker startup never runs DDL. `src/db/schema/*.sql` are earlier design baselines and are not part of this deployment.

## Table ownership

| Table | Granularity | Stored payload | Writers |
|---|---|---|---|
| `control.raw_artifact_dispatch` | one accepted logical parent (a file, several shards, or an extraction-ready set) | `original_envelope` JSONB (parsed, not byte-identical); frozen `expected_batch_count`; `fan_out_completed_at_utc`, `harmonization_completed_at_utc` | `raw_artifact_save_plan`, `raw_artifact_mark_dispatched`, `raw_artifact_ack`, retention cleanup |
| `control.raw_artifact_batch` | one outgoing child batch | `message_body` (stable replay payload), `batch_ordinal`, `materialized_at_utc` (Harmonizer applied the work; NOT "published") | same functions |
| `control.raw_fan_out_dead_letter` | one archived SQS message (`queue_url` + `message_id`) | `original_body` verbatim, best-effort nullable business metadata, `last_error`/`error_type` only from evidence | `RawFanOut.Worker` DLQ consumer, replay/resolve commands, dead-letter retention |

The outgoing plan exists only in `raw_artifact_batch`; the parent holds the incoming envelope once. Table comments in `0003` repeat this. There is a single failure store: `raw_fan_out_dead_letter`.

## Audit columns

`created_date_utc`, `updated_date_utc`, `inserted_by`, `updated_by` exist on the three tables. A trigger pair stamps them: INSERT sets both dates from one clock value and `updated_by := inserted_by`; an UPDATE that names `updated_by` and changes business data stamps `updated_date_utc`; creation fields are immutable (attempts are rejected); a business change without an explicit `updated_by` is rejected, and a no-op update never re-attributes the row. Lifecycle functions take a required trailing `actor` (`raw-fanout-worker`, `raw-fanout-dlq-worker`, `harmonizer`, `operator:<name>`); the actor is passed explicitly, never taken from the DB login or session state, so pooled connections cannot leak it. The trigger guards against stale writers; it is not a security boundary. Audit columns record only creation and the latest change; use correlated logs for intermediate attempts.

## Deployment order

1. Apply `0001`, `0002`, `0003`, then the `PostDeployment` seeds.
2. Deploy `RawFanOut.Worker`, then a Harmonizer that calls `raw_artifact_ack(artifact_id, batch_id, actor)` with its own identity. No caller of the ack function exists in this repository; the Harmonizer integration is external and unverified here.
3. Apply `EXECUTE` and table grants (below) with the database owner.

## Dead-letter archive

Archiving is idempotent per `(queue_url, message_id)`: an identical re-delivery refreshes `last_observed_at_utc` only (not a processing-attempt counter) and never clears resolution; different content for the same identity raises `DeadLetterIntegrityException`. Malformed messages archive with the metadata columns empty. `last_error`/`error_type` are set only when the worker has evidence (an envelope validation failure); the SQS DLQ does not carry the original processing exception. The SQS receipt is deleted only after the archive commit.

Replay republishes and records `replay_requested_at_utc`; it does not resolve. Resolution is the explicit `--resolve <id> --reason <text> --operator <name>` command (the operator name is asserted by the caller; Kubernetes RBAC controls who can run it). Verify recovery before closing, for example by checking that the incident's `artifact_id` has `harmonization_completed_at_utc` set in `raw_artifact_dispatch`.

## Retention

Artifact ledgers: six calendar months after `harmonization_completed_at_utc`, only when the child count equals `expected_batch_count` and every child is materialized (`raw_artifact_cleanup_batch`). Resolved incidents: `ResolvedIncidentRetentionMonths` (default 6, assumption to confirm) after `resolved_at_utc` (`raw_fan_out_dead_letter_cleanup_batch`); unresolved incidents are never deleted. Both run in the `--cleanup` mode with bounded, `SKIP LOCKED` batches. Deleting a ledger removes that layer of deduplication history, so old replays still need downstream idempotency and source-version safeguards. Raw Parquet retention is unchanged.

## Grants

Only trusted service code may call the mutation functions. All functions are SECURITY INVOKER. Review these example grants with the database owner; scripts do not create roles or credentials:

```sql
GRANT USAGE ON SCHEMA control TO ingestor;
GRANT SELECT, INSERT, UPDATE ON control.raw_artifact_dispatch,
 control.raw_artifact_batch, control.raw_fan_out_dead_letter TO ingestor;
GRANT EXECUTE ON FUNCTION control.raw_artifact_save_plan(text,text,integer,text,bigint,jsonb,jsonb,text),
 control.raw_artifact_mark_dispatched(text,text), control.raw_artifact_ack(text,text,text) TO ingestor;
-- Retention cleanup role (SECURITY INVOKER functions delete rows):
GRANT SELECT, DELETE ON control.raw_artifact_dispatch, control.raw_artifact_batch, control.raw_fan_out_dead_letter TO ingestor;
GRANT EXECUTE ON FUNCTION control.raw_artifact_cleanup_batch(timestamptz,integer),
 control.raw_fan_out_dead_letter_cleanup_batch(timestamptz,integer) TO ingestor;
```

A separate Harmonizer role should receive the corresponding required table/function permissions (at least `EXECUTE` on `raw_artifact_ack`, plus the table privileges its SECURITY INVOKER body needs). Only the retention cleanup role needs `DELETE`; no runtime DDL is required. Direct edits to plans or child counts are unsupported: functions enforce the application invariants.

Harmonizer transaction pattern (client placeholders are illustrative):

```sql
BEGIN;
SELECT artifact_id FROM control.raw_artifact_dispatch
 WHERE artifact_id = :artifact_id FOR UPDATE;
-- Acquire component locks/fences in a consistent order.
-- Commit all required component pointers/input state, including lifecycle cleanup.
SELECT control.raw_artifact_ack(:artifact_id, :batch_id, 'harmonizer');
COMMIT;
```

Read `fan_out_completed_at_utc` for dispatch completion, and `harmonization_completed_at_utc` for the whole artifact's processing completion. Neither is a substitute for Item-Day input/version checks when creating a calculation package.
