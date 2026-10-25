# Raw ingestion control tables and publication

> Updated: 2026-10-12 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## 1. Responsibility

Raw stores immutable source capture files. PostgreSQL stores small extraction scheduling/execution records, not another copy of all Raw business rows. Pull sources use `extraction_cursor` and `extraction_run`. Adjustment uses transactional business writes/outbox and extraction run evidence; it normally does not use a polling cursor (DL-004).

Evidence limitation: current Ingestor and operation CREATE TABLE migrations were not supplied for this task. Adjustment DDL has since been inspected; its concrete schema and references are documented in [Adjustment](adjustment.md). The following inventory is a logical contract recovered from existing Discovery/decisions. Exact SQL column names/types and declared FKs must be checked against current migration files. Saved Fan-Out/Harmonizer DDL is documented separately.

## 2. Table ownership, keys and references

| Table | Logical key | Parent/reference relationship | Writer |
|---|---|---|---|
| `extraction_cursor` | Per source + windfarm; exact source-system/stream discriminator unresolved | Windfarm/source catalog associations; `active_run_id` associates current run | Ingestor scheduler/runner |
| `extraction_run` | `run_id` | Source/windfarm association to cursor; `operation_id` correlates root operation | Ingestor |
| `operation` | One root operation identity; actual DDL unresolved | Runs and downstream records carry its ID | Centralized API for API-triggered operation; Ingestor for independently scheduled incremental operation |
| Adjustment business/outbox tables | See Adjustment specification for inspected package keys | Current parent → WTG child FK; retained stream → immutable revisions → outbox, detailed in Adjustment | Business API/dispatcher |

These are logical associations; no FK is claimed without inspected DDL. Keep numeric windfarm/WTG IDs distinct from source names. No environment column: each environment has a separate database.

### 2.1 Operation and run cardinality

- One `operation` can reference one or more `extraction_run` rows. For a standalone scheduled incremental run, normally one operation corresponds to one run.
- One `extraction_cursor` identifies a specific source/windfarm polling stream. It is reused for sequential runs rather than inserted once per execution.
- `extraction_run.operation_id` is the authoritative persisted link to the root operation. `extraction_cursor.active_run_id` identifies the active run; an optional `extraction_cursor.operation_id` would only duplicate information and must not be treated as the source of truth.
- `run_id` and `operation_id` are different identifiers. `run_id` identifies Ingestor execution evidence; `operation_id` correlates that execution with downstream processing and potentially other source runs.

```text
operation (operation_id PK)
    1 ──────────────── * extraction_run (run_id PK, operation_id reference)
                            * ──────────────── 1 extraction_cursor
                                                  (source_type + windfarm_id +
                                                   optional source discriminator)

extraction_cursor.active_run_id ──> current extraction_run.run_id
```

## 3. Cursor contract

| Field group | Required meaning; candidate names from prior design |
|---|---|
| Identity/configuration | `source_type`, `windfarm_id`, enabled flag, polling interval/backoff |
| Committed checkpoint | `last_committed_updated_at` plus source-specific stable tie-breakers |
| Lease | `lease_token`, `lease_expires_at`, `active_run_id` |
| Scheduling | `next_eligible_utc`, last started/completed times |
| Outcome | Consecutive failures, last outcome/error |

Never-run cursors are eligible immediately. A successful cursor becomes eligible after its interval; a failed cursor after backoff. Expired leases may be recovered; active leases exclude another owner. Run Now bypasses normal eligibility time, not active ownership/fencing. The discussed lease duration is 10 minutes; verify actual configuration and renewal behavior.

Production and Outage have different continuation keys. Outage uses classification ID with updated time; Production routes can page by updated-time/WTG/start or business-time/WTG. Do not create one generic numeric tie-breaker that loses source identity. Separate live/deleted Outage continuation must avoid advancing past unprocessed tombstones; one-vs-two cursor rows remains a migration/runner check.

## 4. Run contract

A run records the claimed execution: source/windfarm, operation correlation, mode, attempt, start/completion, frozen query scope and from/to checkpoint. Candidate prior names include `execution_mode`, `query_start_inclusive_utc`, `query_end_exclusive_utc`, `attempt_count`, `operation_id`, `started_utc`, `completed_utc` and `status`.

A run should make an extraction reproducible: which window, selected membership, source query route, exact Raw outputs and publication outcome. Preserve failure evidence independently of the cursor's current state. These are specification requirements, not proof every field is currently persisted.

## 5. Data creation and commit sequence

### 5.1 Scheduled incremental: table-by-table flow

**Ownership rule:** A scheduled incremental run has no incoming Centralized API `operation_id`. Once an eligible cursor is claimed, the Ingestor registers a new root operation and writes that returned ID into `extraction_run` before processing source data. The `operation` table exists to issue the ID, not to replace the cursor/run tables as the execution state machine.

1. **Select cursor:** the Ingestor scheduler queries `extraction_cursor` for an enabled, eligible `(source_type, windfarm_id[, discriminator])` stream using its checkpoint and `next_eligible_utc`.
2. **Claim cursor lease:** atomically acquire a lease/fencing token; competing pods must not claim the same active cursor. If the lease cannot be acquired, do not create a new operation.
3. **Register operation:** insert a root row into `operation` and get its generated `operation_id` (for example, `5001`). This step is performed by the Ingestor for a new independent scheduled run.
4. **Create extraction run:** insert one `extraction_run` row (for example, `run_id=9001`) with `operation_id=5001`, `execution_mode='incremental'`, source/windfarm, frozen checkpoint/query scope, start time and running status.
5. **Associate cursor:** update `extraction_cursor.active_run_id=9001` under the matching lease token. No `operation_id` column is required on the cursor; monitoring can obtain it by joining `active_run_id` to `extraction_run`.
6. **Commit registration atomically:** registering operation, run and active-run association should be one database transaction after (or as part of) a correctly fenced lease claim. Roll back if the association fails or the lease is no longer owned. Generated numeric IDs can have harmless gaps after rollback.
7. **Extract and publish:** query incremental source data from the captured checkpoint, write/finalize immutable Raw artifacts, then send the Raw SQS envelope carrying **the same** `operation_id=5001` and `run_id=9001`. Fan-Out, Harmonizer and later stages propagate `operation_id` unchanged.
8. **Finish safely:** after durable artifact completion and successful handoff, conditionally advance the cursor checkpoint, record completion/outcome on `extraction_run` and release/clear `active_run_id` using the current lease/fencing token. On failure record run evidence, backoff, and retry/recovery state without advancing an uncommitted checkpoint.

Example logical database state (illustrative IDs, not production data):

| Phase | `operation` | `extraction_run` | `extraction_cursor` for `production` / farm `101` |
|---|---|---|---|
| Before next run | No new row | Past run rows retained | Checkpoint persisted; `active_run_id=NULL` |
| After registration | `operation_id=5001` inserted | `run_id=9001`, `operation_id=5001`, `mode=incremental`, `status=running` | Lease held, `active_run_id=9001` |
| After successful publication | Row `5001` retained | Run `9001` completed with publication evidence | Checkpoint committed; lease/active run cleared |
| Next independent scheduled run | New `operation_id=5002` | New `run_id=9002`, linked to `5002` | Same cursor row reused |

Illustrative transaction pseudocode (schema field names and `operation` identity implementation require verification):

```sql
-- Acquire/verify exclusive fenced cursor ownership first.
BEGIN;

-- Confirm this transaction still owns the expected lease/fence.
-- Example: SELECT ... FOR UPDATE, checking lease_token and expiry.

INSERT INTO operation DEFAULT VALUES
RETURNING operation_id; -- :new_operation_id

INSERT INTO extraction_run (
    operation_id, source_type, windfarm_id,
    execution_mode, status, started_utc
) VALUES (
    :new_operation_id, :source_type, :windfarm_id,
    'incremental', 'running', NOW()
)
RETURNING run_id; -- :new_run_id

UPDATE extraction_cursor
SET active_run_id = :new_run_id,
    last_run_started_utc = NOW()
WHERE source_type = :source_type
  AND windfarm_id = :windfarm_id
  AND lease_token = :lease_token;

-- Require exactly one updated cursor row, otherwise ROLLBACK.
COMMIT;
```

The SQL illustrates relationship and ordering only; actual implementation may use a stored procedure and additional discriminator/lease checks. This does **not** make PostgreSQL + SQS atomic.

### 5.2 API-triggered refresh / grouped request

1. Centralized API creates **one** `operation` row before dispatch, and passes its `operation_id` to the Ingestor(s).
2. Each Ingestor claims the necessary cursor/work boundary and creates its own `extraction_run`, referencing the **existing** `operation_id`; it **must not generate another operation ID** for that request.
3. If one API request refreshes several source types or windfarms, several `extraction_run` records share one operation ID, allowing unified progress tracking.
4. Each run emits Raw messages with both its own `run_id` and the shared `operation_id`.

| Trigger | Who creates `operation_id`? | `extraction_run.operation_id` | Cardinality |
|---|---|---|---|
| Independent scheduled incremental | Ingestor | New generated ID | Normally one operation per new run |
| API-triggered refresh | Centralized API | Reuse request's existing ID | One operation may have many runs |
| API-triggered Run Now/incremental | Centralized API | Reuse request's existing ID | One operation may have many runs |
| Retry/recovery of the same logical run | No new operation | Reuse original ID | Attempts stay correlated to original operation |

### 5.3 Retry, lease expiry, and transaction boundaries

- Retry within the **same logical extraction** must reuse the existing `operation_id` and frozen scope. Whether a retry updates the same `extraction_run` with a new attempt or creates a linked run row is an implementation/migration decision; either way retain operation correlation.
- A newly scheduled execution after a prior independent execution has finished is a **new** logical operation and gets a new ID, even if it extracts the same data window due to an unchanged checkpoint.
- Expired lease recovery must fence the previous owner. No losing worker may publish a success checkpoint or overwrite current run ownership.
- Registration of operation/run/cursor associations should be a PostgreSQL transaction. Artifact writes and SQS sends occur outside that transaction; use stable artifact identity, idempotent handoff and recovery logic.

### 5.4 Shared publication sequence

1. Resolve or generate the root OperationId according to trigger ownership above.
2. Claim a lease atomically and create run evidence for the frozen scope.
3. Read source pages and stream immutable Raw files. Incremental/refresh mode determines query axis and windowing.
4. Finalize all declared parts. For shared refresh, produce a complete `RawSnapshotManifestV1` and pointer notification.
5. Publish ready evidence to the Raw queue. A multi-part extraction can have one logical completion notification.
6. Advance committed checkpoint and finish the run only under the current lease/fence after durable Raw and successful handoff.
7. Fan-Out accepts the envelope and freezes downstream batches. Later stages retain the same operation correlation.

Database and SQS are separate systems. A crash after send and before checkpoint commit can duplicate handoff. The exact ingestion recovery/outbox implementation must be verified; do not claim a distributed atomic transaction. Stable artifact identity and immutable paths support duplicate-safe recovery.

## 6. Raw message and manifest contract

Aligned deliveries use Raw envelope version 2 and active payload schema 2. Include source, numeric farm identity/name, execution/run identity, extraction token, mode/query axis, exact file or manifest path and opaque OperationId. Never infer cross-stage order from SQS MessageId.

Refresh validation includes complete scope, frozen turbine roster, completed intervals, every required part, total/per-part row counts and pointer/manifest identity consistency. Explicit empty scope is data; missing files or failed source reads are errors. Manifest completion does not confer snapshot absence authority.

Paths remain source-owned examples, not a new globally renamed standard: Production uses the existing `mode=refresh` example; Outage retains events/deleted-events branches. Exact incremental mode path spelling remains to be reconciled with actual writers.

## 7. Recovery and open questions

Expired owners cannot commit. Retry must preserve the original operation and scope unless it is deliberately a new business request. Failed file writes must not advertise completion. Old Raw must survive replay/lineage retention requirements.

Q-002: verify cursor PK, source discriminators, live/deleted checkpoints and run statuses. Q-003: inspect actual stage records. Q-012: finalize retention; the earlier one-month suggestion is not an accepted universal deletion policy. Verify checkpoint failures before/after send, lease expiry, concurrent Run Now, equal-timestamp pages, explicit empty refresh and outbox replay. Also verify `operation` identity generation DDL, whether cursor has a redundant `operation_id`, and whether retry uses the same run record or a linked run.

## Adjustment implementation cross-reference

The supporting outbox/revision SQL does not itself implement the later dispatcher/ingress deployment boundary or persist root OperationId. Preserve DL-004 while aligning those gaps (Q-015). Do not use a polling cursor as the outbox retry boundary. See [Adjustment](adjustment.md).
