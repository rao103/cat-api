# Centralized API — Discovery

**Updated:** 2026-10-12 (Asia/Kuala_Lumpur)  
**Repository target:** `docs/architecture/discovery/loss-calculation/centralized-api.md`  
**Status:** Discovery and proposed implementation contract; no API code, migration or deployment has been verified.  
**Architecture relationship:** Supplements the V13 baseline. This document does not amend V13 or mark proposals as accepted architecture decisions.

## 1. Purpose and scope

Centralized API provides the operational entry point for requesting extraction and reading progress across the Loss Calculation Platform. It records durable requests, returns an operation identity, and exposes execution evidence. Ingestors perform source extraction; downstream workers perform Fan-Out, harmonization, calculation and publication.

The immediate scope is two refresh endpoints:

1. Refresh one windfarm for a date range across all refresh-supported source types configured for that windfarm.
2. Refresh one windfarm and one source type for a date range.

Both use the same request-registration service. Their difference is source selection. Requests are asynchronous: the HTTP response confirms durable acceptance, not extraction or pipeline completion.

Run Now/incremental control and bringing forward an earlier version are related platform capabilities, but their exact endpoints and implementation are deferred here. Adjustment creation/edit/delete remain the responsibility of the Business Web API and transactional outbox.

## 2. Established direction and proposed refinements

| Topic | Status | Contract or finding |
|---|---|---|
| Two refresh endpoint scopes | User requirement | Windfarm + range; windfarm + source + range. |
| Shared operation identity | Established discussion direction | One API request creates one operation identity; all selected runs and downstream work carry it. |
| Minimal operation table | Established discussion direction | Generates/registers identity; scope and execution state belong to run/stage records. |
| Cursor/run separation | Current ingestion specification | Cursor is reused for scheduling/checkpoint/lease; run is execution evidence. Cursor does not need a duplicate operation ID. |
| API inserts Queued runs | Proposed in this discussion | Reuse extraction_run as durable request storage; avoid introducing another request table. |
| Ingestor claims pre-created run | Proposed consequence | Refresh runner updates the existing queued run instead of inserting another run at claim time. |
| Shared refresh/incremental lease | Proposed | Serialize extraction for the same source/windfarm ownership boundary; allow different sources to run concurrently. |
| Operation ID physical type | Unresolved | Prior raw-ingestion examples use numeric IDs; this discussion proposed UUIDs. Check current DDL and message contracts before choosing. |

The current `raw-ingestion.md` describes Ingestor-created refresh runs after claim. Implementing API-created Queued rows requires aligning that topic specification, migrations and runner code. This is a visible proposed change, not evidence of existing behaviour.

## 3. API contract

Endpoint paths below are proposed names; the two required scopes are fixed.

| Method and path | Selection | Registration |
|---|---|---|
| `POST /windfarms/{windfarmId}/refresh` | All configured refresh-supported sources | One operation, one queued logical run per selected source. |
| `POST /windfarms/{windfarmId}/sources/{sourceType}/refresh` | One configured refresh-supported source | One operation, one queued logical run. |

Shared request:

```json
{
  "startDate": "2026-09-01",
  "endDate": "2026-09-30"
}
```

Recommended date convention: inclusive UTC calendar dates at the HTTP boundary. Persist the normalized half-open interval as `query_start_inclusive_utc=2026-09-01T00:00:00Z` and `query_end_exclusive_utc=2026-10-01T00:00:00Z`. Confirm this convention before implementation; do not depend on client-local timezone conversion.

Validation checks windfarm existence, caller authorization, valid ordered dates, supported source values, configured source membership and refresh availability. Define any range limit from actual source capacity rather than inventing an arbitrary limit. Reject a request that resolves to no executable sources.

For all-source refresh, resolve and freeze the selected source list during registration. Do not silently omit a configured source that cannot execute; expose the validation issue. Sources not configured for that windfarm are outside the selected set. Whether a polling-disabled cursor allows manual refresh needs an explicit policy.

Adjustment needs a separately defined refresh adapter because normal changes use the business outbox rather than a polling cursor. Do not report Adjustment as included unless that behaviour exists. Until resolved, document the endpoint as refreshing supported pull sources: Production, Outage, Alarm and Curtailment where configured.

Example accepted response (UUID is illustrative pending ID-type reconciliation):

```json
{
  "operationId": "84a3ecf8-9750-4a9a-9875-6a1f2c48b920",
  "status": "Accepted",
  "windfarmId": 12,
  "sourceTypes": ["Production", "Outage", "Curtailment"],
  "startDate": "2026-09-01",
  "endDate": "2026-09-30"
}
```

Return `202 Accepted` after committing registration, with a link to the operation resource. Suggested errors: `400` invalid contract/range/source, `401/403` authentication/authorization failure, and `404` missing windfarm or operation. An active cursor lease is normally queued work, not an HTTP conflict.

## 4. Database responsibilities

| Table | Key/reference | Centralized API responsibility | Ingestor responsibility |
|---|---|---|---|
| operation | operation_id PK | Insert once per accepted API request | Insert for independently scheduled incremental execution; reuse incoming identity for API requests. |
| extraction_run | run_id PK; operation_id reference | Proposed: insert Queued refresh rows with frozen source/farm/window | Claim existing refresh row; record attempts, execution and outcome. |
| extraction_cursor | Existing source/farm/stream key; active_run_id reference | Read configuration/ownership for validation and monitoring | Own lease, active-run association, checkpoint and incremental scheduling. |
| Downstream stage tables | Existing stage identities + operation_id | Read stage evidence | Corresponding downstream worker writes its own evidence. |

These are logical relationships. Exact PKs, FKs, column names and stream discriminators require migration inspection. Separate databases exist for Dev/Test/PreProd/Prod; no environment column is introduced.

### 4.1 Minimal operation record

Candidate structure discussed:

| Column | Candidate type | Meaning |
|---|---|---|
| operation_id | uuid, or existing generated numeric type | Immutable root correlation identity. |
| created_date_utc | timestamptz | Request/operation creation timestamp. |
| created_by | text | Authenticated caller identifier or worker identity. |

If UUID is selected, the application generates it and inserts it with the request transaction. If existing DDL uses generated numeric identity, use `INSERT ... RETURNING operation_id`. Use the same type consistently through database, API and messages; avoid migrating solely because an example used a UUID.

No operation status, source, windfarm, window, progress or environment columns are required. Use the project's existing audit naming convention when implementing; verify `created_by` versus `inserted_by` before creating a competing convention.

### 4.2 Queued extraction run

The logical minimum is run_id, operation_id, source_type, windfarm_id, execution_mode, normalized query window, status, creation audit, attempt_count, started_utc and completed_utc. Retain existing error/publication fields where present. A queued run has no start/completion timestamp and no execution lease yet.

Refresh scope is business/event time according to the source contract, not the incremental LastUpdated checkpoint axis. Source query and snapshot authority remain source-specific.

One API request creates one logical run per selected source. A source's live/deleted streams or internal refresh windows need not become separate root operations. Exact run cardinality for multiple physical stream cursors remains a DDL/runner check. Multiple Raw parts may belong to the same logical run.

## 5. Request registration and execution flow

### 5.1 Centralized API transaction

1. Validate caller, windfarm, sources and normalized date range.
2. Begin a PostgreSQL transaction and resolve/freeze selected configuration consistently.
3. Insert one operation record.
4. Insert all selected extraction_run rows with Refresh mode, Queued state and the same operation ID.
5. Commit; return 202 and the operation ID.

Any insertion failure rolls back the entire registration. Acceptance must not depend on a worker already being available. Do not acquire a cursor lease or update active_run_id/checkpoints at request time.

### 5.2 Ingestor claim transaction

The proposed first implementation discovers queued requests directly from PostgreSQL on the Ingestor's existing scheduling cadence. This avoids adding an API-to-Ingestor queue/outbox solely for a durable request that already exists in the database. A CronJob must check queued refresh runs; accepting a request does not imply immediate execution. Confirm required start latency and actual hosting mode.

For each selected source, the Ingestor atomically locks/revalidates a candidate queued run, claims its cursor/work ownership boundary and changes the run to Running. Set active_run_id, lease token/expiry, started_utc and attempt evidence within that transaction. If ownership is unavailable, leave the request queued. Use a consistent lock order and fencing checks across incremental and refresh claims; no pod may claim the same logical work twice.

The worker reuses the API operation ID and existing run ID. It must not create another root operation or another ordinary refresh run during claim.

### 5.3 Execute and finish

Read the source using the frozen refresh scope, write immutable Raw files, finalize required manifest/parts and publish the agreed completion notification to the Raw queue. All messages retain operation_id and run_id. Complete the extraction_run only after durable Raw completion and successful handoff according to the ingestion recovery contract.

Under the current fence, clear active_run_id and release the lease. Refresh does not advance/reset incremental committed checkpoints. Preserve incremental scheduling/backoff state; confirm whether existing last-run timestamps/outcome are incremental-only or describe all executions before reusing them for refresh.

PostgreSQL, file storage and SQS do not share a transaction. Publication recovery needs stable artifact identity and duplicate-safe downstream handling; this Discovery does not claim an implemented outbox or exactly-once delivery.

## 6. Concurrency, retries and duplicate requests

| Situation | Proposed behaviour |
|---|---|
| Incremental already owns same source/farm | Refresh stays queued until ownership is available. |
| Different sources for same windfarm | May execute in parallel under separate ownership boundaries. |
| Two refreshes for same source/farm | Separate operations/runs; serialize through the same lease. |
| Worker fails before completion | Retain operation/scope and failure evidence; apply bounded retry/recovery. |
| Lease expires | Recover with a new fence; previous owner cannot commit or clear current ownership. |
| Same logical execution retries | Reuse operation identity; proposed approach reuses the run and increments attempt evidence. |
| User deliberately submits a new refresh | Create a new operation even for the same range. |

Queued → Running → Completed is the normal path. Running → Retrying → Running or Running → Failed are proposed failure paths; exact stored vocabulary must align with current migration constraints. Retrying needs a durable retry eligibility time or equivalent existing mechanism. It must not repurpose the incremental cursor backoff to represent refresh retry timing.

Overlapping refresh and later downstream processing require existing version/order rules. Serializing extraction alone does not prove older work cannot overwrite newer harmonized/serving results.

HTTP retries can otherwise create duplicate operations after a lost response. Before promising retry-safe submission, define an Idempotency-Key persistence contract with uniqueness and request-body comparison. Its durable storage is unresolved; do not claim the minimal three-column operation schema provides this. A repeated intentional request remains valid new work.

## 7. Operation monitoring

Proposed resource: `GET /operations/{operationId}`. Centralized API joins extraction_run and available downstream records by operation_id. Authorize access based on the operation's associated windfarm scope, not knowledge of its ID alone.

| Evidence | Meaning |
|---|---|
| Queued run | Request registered; extraction has not started. |
| Running/retrying run | Extraction execution/recovery is in progress. |
| Completed extraction run | Raw extraction and handoff completed. |
| Fan-Out receipt/batch evidence | Raw was accepted and downstream work registered. |
| Harmonizer source revision | Source data was harmonized for relevant scope. |
| Calculation/publication evidence | Calculation or serving publication completed at its own boundary. |

Show per-source and per-stage results, timestamps and failures. Missing stage evidence means no evidence is available; it does not prove completion or failure. Do not report overall Completed merely because all extraction runs completed. Different selected sources may finish independently; no automatic all-source calculation barrier is introduced by sharing an operation ID.

Compute overall state from the applicable stage contracts. Numeric progress needs a reliable expected work count; do not invent percentages from elapsed time or source count. Stage schemas and completion authority remain open verification work.

## 8. Related capabilities and component boundaries

| Capability | Responsibility/status |
|---|---|
| Run Now/incremental | Related control capability; bypass normal schedule eligibility but preserve active ownership. Exact request registration contract deferred. |
| Adjustment writes | Business Web API transaction + outbox; not implemented by refresh endpoints. |
| Bring forward earlier version | Later capability: read selected historical Harmonized state and submit it as new input through the ingestion flow. As-of authority and operation evidence must be specified first. |
| Historical queries | Query Engine/provider responsibility; Centralized API may expose orchestration/control, not implement every query provider here. |
| Source SQL and Raw formats | Source adapters and shared ingestion/message contracts. |

Deploy Centralized API as the stateless operational HTTP service using the platform's existing deployment/security conventions. Durable requests remain in PostgreSQL, allowing multiple API replicas. Emit logs/traces/metrics for request acceptance, rejection and reads with operation_id and relevant run/farm/source identifiers. Do not log credentials or source business payloads unnecessarily.

## 9. Validation required before implementation is declared complete

- All-source registration creates one operation and every selected run atomically; single-source creates one run.
- Invalid/unauthorized requests create no operation/run records.
- Two Ingestor pods cannot claim the same request; incremental ownership blocks refresh claim for that boundary.
- Queued scope survives an API restart and delayed worker start.
- Refresh success and failure preserve committed incremental checkpoints and scheduling semantics.
- Retry/lease recovery retains correlation and fences the previous owner.
- Raw messages retain shared operation_id and per-run identity; partial writes are not advertised complete.
- Monitoring distinguishes extraction completion from downstream and serving completion.
- Explicit empty source results follow source/manifest contracts; absence is not assumed to authorize event deletion.

These are implementation acceptance checks, not tests that were run during document generation.

## 10. Open questions and next implementation task

| ID (local to this document) | Question/action |
|---|---|
| CA-Q01 | Accept API-created Queued extraction_run rows and reconcile raw-ingestion ownership/flow. |
| CA-Q02 | Inspect operation/run/cursor migrations: ID types, status constraints, nullable queued fields, FK/stream keys, audit names and claim procedures. |
| CA-Q03 | Decide manual-refresh behaviour for polling-disabled sources and freeze all-source membership policy. |
| CA-Q04 | Confirm date convention, source-specific refresh query semantics and allowed request range. |
| CA-Q05 | Confirm worker cadence/start-latency target, refresh priority/fairness and durable retry eligibility mechanism. |
| CA-Q06 | Define Adjustment refresh inclusion and physical Outage live/deleted ownership boundaries. |
| CA-Q07 | Define durable HTTP idempotency storage if retry-safe submission is required. |
| CA-Q08 | Inspect stage evidence and completion authority for monitoring; reconcile overlapping refresh ordering. |

These local IDs are not additions to the canonical platform Q-001–Q-019 backlog. Relevant platform questions include Q-002 (cursor/run contracts), Q-003/Q-013 (stage evidence/correlation) and Q-015 (Adjustment integration).

Next Micro-RPI: inspect current migrations and Ingestor request/claim code, resolve CA-Q01/02, then implement the two endpoints and queued-run claim flow with the necessary database changes and integration checks. Do not introduce an additional request table unless inspected constraints make extraction_run unsuitable.

## 11. Evidence and document alignment

Sources consulted: current saved `raw-ingestion(3).md` and `platform-discovery(1).md`, plus the visible 12 October discussion of refresh endpoints and minimal operation schema. V13 and active repository code were not inspected directly for this document. Numeric-vs-UUID examples and Ingestor-vs-API run creation are explicitly unresolved.

When accepted and implemented, align repository `raw-ingestion.md`, `operation-tracking.md`, the Discovery index and Decision Log with this contract. Keep source query detail in the relevant source specification and executable schema in migrations. Existing companion documents and V13 were not modified by this deliverable.
