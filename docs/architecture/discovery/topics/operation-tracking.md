# Operation ID propagation and tracking

> Updated: 2026-10-11 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## 1. Agreed model

One top-level business trigger receives one opaque OperationId. One lightweight `operation` table generates/identifies it; existing component records own scope, status and progress. Do not add `operation_work` or `operation_execution` simply to duplicate those records (DL-002).

OperationId is correlation, not a lease, revision, attempt counter or trace ID. A retry of the same business request retains it. A deliberate new refresh/restore request receives a new operation. Exact operation DDL and ID type remain unverified; downstream inspected tables use nullable text correlation for legacy compatibility.

## 2. Identity mapping

| Identifier | Owner and meaning |
|---|---|
| OperationId | Root business/control request across stages |
| `extraction_run.run_id` | Specific ingestion execution evidence |
| ExecutionId | Existing message/telemetry execution metadata; reconcile actual mapping to run ID rather than assuming equivalence |
| ExtractionToken | Fenced capture/extraction identity |
| ArtifactId / BatchId | Durable Fan-Out plan and child work |
| EventId / MemberKey | Business event and WTG applicability |
| Revision / PlanningGeneration | Published source state and pending rebuild version |
| Lease/Attempt | Ownership of a write attempt |
| MessageId / trace ID | Transport delivery / telemetry execution |

## 3. Correlation propagation

Control trigger → extraction run → Raw envelope/manifest → frozen Fan-Out children → Harmonizer batch/receipts/lineage → outbox coordination intent → package/calculation/publication records once those consumers are implemented.

Downstream components retain upstream OperationId and reject conflicting pointer/manifest/work metadata. Harmonizer does not invent an ingestion OperationId. Legacy messages without it remain untracked rather than receiving a misleading new root correlation.

The aligned Fan-Out preserves correlation inside existing original envelope JSON and exact child message text; no new standalone dispatch column is claimed. Harmonizer has indexed `operation_id`/`execution_id` on batch/outbox and last IDs on daily state. Last IDs identify the latest update, not all historical work for an operation.

## 4. Stage evidence

| Stage | Evidence source | Meaning / limits |
|---|---|---|
| Root request | Lightweight operation identity | Request exists; no execution inferred |
| Ingestion | extraction_run plus cursor/control evidence | Claimed/running/succeeded/failed per actual runner; latest DDL unresolved |
| Fan-Out planned/dispatched | Dispatch + frozen batch records | Expected children and dispatch completion |
| Canonical accepted | harmonizer_batch canonical timestamp + member receipts | Durable event state/plans; daily work may remain |
| Harmonized materialization | Batch completion, materialized item receipts, lifecycle pending ranges | Required current generation completed through processable cutoff |
| Readiness | Daily state + pending function/view | Source eligibility, including absent Outage; different from receipt completion |
| Package/calculation | Actual coordinator/calculation records to inspect | Do not invent table names/statuses |
| Serving published | Actual Publisher commit/journal evidence to inspect | End-to-end business availability; not proved by outbox sent time |

A missing extraction run means execution not observed, not necessarily failed. A long-running event can be canonically accepted while materialization remains pending. A sent notification means transport send, not downstream completion.

## 5. Read-only queries supported by saved DDL

```sql
-- Bind :operation_id through the PostgreSQL client; illustrative parameter notation.
SELECT artifact_id, batch_id, source_type, work_kind,
       canonical_applied_at_utc, completed_at_utc, last_error_code
FROM control.harmonizer_batch
WHERE operation_id = :operation_id
ORDER BY created_date_utc, artifact_id, batch_id;

SELECT b.artifact_id, b.batch_id, i.member_kind, i.wtgid,
       i.member_key, i.lifecycle_work_id, i.planning_generation,
       i.applied_at_utc, i.materialized_at_utc
FROM control.harmonizer_batch b
JOIN control.harmonizer_batch_item i
  ON i.artifact_id = b.artifact_id AND i.batch_id = b.batch_id
WHERE b.operation_id = :operation_id;

SELECT id, deduplication_key, created_date_utc, sent_at_utc
FROM control.harmonizer_outbox
WHERE operation_id = :operation_id
ORDER BY id;
```

For Fan-Out, OperationId is in JSON/message bodies in the inspected baseline. Confirm exact serializer casing before building a JSON extraction query; do not assume a standalone column or use substring matching. Use artifact/batch links returned from Harmonizer to drill down into frozen Fan-Out records, retaining visibility of upstream-only operations via validated envelope lookup.

## 6. API/UI projection requirements

Centralized API reads existing stage evidence and returns root correlation, source/windfarm grouping, executions, expected/completed work, observed failures and last evidence time. Projection labels such as NotStarted/Running/Failed/Completed may be useful, but are derived API semantics to agree against actual records, not a requirement for new database heartbeat/status tables.

Show separate stage counters. Do not join parent batches and receipts and count duplicated rows as more completed work. Denominators can expand as a stage freezes its plan; no reliable overall percentage exists before the expected scope is known. A complete parent extraction is not a complete business operation. Explain pending/no-evidence directly instead of fabricating success or failure.

Completion requires actual publication evidence for the requested scope, not queue emptiness. Failures/retries should preserve original execution evidence. Stale superseded generations must not count as the current plan's finished work. Clock-driven future event continuation is separate ongoing maintenance, not an indefinitely unfinished original operation.

## 7. Telemetry

Use existing ITelemetry/OpenTelemetry logs, traces and metrics. Correlate OperationId, execution/run, source/farm/WTG, artifact/batch/member, attempt, extraction token and message ID where available. Preserve W3C trace propagation separately. High-cardinality IDs belong in logs/traces rather than metric labels.

## 8. Open questions and validation

Q-003/Q-013: obtain actual operation/Ingestor DDL, ExecutionId mapping, Publisher evidence and serializer shape. The standalone Production V2 contract may lack additive OperationId in one supplied file; verify shared Messaging.Contracts and consumer project references before claiming compilation compatibility.

Q-007/Q-008: validate canonical-versus-final completion and real downstream consumption. Required scenarios: multiple sources under one request, failed Ingestor with no Fan-Out, upstream-only operation, retry retaining correlation, legacy untracked work, mismatch rejection, partial/failure/recovery, duplicate send, superseded lifecycle generation and final serving completion.
