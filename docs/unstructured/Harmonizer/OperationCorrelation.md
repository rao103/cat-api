# Operation correlation in Harmonizer

The upstream business trigger creates one `OperationId` for a scheduled run, refresh,
backfill, adjustment, reprocess or restore. It supplies the corresponding `ExecutionId`
for each operation/windfarm/source execution. All turbine/day batches in that execution
keep the same IDs. Harmonizer neither generates these identifiers nor substitutes
`RequestId`, `AttemptId`, `TraceId`, artifact IDs or batch IDs for them.

## Envelope and compatibility

Both Production V2 and V3 accept additive PascalCase string fields:

```json
{
  "OperationId": "OP-001",
  "ExecutionId": "EXEC-001"
}
```

Add these fields to the existing work envelope; this fragment is not a complete work
message. They may also be carried in the JSON envelope inside `OriginalMessageBody`.
A missing/null top-level field uses the embedded value. When both are present they
must agree. Duplicate, non-string, blank, oversized or control-character identifiers
fail validation. IDs are opaque strings, at most 256 characters; no UUID format is
assumed. Either field can be missing during gradual rollout. Legacy messages with
neither remain untracked. `OriginalMessageBody` must contain a JSON object.

The supplied shared contract classes remain unchanged. An envelope reader extracts
metadata separately; the original SQS body remains unchanged for exact frozen Fan-Out
plan matching. Producers must persist the same body in their dispatch ledger before
sending it. Adding fields only after dispatch-plan creation will fail frozen-body
validation. A new business operation needs its own upstream artifact/batch identity;
retagging an already registered batch is rejected.

## Where the IDs go

| Boundary | Representation |
|---|---|
| Internal Production work and component commit | Shared `OperationIdentity` object |
| Validated reader, catalog normalization, batch and queue-delivery logs | Structured `OperationId` / `ExecutionId` scopes |
| Consumer, batch and notification producer spans | `cat.operation.id`, `cat.execution.id`, `cat.pipeline.stage` |
| Batch span | Also source type, farm ID/name and batch ID under `cat.*` |
| New row origins and snapshot contributions | Both IDs alongside original raw path, extraction token, request and attempt |
| New lineage manifest | Triggering IDs at the root; contributing rows retain their own origins |
| `control.harmonizer_batch` | Nullable `operation_id`, `execution_id`, frozen with registration |
| `control.wtg_day_input_state` | Nullable `last_operation_id`, `last_execution_id` on commit/failure |
| `control.harmonizer_outbox` | Nullable IDs committed atomically with component state and receipts |
| Outgoing coordination/summary JSON | `OperationId`, `ExecutionId`, `SourceType`, `PipelineStage` |
| Notification publishing logs/spans | IDs from the durable outbox row |

On fresh databases, the consolidated Harmonizer 0004 baseline includes operation
correlation. Existing deployments need a forward migration before running this version.
Existing rows remain null. An exact-body registration replay can fill
previously null batch correlation after validation; it cannot change populated IDs.
Current-state columns describe the latest commit or recorded failure, not a complete
operation history. Durable batch records, immutable lineage and telemetry provide
additional movement evidence. Parse failures before validated metadata is available
can only carry delivery/message context.

## Reused data, retries and observability

Business correlation does not alter source-version comparison, canonical data hashes
or revisions. A tracked operation consuming unchanged healthy data retains the old
revision/artifact and queues its own `PackageCoordinationRequested` notification.
It does not invalidate an unchanged summary. Changed data or recovery from Failed
state queues both existing notification kinds. Legacy untracked healthy no-ops retain
the previous behavior of emitting neither.

Tracked notification deduplication includes a hash of OperationId/ExecutionId, as well
as the existing kind/component/date/revision identity. Retries of that operation remain
idempotent; another operation reusing the revision receives a separate notification.
Delivery remains at least once. Consumers must deduplicate NotificationId and recheck
current component readiness and generations. Partial materialization is not calculation
readiness or whole-operation completion.

An unchanged component keeps its immutable historical lineage; its current triggering
operation is recorded in batch/state/outbox evidence. Existing row origins are never
rewritten just to attach a new operation. Operation IDs are business search attributes,
not W3C traceparent values. Metric dimensions do not include these unbounded IDs.

## Integration still required

This implements correlation in Production and Outage Harmonizer paths. The
central API still owns ID creation and operation/execution tracking. No central tracking
tables, status API, progress aggregator or completion authority are invented here.
The final central schema/API must be agreed before adding those writes. Upstream
services must supply the metadata and downstream coordination/calculation/publishing
services must continue it. Outage queue processing, canonical persistence and lifecycle
materialization use the shared identity; downstream consumer adoption remains pending.


## Outage lifecycle integration (6 October 2026)

EventWorkV3 correlation is validated against OriginalMessageBody and carried through
batch rows, canonical origins, range plans, daily lineage, scopes/activities and
coordination notifications. SourceWatermarkUtc is origin/capture evidence, not business
content or operation identity. A canonical no-op still retains the incoming operation in
its accepted batch receipt. Clock continuation creates untracked autonomous plans and
keeps original event origins as provenance; it does not reopen a completed user operation.
Central stage/progress API writes remain outside this component.
