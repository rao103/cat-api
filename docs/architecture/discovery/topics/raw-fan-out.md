# Raw Fan-Out tables and work planning

> Updated: 2026-10-11 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## 1. Scope and contracts

Raw Fan-Out consumes immutable Raw notifications, resolves/validates identity and capture evidence, freezes a deterministic outgoing plan and sends its child batches. Core holds planning/providers; Worker holds long polling, hosting, telemetry and recovery services. Preserve both projects and the current ITelemetry boundary.

Evidence: saved `0003_create_raw_fanout_schema.sql` plus aligned `FanOut.zip`. Accepted baseline: DL-013. Production/Curtailment use daily-series work; Outage/Alarm/Adjustment use event/member work. Alarm/Adjustment adapters remain subject to real contract verification.

## 2. Saved physical tables

| Table | Primary/unique keys | Foreign keys | Purpose |
|---|---|---|---|
| `control.raw_artifact_dispatch` | PK `artifact_id` | None declared in saved CREATE | One logical accepted Raw envelope and parent progress |
| `control.raw_artifact_batch` | PK `(artifact_id,batch_id)`; UNIQUE `(artifact_id,batch_ordinal)` | `artifact_id` → dispatch PK | Frozen outgoing serialized work and final materialization ACK |
| `control.raw_fan_out_dead_letter` | PK `incident_id`; UNIQUE `(queue_url,message_id)` | None; nullable artifact link is logical | Original DLQ body, recovery evidence and resolution |

`fan_out_failure` appears in older inventories. It is not created by this saved 0003 baseline; do not add it or document a physical FK merely because it appeared in historical discussions. Check current forward migrations before declaring it absent from a deployed database.

### Parent dispatch fields

`artifact_id` is a 64-character lowercase hexadecimal identity. Parent data includes source type, integer windfarm ID, `run_id`, nonnegative extraction token, parsed `original_envelope` JSONB, positive expected batch count, planning time, Fan-Out completion and harmonization completion.

`original_envelope` preserves parsed JSON, not byte-for-byte transport text. Correlation such as OperationId is retained in this JSON; the saved baseline has no standalone dispatch OperationId column.

### Child batch fields

Each batch stores artifact/batch IDs, ordinal, exact `message_body` text and `materialized_at_utc`. Its message is the durable replay body. `materialized_at_utc` means Harmonizer applied the batch, not Fan-Out sent it.

### Dead-letter fields

Archive verbatim `original_body` even if malformed, transport attributes, artifact paths, optional parsed identities, first/last observation, error evidence, replay request and verified resolution. Replay requested is not resolved. Nullable parsed metadata must remain unknown if parsing failed.

All three tables use `created_date_utc`, `updated_date_utc`, `inserted_by`, `updated_by`; saved audit triggers require explicit actors and preserve attribution/timestamps on no-op updates.

## 3. Planning and data flow

1. Parse and validate Raw envelope/source/schema/operation metadata.
2. Resolve exact parts. Shared refresh validates manifest identity, frozen scope, completeness and row counts.
3. Read identity columns and resolve legacy source names through the catalog.
4. Build the full deterministic batch plan and persist parent plus child records before sending.
5. Send stored child bodies; retry uses the same IDs, ordinal and body, rather than replanning from changed catalog membership.
6. Mark dispatch completion after required sends. Duplicate sends remain possible under at-least-once delivery.
7. Harmonizer records receipts and final batch acknowledgment. Parent harmonization completion requires the expected children; it is a separate boundary from dispatch completion.

Saved empty observations/scope barriers ensure a real logical extraction can still produce explicit receipt work; expected batch count is positive. Do not model an empty source read as a silently missing parent.

## 4. Source work shapes

| Source shape | Frozen work | Owner of affected-day planning |
|---|---|---|
| Production | `ProductionItemDayWorkV3`; numeric IDs, separate names, exact parts/date/scope | Fan-Out for daily-series partitioning |
| Outage | EventWorkV3 canonical members and any snapshot/observation work | Harmonizer using old/new canonical footprints |
| Curtailment | Daily-series direction; inspect actual adapter coverage | Daily-series route |
| Alarm / Adjustment | Event direction; real mappings required | Harmonizer lifecycle after contract alignment |

Production V2 remains a compatibility reader; existing frozen V2 bodies must not be silently transformed into V3. Prefer WTG in new vocabulary while preserving existing contract type names until a compatible migration exists.

## 5. Refresh and ownership rules

Active schema 2 carries numeric IDs and names. Legacy schema 1 numeric-looking source names are still names. A single Outage event ID cannot belong to multiple WTGs within the batch.

Aligned shared Outage refresh emits ReceiptOnly completion. Omitted events are not deleted. Existing authoritative processing requires source-version evidence; see [event management](harmonizer-event-management.md). Fan-Out cannot recover old event cleanup dates using only the new row.

## 6. Recovery and retention

Use stored exact child bodies for redrive. Archive a DLQ delivery before transport deletion. Separate operator replay request from verified business recovery. Parent/batch cleanup must respect pending harmonization; unresolved incidents and referenced Raw/lineage must remain recoverable.

Saved `raw_artifact_ack` is used by Harmonizer. There is deliberately no FK from Harmonizer batches to this shorter-lived ledger. Harmonizer recovery receipts have their own lifetime. Final retention duration remains Q-012; do not infer a new one-month policy.

## 7. Verification

Check multi-page parts, duplicate parent delivery, crash after child send, deterministic catalog membership, malformed DLQ body, row-count mismatch, empty manifest, OperationId conflict, delayed/out-of-order event batches, failed child and exact redrive. No compilation, live SQS/storage or PostgreSQL execution was performed for this specification.
