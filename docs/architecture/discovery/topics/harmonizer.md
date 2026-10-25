# Harmonizer tables and source publication

> Updated: 2026-10-11 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## 1. Responsibility

Harmonizer Core/Worker accepts Production daily work and Outage event work, merges source state, publishes immutable canonical/daily candidates, commits authoritative pointers and records durable receipts/outbox intents. Event lifecycle runs inside the same worker. It does not create calculation packages or dispatch CalculationWork; Package Coordinator owns those boundaries (DL-015).

Evidence: saved consolidated `0004_create_harmonizer_schema.sql` and aligned `Harmonizer.zip`. Preserve current project structure, telemetry, provider separation and Outage processor guards. See [event management](harmonizer-event-management.md) for lifecycle rules.

## 2. Physical tables and keys

| Table | Primary/unique key | References | Main role |
|---|---|---|---|
| `control.harmonizer_batch` | PK `(artifact_id,batch_id)` | No FK to Fan-Out ledger | Frozen work, expected members, correlation, canonical/final completion |
| `control.harmonizer_batch_item` | Partial UNIQUE `(artifact_id,batch_id,wtgid)` for WtgDay; partial UNIQUE `(artifact_id,batch_id,member_key)` for other kinds | Declared composite FK → harmonizer_batch | Applied/materialized member receipt; no separate PK declared |
| `control.wtg_day_input_state` | PK `(windfarm_id,wtgid,data_date,input_type)` | Logical business-key associations | Current daily pointer, completeness, lease and first failure |
| `control.harmonizer_outbox` | Identity PK `id`; UNIQUE `deduplication_key` | Logical association, no batch FK | Durable downstream notification intent |
| `control.event_partition_state` | PK `(windfarm_id,source_type,partition_key)` | Logical catalog/partition association | Canonical partition pointer and fenced ownership |
| `control.event_materialization_state` | PK `(windfarm_id,source_type,work_id)` | Logical partition/work and receipt association | Bounded pending ranges and open-event continuation |

Only the batch→item FK above is declared among these saved tables. Fan-Out→Harmonizer is shared artifact/batch identity, not a physical FK. Numeric column `wtgid` is existing DDL spelling; do not silently change it to `wtg_id`.

## 3. Column responsibilities

| Table | Important columns / meaning |
|---|---|
| Batch | `message_body`, `expected_wtgids`, `expected_member_keys`, `expected_event_batch_ids`, `work_kind`, `source_type`, `windfarm_id`, `operation_id`, `execution_id`, `canonical_applied_at_utc`, `completed_at_utc`, `last_error_code` |
| Item | `member_kind`, optional `wtgid`/`member_key`, `event_id`, `lifecycle_work_id`, `planning_generation`, `component_revision`, applied/materialized timestamps |
| Daily state | Partial/Complete/Failed, component revision/hash, committed attempt, Parquet/snapshot/manifest paths, count, lease token/owner/expiry, first-failure artifact/batch/paths, last operation/execution IDs |
| Outbox | Unique intent key, message body, correlation, creation and `sent_at_utc` |
| Event partition | `_catalog`, `yyyy-MM` or `_tombstones` key, revision, committed attempt, Parquet/snapshot/lineage paths, lease and error |
| Materialization | Event/work identity, event revision, planning generation, immutable plan path, pending ranges, has_pending, continuation flag/date, due time and error |

Audit-stamped daily/event state tables use the existing Raw Fan-Out audit functions. All exact fields/constraints come from the saved migration; later active migrations must be compared before implementation.

## 4. Production write sequence

Register/validate frozen batch membership → claim daily lease → load committed snapshot → read required immutable Raw parts → merge source versions → write candidate artifacts → fenced database transaction commits pointer, member receipt and notification intents. The final member invokes Raw batch ACK in the same transaction; failure rolls it back.

144 distinct aligned UTC slots are required for Complete Production. Partial can be durably acknowledged but remains ineligible for calculation. First failed contribution pins its original artifact/batch; a later healthy contribution does not silently remove that blocker. Exact redrive retries completed members as receipt-based no-ops.

## 5. Event write sequence

Register canonical member work → claim windfarm/source ownership → merge canonical state and publish partition/catalog candidates → durably commit canonical receipts and resumable plans → bounded lifecycle passes rebuild old/new affected days → mark generation-aware materialization receipts/final batches complete.

Transport deletion after durable canonical/plan acceptance is different from final daily completion and Raw ACK. Receipt-only scope barriers wait for expected canonical receipts. Keep these distinctions in monitoring and recovery.

## 6. Artifacts and authority

Example daily candidate:

`harmonized/wtg/windfarm=WF01/wtg=T001/date=2026-07-18/production/revision=5/attempts/token=44/`

It contains `production-10minute.parquet`, `snapshot.json` and `lineage.json`. Outage daily candidates use `outage-segments.parquet`. Event canonical files are organized by start month with separate tombstone/catalog branches and lifecycle plan files.

`Snapshot.json` is normalized merge state, not a universal historical publication index. Lineage records exact Raw/canonical provenance. A PostgreSQL committed pointer selects an authoritative immutable candidate. Partial uploads and expired-owner uploads remain unreferenced.

`packages/revision=N/manifest.json` remains a required package artifact owned by Package Coordinator, not this worker. Serving pointers are Publisher-owned.

## 7. Readiness and downstream notifications

`control.harmonizer_input_readiness` overlays existing daily rows with Pending where event work is unresolved. `control.harmonizer_event_day_pending(farm,wtg,date,source)` must also be checked when no day row exists.

No known active Outage and no pending work can be valid empty Outage input. Pending canonical work, cleanup, failed reads or conflicts cannot be mistaken for ready absence. Do not universally require five physical Complete source rows.

Outbox delivery is at least once; consumers deduplicate NotificationId and recheck current generation/readiness. Latest README leaves notifications disabled/unset until real Package Coordinator contracts align. Proposed `PackageCoordinationRequested` and `ProductionSummaryInvalidated` payloads are not proof of integrated consumers.

## 8. Runtime and migration

Worker long-polls and runs bounded lifecycle continuation with shared telemetry. Keep at least one replica, because lifecycle backlog can remain when SQS is empty. Different farms can progress concurrently; same farm/source publication uses a fenced lease. Distinguish Raw read provider from Harmonized read/write provider.

The saved 0004 is a fresh/unapplied consolidated baseline after Raw Fan-Out prerequisites. Existing deployments need forward migrations. Worker startup does not run DDL. Separate environment databases; no environment column.

## 9. Validation and open questions

Current delivered alignment was inspected, not compiled. No runtime/database migration was executed here. Q-007 requires crash/retry evidence across canonical/transport/final ACK boundaries; Q-008 requires a real package consumer; Q-009 covers remaining adapters; Q-012 covers independent receipt/artifact retention. Test stale owner commit, generation supersession, partial output, same-version conflict, stuck first failure, no-row pending Outage, duplicate notifications and recovery after canonical acceptance.
