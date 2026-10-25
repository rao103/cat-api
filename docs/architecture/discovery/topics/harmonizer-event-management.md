# Harmonizer event lifecycle management

> Updated: 2026-10-11 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## 1. Event model and ownership

Event lifecycle lives inside Harmonizer, using the same worker configuration, clients and telemetry (DL-014). Production/Curtailment use daily-series work. Outage/Alarm/Adjustment use event/member work, but the inspected lifecycle implementation is Outage; do not claim complete Alarm/Adjustment coverage without real mappings/tests.

| Identity | Meaning |
|---|---|
| Event ID | Stable business key; Outage classification ID |
| WTG member | Event applicability to a WTG; Outage has one, Adjustment parent may have many |
| Artifact/batch | Frozen transport/replay identity |
| Event revision | Accepted canonical state version |
| Planning generation | Version of resumable materialization/cleanup work |
| Attempt/lease token | Publication ownership; not business event ordering |

The latest aligned Outage processor rejects one event belonging to multiple WTGs within a batch. Retain that invariant while supporting Adjustment's parent/child relationship separately.

## 2. Canonical merge rules

| Incoming condition | Required result |
|---|---|
| Newer live version | Accept attributes/start/end and plan affected-day rebuild |
| Exact duplicate | Idempotent receipt/no-op; do not create false new work |
| Older live/delete version | Preserve newer canonical state; report outcome according to handler |
| Equal source version, conflicting content | Error; do not guess precedence |
| Newer tombstone | Preserve deletion version and remove old daily footprint |
| Live row older than tombstone | Do not resurrect |
| Legitimately newer live after tombstone | May supersede tombstone, subject to snapshot fence |
| Delete arrives before live | Persist tombstone in delete-first state |
| End remains null | Open event; continue through bounded processable horizon |

Order business mutations by source LastUpdatedDate, not SQS order, observation time or extraction token. Authoritative scoped absence fences add a separate rejection boundary for old event versions.

## 3. Old/new footprint reconciliation

Canonical state retains enough previous applicability to identify cleanup. A new row alone cannot describe all old dates affected by shortening, moving or deletion.

Affected work includes the union of previous footprint, new footprint and unfinished cleanup. Rebuild each affected WTG-day from all applicable current known events. Removing one event must not remove another overlapping event's contribution.

| Change | Daily action |
|---|---|
| Event shortened | Rebuild retained overlap and remove obsolete tail contributions |
| Event moved or start month changed | Update canonical month/catalog pointers; clean old dates and build new dates |
| Event deleted | Rebuild old dates from remaining events; publish empty input when appropriate |
| Open event closed | Materialize new bounded interval and clean any obsolete prior footprint |
| Adjustment membership removed | Clean removed WTG as well as processing retained/new children; contract still to implement |

Daily segments are clipped to UTC day boundaries. Closed intervals ending at day start do not contribute to that day. Null end stays null in canonical history; do not write DateTime.MaxValue as source truth.

## 4. Files and compact control state

Canonical monthly events live under `harmonized/events/outage/windfarm=<name>/startyear=YYYY/startmonth=MM/revisions/revision=N/attempts/token=T/`. Keep separate deleted-events/tombstone state, immutable `_catalog` candidates, snapshots/lineage and lifecycle plan files. Exact filenames/partition spelling follow the current writer.

`event_partition_state` selects committed partition/catalog candidates and ownership. `event_materialization_state` stores plan path, pending ranges, generation and continuation schedule. `harmonizer_batch_item` links receipts to lifecycle work/generation. Daily pointers live in `wtg_day_input_state`.

These compact tables recover publication progress; they are not row-by-row business event history or a duplicate operation-execution ledger.

## 5. Acceptance, transport and final completion

1. Register frozen event members and expected scope receipts.
2. Claim fenced ownership and validate/read current canonical candidates.
3. Publish and commit canonical state plus durable resumable work.
4. Record canonical acceptance. Only then can the inspected Outage transport route delete that SQS delivery.
5. Lifecycle passes rebuild bounded days and commit daily pointers/receipt generations.
6. Mark final member/batch materialization and Raw ACK after required work completes.

A receipt-only barrier waits for expected canonical receipts. Canonical acceptance is not a claim that daily cleanup finished. Restart after step 4 recovers from PostgreSQL plans rather than requiring the same SQS message to remain visible. Q-007 remains an end-to-end verification requirement.

## 6. Refresh modes

| Mode | Evidence | Effect on omitted events |
|---|---|---|
| ReceiptOnly shared refresh | Validated complete Raw manifest/scope; expected canonical receipts | No absence deletion |
| Authoritative reconciliation | Explicit verified SourceVersionCutoffUtc plus complete scoped capture and receipt barrier | Scoped absence may apply under source-version fences |

Aligned archive code/README now explicitly preserve nullable source cutoff metadata and the authoritative guard. Host SnapshotAsOfUtc and shared extraction token are not valid replacements for verified source authority. A newer event version must survive an older authoritative snapshot. No producer cutoff is assumed verified by this document.

Source query inclusivity and Fan-Out half-open overlap differ at an exact boundary; see Q-014. Absence reconciliation must not be enabled by interpreting missing files, failed reads or arbitrary host timestamps as complete source state.

## 7. Open-event continuation and scaling

Inspected defaults include processable day offset -1 (yesterday UTC), 31 days per pass, 30-second polling, bounded farms/partitions and retries. These are checkpoint configuration values, not permanently fixed business requirements. Current UTC dates become processable as time advances; ongoing events produce later daily outputs without requiring a source change.

Keep at least one worker running, including KEDA minimum one. SQS-only scaling cannot see durable continuation backlog. Serialize same-farm/source publication with leases/fences and allow independent farms concurrently. Recover expired owners; do not commit their uploaded candidates.

Clock-driven future continuation must not keep the originating user operation open forever. Report accepted/materialization-through-cutoff and ongoing continuation separately; final API integration remains to verify.

## 8. Readiness and verification

Check pending predicate even if no current Outage daily row exists. Known absence with no pending work is valid empty input; pending cleanup, canonical errors or unreadable state block readiness. Older Complete pointers remain history while newer rebuilds are Pending.

Verify shorter/moved/deleted/open events, overlapping unaffected events, delete-first arrival, equal-version conflict, delayed snapshot, stale owner, crash after durable acceptance, pending generation supersession, UTC boundaries and empty scope. Latest archive reports source-preservation and regression-test content, not a executed test run. Alarm/Adjustment and real concurrency/load evidence remain open.
