# Outage implementation — 6 October 2026

Outage is implemented in Harmonizer Core and Worker. The same executable hosts inbound source strategies and autonomous event lifecycle polling. Production V2/V3 remain supported. Ingestors, Raw Fan-Out, Package Coordinator, Calculator and Serving Publisher are outside this delivery.

## Verified input

The shared `EventWorkV3` contract is reused from the latest Raw Fan-Out implementation; it is not duplicated in Harmonizer. Its source is Outage; work kinds are `CanonicalEvents`, `EventSnapshotReconciliation`, and the empty incremental `ArtifactObserved` observation. The work contains farm ID/name, event/member keys, numeric WTG ID/name, exact contributing Raw paths, extraction/run/attempt metadata, optional operation/trace IDs, and the original envelope. It contains no calculated affected dates. Snapshot barriers contain the frozen WTG scope, every expected canonical batch ID, an explicit reconciliation mode, observation provenance, and an optional verified source-version cutoff.

`EventKey` hashes the default System.Text.Json serialization of `(SourceType, WindfarmId, EventId)`. `MemberKey` adds nullable numeric WtgId. The reader validates those exact hashes, numeric catalog identities, names, path membership, payload/mode authority, UTC scope and duplicate JSON fields. The original body remains unchanged for matching the persisted Fan-Out dispatch plan.

The actual live/deleted Parquet fixtures have the same physical columns:

| Raw column | Meaning/type |
|---|---|
| EventId | Classification identity, string; never descriptive OutageId |
| WindfarmId | Actual fixture contains a string farm name; numeric INT32 is also supported |
| WtgId | Actual fixture contains a string turbine name; numeric INT32 is also supported |
| OriginalStartUtc | UTC INT96; ignored for tombstones |
| OriginalEndUtc | Nullable UTC INT96; null means ongoing |
| LastUpdatedDate | Required UTC INT96; source version |
| IsTombstone | Required boolean; must agree with OutageDeletedEvents vs live/snapshot payload |
| SourceWatermarkUtc | Required UTC INT96 capture evidence; preserved in origins, excluded from business-content conflicts |
| EventType, Status, SourceRevision | Nullable strings; SourceRevision is opaque, not numerically ordered |
| OutageId, Comment, AlarmCode, AlarmText, Category, SubCategory | Nullable descriptive strings; AlarmCode preserves leading zeros |

The reader validates declared columns and row/byte bounds, restores omitted null dictionary entries from the physical schema, then selects each member only from its exact declared paths. Missing declared members and provider/decoding failures remain errors. Unknown descriptive fields survive in optional ExtraAttributesJson; the known fields above are ordinary typed output columns. Adding a descriptive mapping does not change identity/interval/lifecycle processing.

Catalog SQL joins `extraction_cursor` to `windfarm` and `wind_turbine`, using `cursor.windfarm_id`, `farm.windfarm_name`, `turbine.id AS wtgid`, and `turbine.wtg_name`. Only `source_type='Outage'` cursor rows are used. Farm/turbine names never become numeric IDs. Cleanup uses the identities already preserved in canonical data and plans, including an old turbine after an event moves.

## Source ordering and deletion

Canonical identity is `(numeric WindfarmId, OutageClassificationId)`, independent of WTG and month. Each newer source LastUpdatedDate replaces the previous state; an older change is ignored. Equal timestamps with differing business content fail as a source conflict. Capture watermark changes alone are not business changes. Canonical EventRevision advances only for an accepted source mutation.

A deletion preserves its LastUpdatedDate and classification identity. It replaces the live state, keeps its version barrier, and schedules cleanup from the saved footprint. A delete-first record is stored in `deleted-events`; its placeholder start date is never used to select a month. A stale live message cannot resurrect it. A newer legitimate live source version can replace a tombstone.

An open event retains a null end. Overlap is half-open `[OriginalStartUtc, OriginalEndUtc)`, with null unbounded. Segments clip only to the UTC day and any scoped snapshot exclusions, while retaining the original interval. Midnight ends do not add the next day. INT96 and LastUpdatedTicks preserve source precision.

## Paths and exact links

Live/known-month canonical state:

```text
harmonized/events/outage/windfarm=WF01/startyear=2026/startmonth=07/
  revisions/revision=7/attempts/token=42/
    outage-events.parquet
    snapshot.json
    lineage.json
```

A deletion with known prior state stays in its prior month as a tombstone. Delete-first state:

```text
harmonized/events/outage/windfarm=WF01/deleted-events/
  revisions/revision=1/attempts/token=43/
    deleted-events.parquet
    snapshot.json
    lineage.json
```

WTG-day output, including explicit zero-row Parquet after cleanup:

```text
harmonized/wtg/windfarm=WF01/wtg=T001/date=2026-07-18/
  outage/revision=5/attempts/token=44/
    outage-segments.parquet
    snapshot.json
    lineage.json
```

Each nonempty daily row contains EventId/OutageClassificationId, EventRevision, CanonicalStartYear/Month, CanonicalMonthRevision, CanonicalCommittedAttempt and the exact CanonicalEventPath. A query follows that path and selects that classification/revision; it never guesses the latest event month. Old links remain readable after unrelated month changes or start-month corrections.

Daily lineage contains contributor canonical paths, canonical lineage paths, source origins, operation/execution IDs, trigger type, planning generation, original batch/envelope and Raw paths. Even an empty cleanup file retains TriggerEvidence pointing to the tombstone that caused it.

Internal committed catalog:

```text
harmonized/events/outage/windfarm=WF01/_catalog/
  revisions/revision=9/attempts/token=55/manifest.json
  revisions/revision=9/attempts/token=55/lineage.json
```

The catalog contains month references and scoped snapshot observation/exclusion metadata, not another copy of all business rows. Month snapshots retain complete normalized canonical state, including tombstones and origins, for reproducible merging. Daily snapshots retain the normalized projection and the catalog reference. All files are immutable; PostgreSQL selects committed pointers. Uploads that never commit remain unreferenced candidates.

Durable range plans use:

```text
harmonized/lifecycle/outage/windfarm=WF01/event={encoded-work-identity}/
  generation=4/attempts/token=55/progress={content-hash}/plan.json
```

The progress hash distinguishes successive immutable checkpoints under the same lease. A revision alone cannot distinguish competing owners proposing the same next revision; the fenced attempt token is retained.

`packages/revision=N/manifest.json` remains owned by Package Coordinator. Harmonizer writes no package manifest or CalculationWork. Its transactional outbox emits the existing proposed PackageCoordinationRequested notification; downstream consumers must enforce readiness before dispatching calculation.

## Durable lifecycle and SQS acknowledgment

1. Register the exact frozen batch, operation metadata and expected members in the existing batch table.
2. Read/validate exact Raw files. Claim a fenced farm/source catalog lease and load explicitly committed month pointers.
3. Merge by classification identity. For each changed event, plan the union of the previous materialized footprint, outstanding old cleanup, and new applicability through the processable date.
4. Write immutable changed-month candidates, range plans, and a catalog candidate.
5. One PostgreSQL transaction commits all changed-month/catalog pointers, current lifecycle plan pointers, pending ranges and canonical member receipts. A start-month move updates both month pointers in that transaction.
6. Run a bounded materialization pass. A complete day is rebuilt from all overlapping known canonical events, preserving unrelated contributors. Each day pointer, notification and progress checkpoint commit together behind the catalog fence.
7. After canonical state and its durable plans are accepted, the SQS delivery may be deleted. This is durable handoff, not completed materialization. The autonomous loop resumes remaining pages without requiring repeated SQS delivery.
8. Only after every frozen member target is materialized are final batch completion and `raw_artifact_ack` committed together. Completion sweeps recover a crash between the last day and final batch acknowledgment.

A bounded source target finishes even if an outage remains open. On a later UTC day, clock continuation creates a new bounded plan generation with untracked autonomous operation identity, preserving the original event origins. It does not bump the canonical event revision or extend a completed source operation forever. Future portions of closed events also continue until their end becomes processable.

The default processable cutoff is yesterday UTC. `ProcessableDayOffset` can change that explicitly. Plans use compact ranges rather than allocating every date of a many-year event. Each pass processes at most DaysPerPass sequential dates; the next pending date cannot be skipped. New mutations retain outstanding cleanup even if a previous generation was only partly materialized.

## Empty data and refresh

No active known event overlapping a WTG-day means no outage. There is no independent upstream completeness/baseline gate. An empty rebuilt day is Complete and has a zero-row Parquet file. An empty incremental observation does not erase previously known events. Read, validation, source-conflict and write failures never become empty success.

A snapshot barrier may arrive before its children. It waits for every listed batch's successful canonical receipt; queue ordering or absent receipts cannot substitute for success. `ReceiptOnly` barriers then complete without applying absence. Only `Authoritative` barriers with an explicit verified `SourceVersionCutoffUtc` may record absence, and only inside the declared interval/WTG scope. An omitted event may have moved outside that interval, so scoped absence never creates a global tombstone.

Scoped exclusions apply to the exact canonical event revision and UTC interval. A newer source mutation restores applicability. A later full snapshot can also restore presence without a business version change. Capture metadata protects newer interval observations from delayed older barriers; source row mutation ordering still uses LastUpdatedDate.

Capture ordering assumes ExtractionToken is the monotonic fenced token from the same `(windfarm, Outage)` extraction cursor for incremental and refresh runs. It protects snapshot observation ordering, never replaces LastUpdatedDate for event versions. An authoritative absence fence also persists its source-version cutoff with the frozen scope; subsequent in-scope changes at or below that cutoff are rejected. A refresh using an independent token namespace requires an explicit comparable capture sequence before relying on capture ordering.

`SnapshotObservedAtUtc` is provenance only. Neither it, row `SourceWatermarkUtc`, extraction-token order, nor the maximum timestamp among selected rows may be substituted for a verified source-issued cutoff. Empty snapshots can use `ReceiptOnly`; they cannot authorize absence without an explicit cutoff.

## Database and readiness

There are six Harmonizer control tables: the four existing tables plus `control.event_partition_state` and `control.event_materialization_state`. No event_head, event_revision, event history, per-day lifecycle job, or independent coverage table is added.

The partition table holds catalog/month pointers and the farm/source lease. The materialization table holds current event/work identity, current locator, plan pointer/generation, compact pending ranges, continuation watermark, due time and errors. Source business attributes and historical versions remain in files. Existing batch/item tables now distinguish Production WTG-day receipts, event member receipts, scope receipts and empty observations.

Package Coordinator must call `control.harmonizer_event_day_pending(farm, wtg, date, source)` even when an Outage component row does not yet exist. It accounts for unresolved accepted canonical input, a registered scope waiting for children, and pending materialization ranges. `control.harmonizer_input_readiness` applies the same predicate to existing rows. An older Complete pointer is readable history while a newer rebuild is Pending. A failed contribution cannot silently become no outage.

If this predicate is false and no active known event/component exists, absence is the agreed empty Outage result. No requirement for five physical Complete rows should be introduced for this rule. Other sources retain their own readiness policies; this does not declare unsupported sources ready.

## Telemetry and parallel replicas

All runtime I/O and orchestration boundaries use ITelemetry. Outage reader, artifact adapter, identity/control repositories, canonical loader, batch processor, lifecycle processor/handler, generic host loops and heartbeat log structured context. Pure models/mappers/merge/range/interval functions do not inject loggers.

The existing host emits UTC JSON Timestamp values and OpenTelemetry logs/traces/metrics. There is one appsettings.json, one service resource, and host-owned providers/exporters. IDs, names, paths and dates are log/trace fields; metric dimensions use bounded source/trigger/outcome values. OperationId is business correlation, not a W3C trace ID. Credentials and full payloads are not logged.

Each pod has bounded inbound and lifecycle concurrency. The database lease permits one Outage publication owner per farm; different farms run in parallel. Heartbeats renew ownership, and every canonical/day/progress commit checks token, owner and database lease expiry. A newer pod cannot be overwritten by an expired owner. The source lease stays held through the bounded pass, so a new overlapping event cannot slip into a day upload without the same serialized ownership.

All pods may run the lifecycle loop. Claims prevent duplicate committed work. Keep at least one replica running, including KEDA minReplicaCount=1, so open-event continuation runs when SQS is empty. Queue depth alone does not represent lifecycle backlog. Account for ConcurrentMessages plus ConcurrentFarms per source, multiplied by replicas, when budgeting the shared per-pod Npgsql pool and memory.

## Validation boundary

Release unit tests include the supplied 41 live and one deleted Parquet rows, typed output, exact links, precision/nulls, moved turbine/month, shortening/deletion, cleanup provenance, bounded recovery, superseded generations, clock continuation, empty observations, scoped absence/presence and delayed barriers. PostgreSQL/Npgsql integration tests are included but require a disposable server. Embedded PostgreSQL SQL smoke checks validate migrations and actual repository statements; they do not replace live multi-connection concurrency or AWS/Azure/RDS acceptance tests.
