# Distributed Loss Calculation and Metrics Platform — Refined Architecture Plan

> **Version 10 update:** retains all Version 9 refinements and adds the event-serving model agreed during review: canonical monthly event files for outage, alarm, curtailment and adjustment; Windfarm-Day daily event-reference projections so long/open events are discoverable from any overlapping day; nullable event end times; explicit separation between source/canonical event data in Harmonized and calculated loss values in Calculated; and Synapse query rules that combine daily event references with additive Item-Day calculated summaries without copying calculated loss into Harmonized. It further closes the open-event materialization gap with `EventMaterializationState` and a clock-driven Event Lifecycle Coordinator (Section 6.11), splits calculation materialization from current-day event serving at `CalculationProcessableDate`, and adds `SummaryType = HarmonizedEventReference` with its own `WindfarmDayEventReferenceState`.
>
> **Version 10.1 update — ingestion-path simplification.** The Platform Orchestration API is switched off for this solution's container, and with it the `RawDispatch`, `RawFanOut`, `RawFanOutWorkItem` ledgers and the Harmonization Dispatcher are removed. The extraction pod now owns Raw write and queue publication directly, under a **fenced extraction lease**; Raw artifacts are addressed by `Windfarm + Date + extraction=<LeaseToken>`; `ItemDayInputState` becomes both revision authority **and** readiness state, carrying `Failed`, `AttemptCount` and `LastError`; a reconciliation sweep retries failed Item-Days; and a three-class failure taxonomy (item / batch / poison batch) plus a `FanOutFailure` record covers the pre-fan-out window where no Item-Day identity exists yet. The Adjustment outbox is explicitly retained.

> **Terminology note.** "Fan-out" now refers only to the in-process enumeration of a Raw file into Item-Day batches by the Raw Fan-out Coordinator. It no longer implies a durable SQL child ledger.

> **Version 10.2 update — historical refresh, event-footprint reconciliation and queue portability.** Adds a complete `RefreshAndRecalculate` path for rebuilding a selected range from authoritative sources without deleting immutable Harmonized/Calculated history; tightens event-correction semantics so each `(EventId, ItemId)` reconciles its actual materialized footprint against the new canonical footprint even when old and new intervals partially overlap or are completely disjoint; defaults empty Item-Day components to authoritative logical-empty revisions (`Status = Complete`, `RecordCount = 0`, `path = null`) with ordinary canonical hashing and optional physical zero-row Parquet only when justified; and introduces a provider-neutral work-queue contract so AWS SQS remains an infrastructure adapter rather than a business-state dependency.

## 1. Purpose

This document defines the target architecture for a distributed loss-calculation and metrics platform.

The design evolves the existing single integration-and-calculation application into a staged, event-driven architecture that separates:

- Source extraction
- Raw data retention
- Data harmonization
- Calculation orchestration
- Parallel calculation
- Publication
- UI serving and metrics
- Backfill, recalculation, resilience, and observability

The platform supports approximately thousands of items and multi-year data ranges.

Primary calculation inputs are:

1. 10-minute production data
   - Actual production
   - Possible production
   - Active power and other measurements

2. Outage data
   - Can be shorter than one minute
   - Can last multiple days or weeks

3. Curtailment data

4. Alarm data
   - Primarily used to explain outage conditions

5. User adjustments
   - Created or changed through the UI
   - Stored in Azure SQL
   - Can affect the calculation and trigger recalculation

The normal processing direction is:

```text
Raw → Harmonized → Calculated → Serving
```

Each downstream zone contains everything required by the next stage.

Normal processing should not repeatedly look backward to earlier zones.

---

## 2. Core Architecture Principles

### 2.1 Forward-only normal processing

```text
Raw Zone
   ↓
Harmonized Zone
   ↓
Calculated Zone
   ↓
Query Engine
   ↓
Application API
```

The Raw Zone is retained for audit, recovery, replay, and rebuilding downstream zones if processing logic changes.

Pure recalculation reads from the Harmonized Zone and does not reread Raw.

---

### 2.2 Complete partition replacement

Recalculation replaces a complete logical partition rather than applying untracked deltas.

For normal processing, the preferred logical unit is:

```text
ItemId + CalculationDate
```

Large historical requests may be grouped by `ItemId + Month` as an orchestration/dispatch batching envelope, but such a group is always expanded into Item-Day work items before worker execution. A calculation worker never executes a month as a single unit.

---

### 2.3 Package-driven calculation and published-result state

Calculation input identity and published-result identity are tracked separately.

```text
RequestedPackageRevision
RunningPackageRevision
CommittedPackageRevision   (on CalculatedPartition / published result state)
```

A package revision identifies the exact Harmonized component revisions plus configuration and calculation-code version. A calculated result becomes readable only after validation and publication commit.

Example:

```text
RequestedPackageRevision = 19
RunningPackageRevision   = 18
CommittedPackageRevision = 17
```

Package 18 may finish computation, but it must not publish if package 19 is still the latest requested package. A newer requested package can never be overwritten by an older result.

Pure user-requested re-execution of an unchanged package is tracked by a `RecalculationRun` / result revision rather than manufacturing changed Harmonized component revisions.

---

### 2.4 At-least-once processing with idempotency

AWS SQS may retry or redeliver messages.

All queue consumers must therefore be idempotent.

The same work item can run more than once without producing duplicated or inconsistent results.

---

### 2.5 Deterministic calculation

For the same:

- Harmonized input revision
- Adjustment revision
- Configuration version
- Calculation code version

the calculation must always produce the same result.

Input ordering must not affect the output.

---

### 2.6 Independent scaling

The following workloads scale independently:

- Extraction
- Harmonization
- Normal calculation
- Backfill calculation
- Calculated publication

This prevents one workload type from consuming all platform capacity.

---

### 2.7 Separate historical backfill capacity

Historical processing must not starve normal operational processing.

Use independent calculation queues and worker limits for:

```text
Normal calculation
Historical backfill
```

---

### 2.8 The lake is the analytical data plane; Azure SQL is the transactional/control plane

High-volume source, harmonized, calculated, and aggregate datasets remain in object storage as standard Parquet files.

Azure SQL is retained only for small, transactional, low-volume state:

- Configuration and configuration versions
- User adjustments, versions, audit and outbox
- Calculation control metadata such as work ledger, runs and leases
- Other operational metadata where relational transactions are useful

Azure SQL does **not** store the large 10-minute production facts, calculated-loss history, or analytical daily rollups.

```text
Azure SQL
  = configuration + transactional user input + orchestration/control state

Data Lake
  = Raw + Harmonized + Calculated (+ optional future Gold optimization datasets only if justified)
```

### 2.9 Query-engine abstraction

The Application API does not depend directly on one query technology.

```text
Application.Api
      ↓
Query abstraction
      ├── Synapse Serverless provider   ← initial implementation
      ├── DuckDB provider
      └── AWS Athena provider
```

**Synapse Serverless SQL is the initial query engine** because the lake currently resides in Azure Blob Storage, allowing filtering and aggregation to execute close to the data before the reduced result crosses back to EKS.

Provider-specific implementations own SQL dialect differences, authentication and query execution. Controllers do not construct engine-specific external-file SQL.

### 2.10 Plain Parquet only

The initial implementation uses standard Parquet files only.

The architecture does **not** require Delta Lake, Apache Iceberg, Spark, Trino or a lakehouse catalog.

Each revision represents a **complete replacement of its logical partition**, not a delta chain.

A lakehouse table format may be evaluated later only if a concrete requirement appears.

---

### 2.11 Event identity is canonical; daily event references are serving indexes

Long-lived and open-ended outage/alarm/curtailment/adjustment records are not forced into one physical representation for every workload.

```text
Canonical event
= one logical event row with stable EventId and nullable EndUtc

Calculation input
= Item-Day segment / aggregate

Serving index
= one lightweight EventId reference per overlapping Item-Day
```

Calculated loss values remain exclusively in the Calculated Zone. The Harmonized event/reference datasets describe source/canonical facts and where an event participates in the reporting calendar; they do not store calculated loss totals.

## 3. Target Architecture

```text
                         SOURCE SYSTEMS
                              │
                              ▼
                    EKS Extraction Pods
                 incremental / bounded pulls
         ExtractionCursor + fenced lease (Azure SQL)
                              │
                              ▼
              Azure Blob — RAW ZONE (append-only)
        /raw/<source>/windfarm=/date=/extraction=<token>/
                              │
                              ▼
                     AWS SQS raw-fanout
              published by the extraction pod itself
                              │
                              ▼
                 Raw Fan-out Coordinator
                  inspect one Raw file
              + emit Item-Day batch messages
           (no durable child ledger; DLQ on failure)
                              │
                              ▼
              AWS SQS harmonization-processing
                    Item-Day batches
                              │
                              ▼
                    Harmonizer Pods
                              │
        Normalize / Split / Aggregate / Validate
                              │
                              ▼
            Azure Blob — HARMONIZED ZONE
         ┌────────────────────┼────────────────────┐
         │                    │                    │
   normalized detail   reusable source       calculation-ready
                        aggregates               inputs
                              │
                   ├─ adjacent curtailment
                   ├─ daily WTG production
                   └─ daily windfarm production
                              │
                              ▼
                 Calculation Package Coordinator
                              │
                              ▼
                   Calculation Work Ledger
             RequestedPackageRevision / RunningPackageRevision
                              │
                  ┌───────────┴───────────┐
                  ▼                       ▼
          Normal Calculation SQS    Backfill SQS
                  │                       │
                  └───────────┬───────────┘
                              ▼
                    EKS Calculation Pods
                              │
               Deterministic C# Engine
                              │
                              ▼
             Azure Blob — CALCULATED ZONE
         ┌────────────────────┼────────────────────┐
         │                    │                    │
  10-minute loss        daily WTG loss       daily windfarm loss
  / allocation detail   and other-loss       and other-loss
                        summaries             summaries

       HARMONIZED ZONE                     CALCULATED ZONE
               │                                 │
               └───────────────┬─────────────────┘
                               ▼
                     QUERY-ENGINE ABSTRACTION
                 ┌─────────────┼─────────────┐
                 │             │             │
        Synapse Serverless   DuckDB        Athena
          (initial)          (option)      (option)
                 │
                 ▼
                    Application.Api
             ┌─────────┼──────────┐
             │         │          │
          Metrics   Adjustments  Recalculation/
                                 processing status
             │
             ▼
             UI
```

The Raw Blob **container** remains operated by the infrastructure team, but its file-lifecycle orchestration is switched off for this solution. No external service assigns `New → Running → Completed` to a Raw file, and nothing outside this solution waits on Raw-file completion.

Consequently this solution owns the Raw path convention it writes into, the extraction lease that authorizes the write, and every stage after it. Before switching the Orchestration API off, confirm that no other consumer or compliance process depends on the Raw file lifecycle; that is an ownership question, not a technical one.

Control state is deliberately confined to where coordination is genuinely required:

```text
SQS  = execution delivery
Blob = data
SQL  = authoritative state only where correctness needs it
```

### Optional Gold Zone

A physical Gold Zone is **not required initially**.

Gold is reserved only for future serving-optimized datasets that are expensive to derive repeatedly from Harmonized and Calculated data.

Do not copy data into Gold merely because a medallion architecture contains a Gold layer.

### User adjustment path

```text
UI
 ↓
Application.Api
 ↓
Azure SQL Adjustment + Audit + Outbox      ← RETAINED, see below
 ↓
Outbox processor
 ↓
Raw adjustment Parquet (Azure Blob)
 ↓
raw-fanout SQS
 ↓
Raw Fan-out Coordinator
 ↓
harmonization-processing SQS
 ↓
Harmonizer
 ↓
/harmonized/item/.../adjustment/revision=<n>/adjustment-segments.parquet
 ↓
Calculation Package Coordinator
 ↓
new package revision for affected Item-Dates
 ↓
Calculation Dispatcher
 ↓
Calculation SQS
```

The outbox is **not** optional and is not removed by the ingestion simplification. For source extraction the upstream system plus the cursor can always re-derive missed work; for adjustments **Azure SQL is itself the source of truth**, so without an atomic `Adjustment + Outbox` commit an API crash after `COMMIT` produces the worst failure class in the platform:

```text
Adjustment saved and visible in the UI
        ↓
API crashes before the lake push
        ↓
adjustment exists, calculation never sees it
        ↓
wrong numbers, no error anywhere
```

# 4. Feature / Zone 1 — Extraction and Raw Data Zone

## 4.1 Responsibility

Extract changed source data safely, write it durably to the Raw Zone, and publish it for harmonization.

The zone answers:

> What changed in the source, which business partitions are affected, and which Raw artifact represents the durable extraction result?

Raw is retained for audit, recovery, replay, and downstream rebuilds. The extraction pod owns the complete unit of work: acquire lease → read source → write Raw → publish to SQS → advance cursor. There is no separate dispatcher stage and no external file-lifecycle service.

---

## 4.2 Infrastructure

- EKS extraction pods
- Azure Blob Raw storage
- Azure SQL `ExtractionCursor` (durable cursor + fenced lease)

---

## 4.3 Raw storage layout and logical grain

This solution owns the Raw path convention for the data it writes.

For dense 10-minute Production data, the logical Raw payload is a bounded **Windfarm-Day snapshot** rather than one Raw file per turbine-day or one very large multi-month windfarm file.

```text
Production Raw logical grain
= Windfarm + ProductionDate
```

A full normal Production day contains approximately:

```text
Expected rows
≈ NumberOfWTGs × 144 ten-minute intervals
```

For example, 1,500 WTGs produce approximately 216,000 ten-minute rows for one complete UTC day. This is intentionally coarser than the Harmonized Item-Day grain so that source-query count and Raw-file count remain bounded.

For sparse event sources such as outage, alarm, curtailment and adjustment, Raw remains event-shaped and is **not** forced into the Windfarm-Day payload shape. Its grain is defined in Section 4.3.1.

---

### 4.3.1 Raw event grain — append-only extraction batches

Raw event ingestion and the monthly canonical event partition (Section 6.10.1) are **different concerns and must not share a layout**.

```text
Raw event ingestion
≠
monthly canonical event partition

Raw          = append-only extraction batches, never rewritten
Monthly      = Harmonized canonical/serving projection, revisioned
```

One extraction cycle produces one immutable Raw batch per source and windfarm:

```text
Source extraction cycle
        ↓
Raw batch #18492

O1001   current state
O1007   current state
O1021   tombstone
...
```

The payload carries the **full current state of each changed event**, not a delta of changed fields:

```text
Raw event payload
-----------------
EventId                 stable logical identity
WindfarmId / ItemId
EventType
OriginalStartUtc
OriginalEndUtc          NULL while the event is open
Status                  Open | Closed | Cancelled
SourceRevision
SourceWatermarkUtc      source LastUpdated for the conflict rule (Section 8.4)
IsTombstone             explicit deletion/invalidation marker
<type-specific attributes>
```

Rules:

```text
1. Raw event files are NOT partitioned by event start month.
2. Raw event files are NEVER rewritten, including while an event remains open.
   A later change to an open event is a new row in a later batch, not an edit.
3. Deletion and invalidation are explicit tombstone rows; silent omission is
   never a delete signal.
4. Full current state per changed event is what makes the safety-lag re-read
   (Section 4.4.1) and SQS redelivery no-ops rather than partial merges.
5. Harmonization — not extraction — determines which monthly canonical partition
   and which Item-Days a batch affects.
```

Because a batch is append-only, no *Raw-side* revision counter or read-modify-write is required. The extraction lease (Section 4.4.2) still applies, because it prevents two pods from extracting the same source concurrently; it is a claim on the extraction run, not a lock on a Raw file. All content contention is deferred to the Harmonized layer, where it is fenced explicitly (Sections 8.4 and 6.10.1).

Unlike Production, an event extraction batch has **no stable business identity**: the set of changed events differs on every cycle, so no deterministic batch key can be manufactured. Idempotency for events comes from downstream keyed merge on `EventId` plus `CanonicalContentHash`, never from Raw path identity.

```text
Production
----------
full authoritative day snapshot
→ business day IS a stable logical identity

Events
------
changed-event extraction batch
→ the batch is NOT a stable identity
→ EventId is the stable business identity
→ downstream merge provides idempotency
```

---

## 4.4 Production incremental extraction

Production change detection remains **windfarm-grain incremental extraction** using a composite source cursor:

```text
WindfarmId
+
LastUpdatedDate
+
LastCommittedId
```

`LastUpdatedDate` identifies when a source row changed. `ProductionTimestamp` identifies which business date is affected. They are different concepts and must not be conflated.

Conceptual source predicate:

```sql
WHERE WindfarmId = @windfarmId
  AND LastUpdatedDate >= @effectiveQueryLow
  AND LastUpdatedDate <  @windowEnd
ORDER BY LastUpdatedDate, Id
```

`@effectiveQueryLow` is derived in Section 4.4.1 and is **not** the durable committed cursor. Two cursors exist and must not be collapsed into one:

```text
DurableCommittedCursor
= (LastUpdatedDate, Id) persisted after Raw registration succeeds

EffectiveQueryLow
= DurableCommittedCursor.LastUpdatedDate − SafetyLagInterval
```

Because the run deliberately rewinds below the durable cursor, the previous run's `LastCommittedId` must **not** be used to exclude rows inside the lag interval — doing so would re-open the gap that Section 4.4.1 exists to close.

`(LastUpdatedDate, Id)` is instead used as the **keyset-pagination key within the current extraction run**:

```sql
-- continuation page inside one run
AND (
      LastUpdatedDate > @pageLastUpdatedDate
      OR (LastUpdatedDate = @pageLastUpdatedDate AND Id > @pageLastId)
    )
```

The composite key therefore serves two distinct purposes: `Id` prevents rows sharing a `LastUpdatedDate` from being skipped across page boundaries, and the durable cursor records how far the *previous* run safely progressed.

The durable extraction cursor is therefore logically:

```text
(LastUpdatedDate, Id)
```

not timestamp alone.

---

### 4.4.2 Extraction lease and fenced cursor commit

With the Platform Orchestration API removed, its atomic `New → Running` transition no longer provides the claim that stops two pods working the same source. That claim moves onto the cursor row itself:

```text
ExtractionCursor
----------------
WindfarmId
SourceType

LastUpdatedDate
LastCommittedId

LeaseOwner
LeaseToken             monotonically increasing
LeaseExpiresAt

LastRunStartedUtc
LastRunCompletedUtc
ConsecutiveFailures
LastError

UNIQUE (WindfarmId, SourceType)
```

One pod at a time owns `WF01 + Production`.

The cursor commit must be **fenced on the token**, not merely guarded by lease ownership. A lease can expire while its holder is still running — a paused pod, a long GC, a network partition — and the zombie does not know it lost the lease:

```sql
UPDATE ExtractionCursor
   SET LastUpdatedDate = @newLastUpdatedDate,
       LastCommittedId = @newLastCommittedId,
       LastRunCompletedUtc = SYSUTCDATETIME()
 WHERE WindfarmId = @windfarmId
   AND SourceType = @sourceType
   AND LeaseToken = @leaseToken;
```

```text
@@ROWCOUNT = 1   → cursor commit succeeded
@@ROWCOUNT = 0   → lease was superseded; the zombie MUST NOT commit,
                   MUST NOT retry the commit, and MUST discard its run
```

This protects the classic case:

```text
Pod A token=41
    ↓ pauses
lease expires
Pod B token=42
    ↓ processes newer data, advances cursor
Pod A wakes, commits with token=41
    ↓
0 rows updated — cannot overwrite Pod B
```

`LeaseToken` is therefore not merely a locking field; it is the **fencing token at commit time**, exactly as in Sections 8.4 and 6.10.1.

The full extraction sequence is:

```text
acquire extraction lease            → LeaseToken = 57
        ↓
read durable cursor
        ↓
query source with safety lag (Section 4.4.1)
        ↓
write Raw under extraction=57 (Section 4.9)
        ↓
SendMessage to raw-fanout SQS
        ↓
fenced cursor advance               WHERE LeaseToken = 57
        ↓
release lease
```

The ordering is mandatory. Advancing the cursor before the SQS send would silently lose the work; the reverse order costs only a bounded replay, which downstream idempotency absorbs (Section 4.7).

---

### 4.4.1 Safety-lag re-read for late-visible source rows

The composite cursor prevents ties from being skipped, and the frozen upper watermark in Section 4.5 prevents the run from chasing a moving source. Neither protects against a third hazard: a source row whose `LastUpdatedDate` is stamped at **transaction start** but which only becomes **visible** after an extraction cycle has already advanced past that timestamp.

```text
13:00  source transaction begins, row stamped LastUpdatedDate = 13:00
13:05  extraction cycle runs, advances cursor to 13:04
13:07  source transaction commits, row becomes visible at 13:00
        ↓
the row is now permanently below the cursor and is never extracted
```

This is silent data loss, not a delay, so it cannot be detected by freshness alerting alone.

Each cycle therefore re-scans a bounded overlap below the committed cursor:

```text
EffectiveQueryLow = DurableLowCursor.LastUpdatedDate − SafetyLagInterval
```

Rules:

```text
1. Re-read the safety-lag window on every cycle; do not use a strict cursor boundary alone.
2. The re-read is safe because Harmonization merging is a keyed idempotent upsert
   (Section 8.4), so an already-applied row leaves the component's canonical content
   hash unchanged and mints no new revision.
3. SafetyLagInterval is configurable per source and should exceed the observed
   maximum upstream write/transaction duration for that source.
4. If a source guarantees commit-time stamping, or exposes a monotonic change
   sequence, SafetyLagInterval may be zero. That guarantee must be recorded as an
   explicit, verified source assumption rather than presumed.
```

The overlap deliberately trades a small amount of redundant source reading for the elimination of an undetectable gap.

---

## 4.5 Bounded catch-up windows

The incremental cursor must not create one unbounded source query after a long ingestion outage.

If the gap is large, split the **LastUpdated timeline** into bounded windows, with one month as the normal maximum catch-up window unless source performance requires a smaller limit.

Example:

```text
Last committed cursor: 2026-05-15
Current source horizon: 2026-08-30

Catch-up windows:
2026-05-15 → 2026-06-01
2026-06-01 → 2026-07-01
2026-07-01 → 2026-08-01
2026-08-01 → 2026-08-30
```

This is still one windfarm-level incremental stream; it is **not** expanded into `1,500 × number-of-days` source queries.

Within a window, keyset paging may be used when row volume exceeds the safe response size. Avoid OFFSET pagination for very large tables.

At the beginning of an extraction cycle, freeze a finite upper watermark so the run does not chase a continuously moving source:

```text
DurableLowCursor  = previous committed (LastUpdatedDate, Id)

EffectiveQueryLow = DurableLowCursor.LastUpdatedDate − SafetyLagInterval
                    (Section 4.4.1 — the value actually used in the predicate)

HighWatermark     = frozen source upper LastUpdatedDate observed for this run
```

The catch-up windows above partition the span between `EffectiveQueryLow` and `HighWatermark`. Section 4.4.1 always applies; nothing in this section reinstates a strict lower bound at the durable cursor.

Rows arriving after the frozen upper watermark are handled by the next extraction cycle.

---

## 4.6 Change discovery followed by Windfarm-Day read-back

The incremental `LastUpdatedDate` query is primarily a **change-discovery mechanism**.

Example:

```text
LastUpdatedDate = 2026-08-30
ProductionTime  = 2026-03-15
```

This means an old March business date was corrected today.

The Production Ingestor therefore:

```text
1. Queries changed rows for Windfarm + bounded LastUpdated window.
2. Derives the distinct affected ProductionDate values.
3. For each affected ProductionDate, re-reads the complete authoritative Windfarm-Day
   production snapshot from the source.
4. Writes one bounded Raw payload per affected Windfarm-Day under the run's
   extraction token (Section 4.9), subject to source/file-size constraints.
5. Publishes one raw-fanout SQS message per Raw payload.
6. Advances the durable extraction cursor, fenced on the lease token, only after
   every Raw payload is durable AND its SQS message has been accepted.
```

Conceptual read-back:

```sql
WHERE WindfarmId = @windfarmId
  AND ProductionTimestamp >= @dataDate
  AND ProductionTimestamp < DATEADD(day, 1, @dataDate)
```

This keeps Raw coarse enough to keep file and message counts bounded while giving the fan-out stage a bounded file to process.

If a source cannot efficiently provide complete Windfarm-Day read-back, retain the source delta payload and let Harmonization perform an idempotent keyed merge; the preferred full-day snapshot rule is source-capability dependent.

---

## 4.7 Extraction cursor commit safety

The cursor is the durability boundary for the whole ingestion handoff. Never advance it before **both** Raw persistence and queue publication are safe.

```text
Query source window
        ↓
identify affected business dates
        ↓
write Raw payloads
        ↓
confirm durable write
        ↓
SendMessage to raw-fanout SQS
        ↓
confirm SendMessage accepted
        ↓
advance extraction cursor (fenced on LeaseToken)
```

The reverse ordering is unsafe and must not be used:

```text
write Raw → advance cursor → send SQS
        ↓
crash between cursor advance and send
        ↓
the work is below the cursor and is never re-derived
```

With the correct ordering, a pod that dies anywhere before the cursor advance simply causes the same source range to be replayed. Replay is safe because:

```text
Production  → the same business day is rewritten as a new extraction artifact,
              and Harmonization rebuilds the component from the full snapshot
Events      → keyed upsert on EventId + CanonicalContentHash makes the replay
              a no-op (Sections 8.4, 6.10.1)
```

This is the same property the safety-lag re-read (Section 4.4.1) already depends on.

---

## 4.8 Long events in Raw

A long outage such as:

```text
15 Jan 12:40 → 25 Jan 04:50
```

is stored once in Raw, in the extraction batch that carried its latest state.

It does not need to be duplicated into every daily Raw partition. Harmonization owns daily segmentation for calculation.

---

## 4.9 Raw artifact identity

Raw has no external lifecycle state machine. Its identity comes entirely from its path, and that path must satisfy two requirements that pull in opposite directions:

```text
idempotent    a crash-and-retry inside one run must rewrite the same location
append-only   a later correction must NOT overwrite the earlier extraction
```

`Windfarm + ProductionDate` alone satisfies only the first. It would produce:

```text
Sep 1 first extraction    → production.parquet
Sep 5 correction to Sep 1 → overwrite production.parquet   ✗ audit history destroyed
```

Introducing a monotonic Raw revision counter would satisfy the second but reintroduce exactly the SQL coordination the simplification removes. The extraction **lease token** already provides a monotonic, run-scoped discriminator, so it is used directly:

```text
/raw/production/
  windfarm=WF01/
    date=2026-09-01/
      extraction=41/
        production.parquet

... later correction, new run, new lease ...

/raw/production/
  windfarm=WF01/
    date=2026-09-01/
      extraction=57/
        production.parquet
```

```text
logical business identity      Windfarm + ProductionDate
physical extraction identity   Windfarm + ProductionDate + LeaseToken
```

Rules:

```text
1. Raw is append-only. An extraction artifact is never rewritten once its run
   has committed the cursor.
2. Within one run the path is deterministic, so a retry before the cursor
   advance rewrites the same location idempotently.
3. If the pod dies long enough for the lease to expire, the recovery run
   acquires a NEW token and therefore writes a NEW artifact. That is correct:
   it genuinely is a separate extraction attempt and is recorded as one.
4. Event Raw batches use the same extraction=<token> discriminator but carry no
   business-date folder, because the batch spans whatever dates changed.
5. Raw is enumerable by Blob listing; no registry service is required.
```

An artifact written by a run whose fenced cursor commit failed is an abandoned extraction attempt. It is harmless — it is either a duplicate of, or superseded by, the winning run's artifact — and is garbage-collected by retention policy.

---

# 5. Raw Fan-out and Harmonization Batching

## 5.1 Why the Raw file and Harmonization work need different grains

A Raw Production file may contain one complete Windfarm-Day and therefore data for approximately 1,500 WTGs. One SQS message per Raw file would let only one Harmonizer pod process all Item-Days serially.

At the opposite extreme, one SQS message per Item-Day would create excessive queue volume and repeated Blob opens.

The design therefore separates three grains:

```text
Raw artifact grain
= Windfarm + business date + source type + extraction token

Correctness grain
= Item + Date + InputType          ← ItemDayInputState

SQS execution grain
= configurable batch of Item-Days
```

The middle grain is the **correctness boundary** and is the one that matters. A batch is only an execution envelope.

---

## 5.2 The extraction pod publishes directly; there is no dispatcher

With no external file-lifecycle service to poll, the discovery-and-claim stage disappears entirely. The extraction pod that wrote the Raw artifact publishes it:

```text
Extraction Pod
   write Raw (extraction=<token>)
        ↓
   SendMessage → raw-fanout SQS
        ↓
   fenced cursor advance
```

Example message:

```json
{
  "sourceType": "Production",
  "windfarmId": "WF01",
  "dataDate": "2026-06-03",
  "extractionToken": 41,
  "blobPath": "/raw/production/windfarm=WF01/date=2026-06-03/extraction=41/production.parquet"
}
```

No `RawDispatch` ledger is required, because the failure window it existed to close no longer exists. That window was *file claimed but message never created*; the cursor now covers it, since a crash before the send leaves the cursor un-advanced and the next cycle re-derives the work (Section 4.7).

---

## 5.3 Why SQS remains between Raw and Harmonization

SQS is retained purely as execution transport. It provides visibility-timeout redelivery, DLQ semantics, and independent consumer scaling.

```text
Extraction Pod
        ↓
raw-fanout SQS
        ↓
Raw Fan-out Coordinator
        ↓
harmonization-processing SQS
        ↓
Harmonizer Pods
```

If a consumer dies after receiving a message, redelivery provides execution recovery without any SQL bookkeeping.

---

## 5.4 Raw Fan-out Coordinator

The **Raw Fan-out Coordinator** consumes one Raw-artifact message, opens the Raw file, determines the affected Item-Days, and emits Item-Day batch messages directly onto the harmonization queue.

It does not perform Item-Day Harmonization itself, and it no longer writes a child ledger.

```text
Raw WF01 / 2026-06-03 / Production / extraction=41
        ↓
Raw Fan-out Coordinator
        ↓
enumerate T001 … T1500
        ↓
batch 1  = T001–T050
batch 2  = T051–T100
   …
batch 30 = T1451–T1500
        ↓
SendMessageBatch → harmonization-processing SQS
```

For event sources, the affected Item-Days are the distinct `(ItemId, UtcDate)` pairs derived from each event's item membership and time span.

Enumeration is **deterministic and repeatable** because the Raw artifact is immutable. A coordinator that crashes mid-enumeration is simply redelivered and re-enumerates the identical set; already-harmonized Item-Days become content-hash no-ops (Section 8.4). This is what makes the durable child ledger unnecessary: replay costs work, never correctness.

The Harmonization Dispatcher stage is removed for the same reason \u2014 with no pending child rows to claim, there is nothing for it to dispatch.

---

## 5.5 Harmonization batch message grain

```text
Correctness grain = Item-Day
Message grain     = Item-Day batch
```

Example Production message with a batch size of 50:

```json
{
  "sourceType": "Production",
  "windfarmId": "WF01",
  "dataDate": "2026-06-03",
  "inputType": "Production",
  "extractionToken": 41,
  "blobPath": "/raw/production/windfarm=WF01/date=2026-06-03/extraction=41/production.parquet",
  "batchId": 7,
  "itemIds": ["T0301", "T0302", "...", "T0350"]
}
```

For 1,500 WTGs and batch size 50:

```text
1 Raw Windfarm-Day artifact
→ 1,500 affected Item-Days
→ approximately 30 SQS Harmonization messages
```

The batch identifier is transport metadata only. Durable truth is `ItemDayInputState`.

Batch size is a performance-tuning knob, not a fixed architectural constant. Measure representative values such as 10, 25, 50, 100, and 200. Production is dense and predictable and may use larger batches; sparse or expensive event inputs may use smaller ones.

Batch size and SQS visibility timeout must be tuned **as one pair** \u2014 see Section 5.7.

---

## 5.6 Harmonizer batch processing

A Harmonizer pod consumes one Item-Day batch and reads the referenced bounded Raw file once for that batch.

```text
SQS batch
        ↓
Harmonizer Pod
        ↓
open referenced Raw artifact
        ↓
for each ItemId in batch:
        select that item's rows / affected dates
        normalize / merge / segment / validate
        commit the Item-Day component independently   (Section 8.4)
        set ItemDayInputState.Status = Complete
   on item failure:
        set ItemDayInputState.Status = Failed
        AttemptCount++, LastError
        continue with the next item
        ↓
ack the batch
```

Each Item-Day commits independently even though the message carries 50 of them. This is what keeps one bad turbine from failing forty-nine healthy ones.

The first implementation accepts the bounded Blob-read amplification of several batches reading the same Windfarm-Day file, because the Raw artifact is deliberately bounded. If benchmarks show Blob I/O dominates, evaluate source-side Parquet clustering / row-group pushdown or an explicit shard in the coordinator; do not make those optimizations mandatory initially.

---

## 5.7 Failure taxonomy

"The batch failed" is not an actionable statement. Three classes are distinguished, and only one of them ever justifies replaying the parent Raw artifact.

### 1. Item failure

Scoped to one `Item + Date + InputType`. Other Item-Days in the batch are unaffected.

```text
Batch: T001–T050

T001–T024  Complete
T025       Failed: source row violates schema constraint
T026–T050  Complete
```

Treatment:

```text
isolate the item, do NOT fail the batch
ItemDayInputState.Status = Failed, AttemptCount++, LastError
commit the other 49, ack the batch
retry via the reconciliation sweep (Section 5.8)
```

Critically, `Status = Failed` **blocks readiness** (Section 8.6), so a failed Item-Day cannot be silently calculated from its previous, now-stale component revision.

### 2. Batch transient failure

A failure that prevents the worker from processing the batch as a whole:

```text
Blob read timeout       network partition
SQL unavailable         pod termination / node restart
```

The message is left unacked and recovered by ordinary SQS redelivery. Nothing is recorded; this is normal operation.

### 3. Poison batch

A batch-level failure that persists until the automatic retry policy is exhausted and the message reaches the DLQ.

```text
Batch B17 / WF01 / 2026-09-01 / Production / T801–T850
ReceiveCount = 5
        ↓
harmonization-processing-dlq
        ↓
ALERT
```

Because item failures are isolated by construction, **a poison batch always indicates a batch-wide cause** \u2014 Raw artifact unreadable, authentication failure, SQL unavailable, invalid message schema, or a repeatedly killed worker. It is never a single bad row. That gives two non-overlapping signals with two distinct runbooks:

```text
DLQ alert                         → infrastructure incident
ItemDayInputState.Status = Failed → data-quality incident
```

### Retry policy and the visibility-timeout pairing

```text
maxReceiveCount     configurable, 5 as a starting point
visibilityTimeout   MUST exceed the p99 batch processing duration,
                    or be heartbeat-extended while the worker runs
```

A visibility timeout shorter than the batch duration will redeliver a batch that is succeeding, burn its receive count, and DLQ a healthy batch. Alerting fires on **DLQ arrival**, never on the first transient failure.

### Recovery rule

> **If a harmonization batch exhausts its automatic retry policy and enters the DLQ, or its processing outcome cannot be reliably reconstructed, initiate recovery. Prefer targeted Item-Day redispatch; if the affected children cannot be determined, replay the parent Raw fan-out message. Already-committed Item-Days are idempotent no-ops by `CanonicalContentHash`.**

Replaying the parent re-enumerates all 1,500 Item-Days and resends ~30 batches. That is the deliberate cost of removing the child ledger, and it is bounded and safe.

---

## 5.8 Harmonization reconciliation sweep

Isolating an item failure and acking the batch removes the message from the queue. Without a second mechanism, the failed Item-Day would never be retried \u2014 the batch that carried it is gone, and SQS cannot redeliver it.

Not acking is not an alternative: an item that fails permanently would then drive its whole batch to the DLQ, reintroducing exactly the coupling that per-item isolation exists to remove.

A sweep therefore closes the loop:

```text
SELECT WindfarmId, ItemId, DataDate, InputType
FROM   ItemDayInputState
WHERE  Status = 'Failed'
  AND  LastAttemptAtUtc < DATEADD(minute, -@backoff, SYSUTCDATETIME())
  AND  AttemptCount < @maxItemAttempts
        ↓
re-dispatch as small single/few-item harmonization batches
        ↓
AttemptCount exhausted
        ↓
alert, stop automatic retry, await remediation
```

This sweep is what replaces `RawFanOutWorkItem`'s retry responsibility. Without it, "prefer targeted Item-Day redispatch" degrades in practice to "always replay the parent Raw artifact".

Because the re-dispatched message must be able to re-read the source rows, `ItemDayInputState` retains the `blobPath` and `extractionToken` of the Raw artifact that last touched the Item-Day.

---

## 5.9 `FanOutFailure` — the pre-fan-out durable handle

There is one failure window that `ItemDayInputState` structurally cannot cover.

```text
Raw event batch
      ↓
raw-fanout SQS
      ↓
Fan-out Coordinator crashes BEFORE enumerating the artifact
```

At that moment the affected Item-Days are unknown, so there is no row to mark `Failed`. The two classes therefore behave very differently:

```text
post-fan-out failure   Status = Failed  → readiness blocks → cannot publish stale
pre-fan-out failure    DLQ message      → alert only       → nothing blocks
```

If a Raw outage batch is never enumerated, every affected Item-Day still reads `Complete` at its previous revision, the Package Coordinator proceeds, and calculation publishes with stale event data. Worse, the DLQ is not a permanent handle \u2014 SQS DLQ retention caps at 14 days, and after that nothing anywhere records that the artifact was never processed:

```text
the extraction cursor already advanced (the send succeeded, the consume failed)
        ↓ so re-extraction will not reproduce it
the Windfarm-Day barrier is already satisfied for a correction to a complete day
        ↓ so it never moves again
deterministic Raw paths make the artifact findable but not identifiable as unprocessed
```

The fix writes a durable record **only on the failure path**, so the happy path keeps its zero-SQL-write property:

```text
FanOutFailure
-------------
FanOutFailureId
RawPath
SourceType
WindfarmId
DataDate                 nullable for event batches
ExtractionToken
FirstFailedAtUtc
LastError
AttemptCount
ResolvedAtUtc

UNIQUE (RawPath)
```

A small consumer drains `raw-fanout-dlq` and upserts the row. It is the replay handle, it outlives DLQ retention, and it appears in the Section 14.7 data-availability view. An unresolved `FanOutFailure` row is an open incident:

```text
FanOutFailure open
        ↓
one or more Item-Days may be silently stale
        ↓
alert until replayed and ResolvedAtUtc is set
```

This is `RawFanOut` reduced to only the rows that carry information, and it is the one place SQL is re-added on the ingestion path.

---

## 5.10 Guarded counter updates

Counters must never be maintained by `COUNT(*)` over the underlying rows on the hot path, and must never be double-incremented by redelivery.

When an Item-Day reaches a terminal state, update the row and any dependent counter in the **same SQL transaction**, with the counter increment guarded by the row transition actually occurring:

```sql
BEGIN TRANSACTION;

UPDATE ItemDayInputState
SET Status = 'Complete',
    CompletedAtUtc = SYSUTCDATETIME()
WHERE WindfarmId = @windfarmId
  AND ItemId = @itemId
  AND DataDate = @dataDate
  AND InputType = @inputType
  AND Status <> 'Complete';

IF @@ROWCOUNT = 1 AND @inputType = 'Production'
BEGIN
    UPDATE WindfarmDayProductionState
    SET CompletedItemCount = CompletedItemCount + 1
    WHERE WindfarmId = @windfarmId
      AND DataDate = @dataDate;
END

COMMIT;
```

SQS redelivery therefore cannot double-increment a barrier. A full aggregate recount is reserved for reconciliation and repair, never the hot path.

This guarded-transition pattern is referenced by the Windfarm-Day barriers in Section 8.6 and by calculated publication in Section 11.2.

---

## 5.11 One health answer per Item-Day

`ItemDayInputState` is deliberately the single place that answers:

> Is `T0345 / Sep01 / Outage` healthy?

A separate processing-failure table is **not** introduced for identified Item-Days. It would have the same key and the same lifecycle, and would create two places to ask one question, which is how the two eventually disagree.

```text
identified Item-Day    → ItemDayInputState (revision authority + readiness)
not yet identified     → FanOutFailure + DLQ (Section 5.9)
```

---

## 5.12 Failure recovery matrix

| Failure point | Recovery mechanism |
|---|---|
| Extraction pod dies before acquiring the lease | Nothing happened; next cycle proceeds normally |
| Extraction pod dies after Raw write, before SQS send | Cursor un-advanced; next run re-derives and rewrites the same business day under a new extraction token |
| Extraction pod dies after SQS send, before cursor advance | Cursor un-advanced; work is replayed and absorbed as a content-hash no-op |
| Lease expires while a zombie pod is still running | Fenced cursor commit fails on `LeaseToken`; the zombie discards its run |
| Two pods attempt the same source | `ExtractionCursor` lease admits one; the other backs off |
| Raw Fan-out Coordinator dies before enumeration | `raw-fanout` redelivery retries the artifact |
| Raw Fan-out Coordinator dies mid-enumeration | Redelivery re-enumerates deterministically; already-committed Item-Days are no-ops |
| Raw fan-out reaches DLQ | `FanOutFailure` row created; alert until the artifact is replayed (Section 5.9) |
| One Item-Day fails inside a batch | Isolated; `ItemDayInputState.Status = Failed` blocks readiness; reconciliation sweep retries (Section 5.8) |
| Harmonizer pod dies mid-batch | Visibility timeout redelivers; committed Item-Days are no-ops |
| Harmonization batch reaches DLQ | Always a batch-wide infrastructure cause; alert, remediate, redispatch affected Item-Days or replay the parent |
| Item exhausts `MaxItemAttempts` | Automatic retry stops; alert; the Windfarm-Day barrier reports `1499 / 1500` |

The recovery responsibility is therefore:

> **`ExtractionCursor` + fenced `LeaseToken` protect source claiming and the ingestion handoff; SQS protects execution; `ItemDayInputState` is both the revision authority and the readiness gate for every identified Item-Day; `FanOutFailure` covers the window before Item-Day identity exists. Nothing publishes a calculation from an input that is known to be stale.**

---

# 6. Feature / Zone 2 — Transformation and Harmonization Zone

## 6.1 Responsibility

Convert Raw source data into complete, canonical, calculation-ready daily partitions.

The zone answers:

> What is the complete standardized input required to calculate this item and date?

Calculation workers do not normally read Raw.

---

## 6.2 Infrastructure

- AWS SQS `raw-fanout` queue (+ DLQ)
- AWS SQS `harmonization-processing` queue (+ DLQ)
- AWS SQS `summary` queue (+ DLQ)
- EKS Raw Fan-out Coordinator / Harmonizer Pods
- EKS Event Lifecycle Coordinator (clock-driven continuation + revision-driven correction, Section 6.11)
- EKS Summary Worker (Windfarm-Day serving projections, Section 8.6)
- Harmonization Reconciliation sweep (Section 5.8)
- Azure Blob Storage Harmonized Zone
- Azure SQL `ItemDayInputState`, `EventMonthState`, `EventMaterializationState`, `WindfarmDayEventReferenceState`, `FanOutFailure`

---

## 6.3 Harmonization responsibilities

The Harmonizer performs:

- Schema normalization
- Timestamp normalization
- UTC and local-time normalization
- Duplicate handling
- Invalid-record validation
- Source revision resolution
- Long-event daily segmentation
- Adjustment normalization
- Event precedence preparation
- Overlap normalization
- Calculation partition creation
- Lineage capture

---

## 6.4 Harmonized storage

Harmonized is organized around the **calculation unit of work**:

```text
Windfarm + Item + Date
```

However, the individual input components are versioned independently. A new calculation package does **not** copy every Parquet file into a new package revision.

Recommended structure:

```text
/harmonized/
├── item/
│   └── windfarm=WF01/
│       └── item=T001/
│           └── date=2026-08-20/
│               ├── production/
│               │   ├── revision=1/
│               │   │   └── production-10minute.parquet
│               │   └── revision=2/
│               │       └── production-10minute.parquet
│               │
│               ├── outage/
│               │   ├── revision=1/
│               │   │   └── outage-segments.parquet
│               │   └── revision=2/
│               │       └── outage-segments.parquet
│               │
│               ├── curtailment/
│               │   └── revision=3/
│               │       └── curtailment-aggregated.parquet
│               │
│               ├── alarm/
│               │   └── revision=2/
│               │       └── alarm-segments.parquet
│               │
│               ├── adjustment/
│               │   └── revision=4/
│               │       └── adjustment-segments.parquet
│               │
│               └── packages/
│                   ├── revision=18/
│                   │   └── manifest.json
│                   └── revision=19/
│                       └── manifest.json
│
├── events/
│   ├── outage/
│   │   └── windfarm=WF01/startyear=2026/startmonth=08/
│   │       ├── current/outage-events.parquet
│   │       └── revisions/
│   │           └── revision=7/
│   │               └── attempts/token=42/outage-events.parquet   ← CommittedAttempt
│   ├── alarm/
│   │   └── windfarm=WF01/startyear=2026/startmonth=08/
│   │       ├── current/alarm-events.parquet
│   │       └── revisions/revision=5/attempts/token=17/alarm-events.parquet
│   ├── curtailment/
│   │   └── windfarm=WF01/startyear=2026/startmonth=08/
│   │       ├── current/curtailment-events.parquet
│   │       └── revisions/revision=4/attempts/token=9/curtailment-events.parquet
│   └── adjustment/
│       └── windfarm=WF01/startyear=2026/startmonth=09/
│           ├── current/
│           │   ├── adjustments.parquet            ← one row per logical adjustment
│           │   └── adjustment-items.parquet       ← AdjustmentId → ItemId membership
│           └── revisions/revision=3/attempts/token=5/
│               ├── adjustments.parquet
│               └── adjustment-items.parquet
│
└── windfarm/
    └── windfarm=WF01/
        └── date=2026-08-20/
            ├── current/                                   ← queried by Synapse
            │   ├── daily-windfarm-wtgs-production.parquet
            │   ├── daily-item-event-references.parquet
            │   └── manifest.json
            └── revisions/
                └── revision=18/
                    ├── daily-windfarm-wtgs-production.parquet
                    ├── daily-item-event-references.parquet
                    ├── manifest.json                          ← contributing item/event revisions
                    ├── data-availability-summary.parquet      ← optional future aggregate
                    └── other-reusable-aggregate.parquet       ← optional future aggregate
```

The Item-Day area contains independently versioned calculation-input components.

A **package revision** is a lightweight immutable manifest that identifies the exact component revisions to use together for one calculation.

Example:

```json
{
  "packageRevision": 18,
  "windfarmId": "WF01",
  "itemId": "T001",
  "date": "2026-08-20",
  "configVersion": 7,
  "calculationVersion": "2.4.1",
  "tzdbVersion": "2026a",
  "production": {
    "revision": 2,
    "path": "production/revision=2/production-10minute.parquet"
  },
  "outage": {
    "revision": 1,
    "path": "outage/revision=1/outage-segments.parquet"
  },
  "curtailment": {
    "revision": 3,
    "path": "curtailment/revision=3/curtailment-aggregated.parquet"
  },
  "alarm": {
    "revision": 2,
    "path": "alarm/revision=2/alarm-segments.parquet"
  },
  "adjustment": {
    "revision": 4,
    "path": "adjustment/revision=4/adjustment-segments.parquet"
  }
}
```

This gives the Calculation Worker one deterministic package reference without copying unchanged Parquet files.

`configVersion` and `calculationVersion` are part of the package because Section 2.5 defines determinism as component revisions **plus** configuration and code version. This also allows a configuration change or a calculation code change to mint a new package revision with unchanged component revisions, which is how those triggers in Section 8.5 advance `RequestedPackageRevision`.

The Windfarm-Day area remains for reusable analytical aggregates.

Key rules:

```text
1. Component revisions are immutable.
2. Each component revision is a complete snapshot for that Item-Day.
3. Only the changed component receives a new Parquet revision.
4. Package revisions are lightweight manifests of component references.
5. Calculation reads the package manifest, not "latest" files discovered at runtime.
6. A new component revision is written under an exclusive lease for
   (WindfarmId, ItemId, DataDate, InputType) — see Section 8.4.
7. Package identity includes configuration version and calculation version.
8. Item-Day component files are addressed by explicit path only.
   No component wildcard scan and no query-engine external table may span
   the revision directories.
9. Every revisioned artifact is physically written under a token-scoped
   attempt path and resolved as revision + CommittedAttempt (Sections 8.4,
   6.10.1). The Item-Day nodes above are drawn without the attempt level
   only to keep the tree readable; the events nodes show it explicitly.
   Anything under a revision that is not the CommittedAttempt is an
   abandoned attempt and is garbage-collected.
```

---

### 6.4.1 Current-revision resolution and query-engine access

`revision=N` is a physical directory, **not** a queryable partition column.

A wildcard external table over an Item-Day component folder would read every historical revision and silently double count. Harmonized therefore has two distinct access modes:

```text
Calculation workers
  → read exact component paths named in an immutable package manifest
  → never resolve "latest" from Blob at runtime

Query engine (serving)
  → reads the Harmonized Windfarm-Day daily aggregates only
  → never scans /harmonized/item/**
  → never scans harmonized 10-minute detail
```

For **Item-Day calculation components**, Azure SQL `ItemDayInputState.ComponentRevision` is the authoritative current revision. Do not maintain a second mutable `_current.json` pointer for every Item-Day component.

Example:

```text
ItemDayInputState
WindfarmId        = WF01
ItemId            = T001
DataDate          = 2026-08-20
InputType         = Outage
ComponentRevision = 2
Status            = Complete
```

The Package Coordinator resolves the current component revisions from `ItemDayInputState` and writes those exact immutable paths into `manifest.json`.

This avoids dual-current-state coordination such as:

```text
SQL says revision 2
but
Blob _current.json says revision 1
```

For **Windfarm-Day serving datasets**, no JSON pointer is used either. Synapse Serverless cannot resolve a path out of a JSON document and use it as an external table location without a second dynamic-SQL step, and a pointer file would reintroduce exactly the dual-current-state problem described above.

Instead the current revision is exposed as a **physical projection** at a stable path:

```text
/harmonized/windfarm/
  windfarm=WF01/
    date=2026-08-30/
      current/
        daily-windfarm-wtgs-production.parquet     ← queried by Synapse
      revisions/
        revision=11/
          daily-windfarm-wtgs-production.parquet
          manifest.json
        revision=12/
          daily-windfarm-wtgs-production.parquet
          manifest.json
```

Synapse then always queries one stable, wildcard-safe location:

```sql
.../windfarm=*/date=*/current/daily-windfarm-wtgs-production.parquet
```

No JSON resolution, no dynamic SQL, no revision-spanning scan.

### Authority and reconciliation

```text
Azure SQL WindfarmDayProductionState.WindfarmDayRevision
  = authoritative current revision

Blob current/
  = derived, queryable projection of that revision
```

Publication order:

```text
1. Write revisions/revision=N+1/ (data + contributor manifest) and validate it.
2. Advance WindfarmDayProductionState.WindfarmDayRevision to N+1 in SQL. ← commit point
3. Overwrite current/ from revisions/revision=N+1/.
4. Mark the SummaryWorkItem Completed (Section 8.6).
```

Step 3 is idempotent and replayable, so a crash between steps 2 and 3 leaves `current/` merely stale, never wrong-by-ambiguity. The contributor manifest inside `current/` carries its `windfarmDayRevision`, so divergence is detectable:

```text
Reconciliation job:
compare WindfarmDayProductionState.WindfarmDayRevision
     to the windfarmDayRevision recorded in current/
if they differ → republish current/ from the authoritative revision
```

SQL always wins. `current/` is never a source of truth and is always rebuildable from `revisions/`.

The same relationship holds for the Calculated Windfarm-Day summaries, where `WindfarmDayCalculationState.CalculatedSummaryRevision` is the authority (Section 8.6).

A revision directory that exists but is not referenced by `ItemDayInputState`, a package manifest, or the Windfarm-Day state tables is not current and may be garbage-collected according to retention policy.

**Serving grain.** The Item-Day component layout is a calculation-input contract, not a serving layout. At thousands of items over multiple years, five components per Item-Day can produce many very small Parquet files; per-file listing and footer overhead would dominate range scans.

The query engine therefore reads **only the `current/` projection of the Harmonized Windfarm-Day serving datasets** for range queries: the daily production aggregate and the lightweight daily event-reference projection. Canonical monthly event files are read only when the UI needs the full event details for the EventIds discovered from those daily references.

Harmonized 10-minute detail is **not** compacted into a serving copy and is **not** queried by the query engine. It stays exclusively in the Item-Day calculation-input layout, read only by calculation workers via manifest paths. This keeps the largest dataset in the platform stored exactly once and keeps every range query on the day grain, where one file per windfarm-day bounds the scan.

Partial-day and 10-minute drill-down is handled as a bounded point read rather than a scan — see Section 12.6.

### 6.4.2 Partition calendar

All `date=` partitions in Raw, Harmonized and Calculated are **UTC dates**. Local-time attributes are carried as columns on the rows, never as the partition key.

Consequences that must be honoured:

```text
A UTC day always contains 144 × 10-minute intervals.
The expected-interval completeness rule in Section 8.6 is therefore constant.
Local-day and DST-affected reporting is a query-time grouping over local columns,
not a storage decision.
```

Without this rule a DST local day contains 138 or 150 intervals and the readiness rule in Section 8.6 would raise false incompleteness twice a year.

### 6.4.3 Reporting calendar — UTC and Local

Partitioning by UTC does not remove the requirement to report on a **local** calendar. These are three separate concerns and must not be conflated:

```text
Instant identity      always UTC — one canonical instant per interval
Partitioning          UTC date — always exactly 144 intervals
Reporting calendar    local date — 23, 24 or 25 hours depending on DST
```

On **10-minute** datasets, local time is an ordinary per-interval attribute: carry `StartTimeLocal` and `EndTimeLocal` as columns alongside the UTC pair. The Harmonizer computes them once.

On **daily aggregates**, local time may **not** be expressed as an additional sum column. A measure is calendar-independent — the energy in an interval does not change according to what the timestamp is called. What differs between calendars is *which intervals belong to the day*:

```text
UTC day 2026-01-15 (Europe/Copenhagen, UTC+1)
  = local 15 Jan 01:00 → 16 Jan 01:00
  ├─ 23 h belongs to LOCAL 15 Jan
  └─  1 h belongs to LOCAL 16 Jan

Local day 15 Jan
  = 23:00 UTC 14 Jan → 23:00 UTC 15 Jan
  ├─  1 h comes from the UTC-14 partition
  └─ 23 h comes from the UTC-15 partition
```

A local-day total therefore requires intervals that physically live in a **different UTC partition**. No column on a UTC-keyed row can supply them.

The local date is consequently part of the aggregate **key**, never a parallel measure column:

```text
… + UtcDate + LocalDate
```

Most UTC days yield two rows per item — the fragment before local midnight and the fragment after — and `ValidIntervalCount` across them still sums to 144. Where the offset is zero the pair collapses naturally to one row, and a DST day needs no special case: a 23- or 25-hour local day is simply what grouping by `LocalDate` produces.

```sql
GROUP BY UtcDate      -- UTC calendar, exact
GROUP BY LocalDate    -- local calendar, exact
```

### Querying a local-date range

Each daily file contains only rows whose `UtcDate` equals its `date=` partition. A local day therefore draws from **two adjacent UTC partitions**, so a local-calendar request must widen the partition predicate before filtering:

```sql
-- 1. widen the UTC range: this drives PARTITION PRUNING (which files are opened)
WHERE UtcDate BETWEEN DATEADD(day,-1,@D1) AND DATEADD(day,1,@D2)
-- 2. restrict to the requested calendar: this drives which rows COUNT
  AND LocalDate BETWEEN @D1 AND @D2
GROUP BY LocalDate
```

The cost is bounded and does not scale with range length, because adjacent local days share the same files:

```text
request          UTC files read     UTC-calendar equivalent
1 local day      2 (or 3 generic)   1
30 local days    31 (or 32)         30
365 local days   366 (or 367)       365
```

Which side must be widened depends on the sign of the offset, and the timezone is already resolved from configuration:

```text
UTC+1 (Copenhagen)  local day D = 23:00 UTC D-1 → 23:00 UTC D   → need D-1 … D
UTC-5 (New York)    local day D = 05:00 UTC D   → 05:00 UTC D+1 → need D   … D+1
```

An offset-aware API widens only the side it needs (`N+1` files); widening both sides unconditionally is also correct (`N+2`) because the extra partition's rows simply fail the `LocalDate` filter.

```text
MANDATORY: the widening must be applied to the partition/path predicate,
not only to the row predicate. Filtering on LocalDate alone while leaving the
path wildcarded defeats partition pruning and scans the entire dataset.
```

### Trailing-edge local-day completeness

A local day at the trailing edge cannot be complete until the **next** UTC partition is published. For a UTC+1 windfarm, local day D is only whole once `date=D` exists, because its final hour lives there.

```text
All available UTC partitions Complete
but the next UTC day not yet published
        ↓
the most recent local day is legitimately PARTIAL
```

This figure must be labelled with the same `IsPartial` treatment Section 6.9 applies to missing turbines. Otherwise the most-watched number on a live dashboard silently reads low every evening for the duration of the offset.

### Timezone resolution and tzdb versioning

```text
1. "Local" is per windfarm, not global. The timezone resolves from versioned
   configuration for (WindfarmId, effective date), like ExpectedItemCount in
   Section 8.6, so a windfarm never retroactively changes timezone across history.
2. The IANA tzdb version is pinned and recorded as `tzdbVersion` in the package
   manifest (Section 6.4).
3. A tzdb upgrade mints new package revisions exactly like a configuration change.
```

Rule 2 exists because DST rules genuinely change — jurisdictions abolish DST or move transition dates. An unpinned tzdb upgrade would silently reassign the `LocalDate` of historical intervals and change an already-published historical figure, violating the determinism requirement in Section 2.5.

---
## 6.5 Long-event daily segmentation

Original outage:

```text
15 Jan 12:40 → 25 Jan 04:50
```

Harmonized segments:

```text
15 Jan 12:40 → 16 Jan 00:00
16 Jan 00:00 → 17 Jan 00:00
17 Jan 00:00 → 18 Jan 00:00
...
24 Jan 00:00 → 25 Jan 00:00
25 Jan 00:00 → 25 Jan 04:50
```

Each segment retains:

```text
EventId
OriginalStart
OriginalEnd
EffectiveStart
EffectiveEnd
CalculationDate
SourceRevision
```

### Fan-out consistency and recovery

One late or corrected long event touches every UTC day it spans. A ten-day outage can therefore produce ten new component revisions and ten new package revisions.

The consistency boundary is **Item-Day**, not the entire multi-day event range. The system does not attempt a distributed transaction across Blob Storage and SQL for all affected days.

Processing rule:

```text
1. Compute all affected daily segments for the source event.
2. Write every affected day's candidate component revision first.
3. After the candidate files are durable, advance each affected
   ItemDayInputState row using the component lease / optimistic-concurrency rules.
4. Advance Item-Day state in a deterministic order, for example oldest date first.
5. The Package Coordinator reacts only to committed ItemDayInputState changes,
   never to file appearance alone.
```

If the worker fails after some days have committed, those Item-Days remain individually consistent and the remaining days are retried by SQS redelivery.

Because component merging is a keyed idempotent upsert (Section 8.4), replaying an already-applied day is a no-op.

Important semantic:

> A multi-day outage is **not published atomically as one range**. Each Item-Day is independently committed and recalculated. Temporary mixed old/new state across different days is acceptable during recovery, but no individual Item-Day package may reference a half-written component revision.

### Source-driven fan-out alone cannot cover an open event

The processing rule above is triggered by an arriving source row: a changed event computes all affected daily segments and dispatches them. That is complete only for an event with a **known end**.

An open event has no terminal day. Once its most recent source update has been processed, no further source row arrives, so no further fan-out is triggered — yet the event keeps overlapping each new UTC day as the clock advances:

```text
01 Sep   O1001 opens, source row arrives    → Sep01 outage-segments written
02 Sep   still open, no source change       → no fan-out
                                            → Sep02 outage-segments MISSING
03 Sep   still open, no source change       → Sep03 outage-segments MISSING
```

The consequence is not confined to serving. Item-Day components are **calculation inputs**, so every un-materialized day understates that day's lost production. The failure is silent in the worst way: the Item-Day is `Complete` and simply contains no outage, which is indistinguishable from a genuinely healthy turbine.

Open events therefore need a second, **clock-driven** trigger alongside the source-driven one. Both are owned by `EventMaterializationState` and the Event Lifecycle Coordinator in Section 6.11.

### Data first, watermark second

Section 6.11 tracks how far each event has been materialized. That watermark is a scheduling hint, never an authority, and the ordering is a hard invariant extending step 3 above:

```text
write candidate component revision
        ↓
fenced ItemDayInputState commit
        ↓
ONLY THEN advance MaterializedThroughDate
```

The asymmetry is the whole point:

```text
watermark lags     → the day is re-materialized; the merge is an idempotent
                     keyed upsert (Section 8.4), so the replay is a no-op

watermark leads    → the sweep believes the day is done and never revisits it
                     → permanently missing input → silently wrong calculation
```

A watermark that advances before the component commit converts a crash into permanent data loss, so it must only ever be conservative.

### An emptied component is published, never deleted

A correction can legitimately remove the last row from an Item-Day component — an event is deleted, shrunk past that day, or an item is removed from an adjustment's membership. The cleanup must publish a new component revision containing **zero rows**:

```text
/harmonized/item/windfarm=WF01/item=T005/date=2026-09-03/
  adjustment/revision=8/adjustment-segments.parquet     ← 0 rows

ItemDayInputState
  ComponentRevision = 8
  Status            = Complete
  RecordCount       = 0
```

Deleting the file instead would be indistinguishable from `not processed yet`, `failed`, `missing` or `never applicable`. This is the same reasoning as the explicit Raw tombstone rule in Section 4: authoritative emptiness is a fact and must be recorded as one.

---

## 6.6 Interval semantics

Use half-open intervals consistently:

```text
[start, end)
```

Overlap is calculated using:

```text
overlapSeconds =
max(
    0,
    min(eventEnd, bucketEnd)
    -
    max(eventStart, bucketStart)
)
```

This avoids double counting at exact boundaries.

---

## 6.7 10-minute explosion

The initial design should keep Harmonized events as canonical daily intervals rather than pre-exploding every event into 10-minute records.

Example:

```text
Outage segment:
12:43 → 18:17
```

The Calculation Zone joins these intervals to the existing 10-minute production buckets.

If performance testing later proves that pre-exploded intervals are significantly faster, the internal Harmonized representation may be changed without altering the overall architecture.

## 6.8 Adjacent curtailment aggregation

Harmonized contains a reusable curtailment aggregate that merges adjacent source intervals when they represent the same continuous business event.

Example:

```text
10:00–10:10
10:10–10:20
10:20–10:30
10:40–10:50
```

If the first three records have compatible business attributes:

```text
10:00–10:30
10:40–10:50
```

Merge only when the records have compatible calculation-relevant attributes, including the same windfarm, turbine and curtailment classification, and the previous end exactly matches the next start.

For calculation, the aggregated curtailment is stored inside the Item-Day Harmonized package:

```text
/harmonized/item/
  windfarm=WF01/
    item=T001/
      date=2026-01-01/
        curtailment/
          revision=3/
            curtailment-aggregated.parquet
```

Suggested fields:

```text
WindfarmId
WtgId
EffectiveStartUtc
EffectiveEndUtc
CurtailmentTypeId
SourceEventCount
FirstSourceEventId
LastSourceEventId
SourceRevision
```

This belongs in Harmonized because it is source canonicalization, not a calculation output.

## 6.9 Daily windfarm WTG production aggregate

The Harmonizer produces one reusable daily production aggregate file per **Windfarm + Date + Windfarm-Day Revision**.

Suggested path:

```text
/harmonized/windfarm/
  windfarm=WF01/
    date=2026-01-01/
      revisions/
        revision=18/
          daily-windfarm-wtgs-production.parquet
          manifest.json
      current/
        daily-windfarm-wtgs-production.parquet
```

### Windfarm-Day revision semantics

The Windfarm-Day revision is **its own monotonic counter**. It is not the item package revision and shares no number space with it. Reusing the item revision number would be meaningless, because the aggregate derives from N turbines that each carry independent component revisions.

Each Windfarm-Day revision is accompanied by a manifest naming the exact turbine production component revisions it consumed:

```json
{
  "windfarmDayRevision": 18,
  "windfarmId": "WF01",
  "date": "2026-01-01",
  "contributors": [
    { "itemId": "T001", "productionRevision": 2 },
    { "itemId": "T002", "productionRevision": 1 }
  ],
  "expectedItemCount": 2,
  "completeItemCount": 2,
  "membershipVersion": 7,
  "missingItemIds": [],
  "isPartial": false
}
```

Recompute rules:

```text
1. The aggregate is normally built when every expected WTG for that windfarm-day
   has Production Complete in ItemDayInputState — the production barrier in
   Section 8.6.
2. Any later production component revision for any contributing WTG
   invalidates the aggregate and re-requests a new Windfarm-Day revision.
   It does NOT decrement the barrier counter (Section 8.6).
3. The aggregate is generated by the Summary Worker from a SummaryWorkItem,
   never inline by the Item-Day worker that tripped the barrier.
4. current/ is republished only after the new revision and its contributor
   manifest are durable and WindfarmDayProductionState has been advanced
   (Section 6.4.1).
5. A stale Windfarm-Day revision must never be served, because Section 12.4
   uses this dataset as the denominator of LossPercentage.
```

Without rule 2 a single late turbine day leaves a stale windfarm denominator while the numerator in the Calculated Zone has already been recalculated, producing a silently wrong loss percentage.

### Partial publication

Waiting unconditionally for all WTGs would let one permanently failing turbine extraction blackhole an entire windfarm-day: with 99 of 100 turbines complete, no aggregate exists and the UI has no windfarm metrics at all for that date.

A completeness deadline therefore bounds the wait:

```text
100 / 100 before the deadline
  → Status = Complete, IsPartial = false, publish

99 / 100 at the deadline
  → Status = Partial, IsPartial = true, publish anyway
  → missingItemIds recorded in the manifest
```

A partial revision is superseded normally when the missing data arrives:

```text
99/100 Partial (revision 18)
     ↓ late production for T100
100/100
     ↓
revision 19, Status = Complete, IsPartial = false
     ↓
current/ republished
```

Rules for partial revisions:

```text
1. IsPartial and MissingItemCount are carried on the served rows or manifest
   so the API and UI can label the figure as incomplete.
2. A partial windfarm-day must be visible in the Section 14.7 data-availability
   view and must raise an alert, not fail silently.
3. A partial revision is never treated as a final published figure for reporting;
   it is an operational best-effort value.
```

The file contains one row per WTG per calendar-day fragment.

Logical grain:

```text
WindfarmId + WtgId + UtcDate + LocalDate
```

`LocalDate` is part of the key, not a measure column — see Section 6.4.3. A UTC day normally produces two rows per WTG whose `ValidIntervalCount` sums to 144.

Suggested additive fields:

```text
WindfarmId
WtgId
UtcDate
LocalDate
TotalActualProductionKWh
TotalPossibleProductionKWh
ActivePowerSum
ActivePowerCount
ValidIntervalCount
MissingIntervalCount
ExpectedIntervalCount
SourceRevision
```

There is deliberately **one** `TotalActualProductionKWh`, not a UTC/Local pair. The measure is calendar-independent; the calendar is selected by the grouping key.

The same dataset supports all four combinations:

```text
WTG-level daily queries
→ filter by WtgId

Windfarm-level daily queries
→ SUM across all WTG rows

UTC-calendar reporting
→ GROUP BY UtcDate

Local-calendar reporting
→ GROUP BY LocalDate
```

A separate daily windfarm-total file is **not required initially** because Synapse Serverless can efficiently aggregate the already-small WTG daily rows.

If performance measurements later show a genuine need for another windfarm-level materialized aggregate, it can be added under the same Windfarm-Day folder without changing the overall storage model.



## 6.10 Event-serving model for outage, alarm, curtailment and adjustment

Calculation and UI serving need different physical representations of event data.

```text
Calculation representation
= Item-Day segments / aggregates under /harmonized/item/**

UI event representation
= one canonical logical event row in /harmonized/events/**

Daily range-discovery representation
= lightweight EventId references in the Windfarm-Day current/ projection
```

The event-serving representation must preserve the original logical event rather than reproducing the Item-Day segmentation used for calculation. A two-hour outage, a six-day outage and an event with no known end date are all represented as one logical event row.

Suggested common fields:

```text
EventId
WindfarmId
ItemId
EventType                 Outage | Alarm | Curtailment | Adjustment
OriginalStartUtc
OriginalEndUtc            nullable while the event is open
Status                    Open | Closed | Cancelled, as applicable
SourceRevision
EventRevision
SourceSystem
```

Type-specific attributes such as outage reason, alarm code, curtailment classification or adjustment reason remain on the appropriate event dataset.

### 6.10.1 Monthly canonical event storage

Canonical events are physically grouped by the month in which the logical event **starts**. The full event is stored once; it is not copied into every month or every day that it overlaps.

Example:

```text
Outage O1001
Start = 2026-08-28 10:00 UTC
End   = 2026-09-03 14:00 UTC

stored under:
/harmonized/events/outage/
  windfarm=WF01/
    startyear=2026/
      startmonth=08/
        current/outage-events.parquet
```

An open event is valid with:

```text
OriginalEndUtc = NULL
Status = Open
```

When an open event later closes, or when its source attributes are corrected, the corresponding monthly event partition receives a new immutable revision and its `current/` projection is republished. Event identity remains stable through `EventId`.

### Authority, lease and fencing

A monthly event partition is a read-modify-write of shared state, exactly like an Item-Day component (Section 8.4), so it needs the same explicit SQL authority rather than an unattributed "fenced publication".

```text
EventMonthState
---------------
WindfarmId
EventType
StartYear
StartMonth

CurrentRevision
CommittedAttempt
CanonicalContentHash

RequestedGeneration
PublishedGeneration

LeaseOwner
LeaseToken
LeaseExpiresAt

Status
PublishedAtUtc
LastError

UNIQUE (WindfarmId, EventType, StartYear, StartMonth)
```

The lease key is identical to the unique key. Publication follows the Section 8.4 pattern without deviation:

```text
1.  Acquire the EventMonthState lease           → LeaseToken = 42
2.  Read the authoritative CurrentRevision
3.  Merge changed events and tombstones, keyed on EventId
4.  Canonicalize the resulting logical rows
5.  Compute CanonicalContentHash
6.  Hash unchanged → no-op; no new revision, no republish
7.  Hash changed  → write revisions/revision=<N+1>/attempts/token=42/
8.  Validate the candidate
9.  Fenced SQL commit:
        WHERE LeaseToken = 42 AND CurrentRevision = N
10. Advance CurrentRevision to N+1
11. Republish current/ from the committed attempt
```

Step 9 is what prevents two Harmonizers, both processing late corrections to the same August outage month, from both reading revision 7 and both writing revision 8. Step 11 is idempotent and replayable, so a crash between 10 and 11 leaves `current/` stale, never ambiguous — the same authority rule as Section 6.4.1. **SQL always wins; `current/` is always rebuildable from `revisions/`.**

### The content-hash no-op is mandatory, not an optimization

The safety-lag re-read (Section 4.4.1) deliberately re-delivers unchanged source rows on every cycle, and the current month is touched by every cycle. Without the hash rule, the current month partition would advance a revision per cycle for no semantic change:

```text
09:00 extraction   O1001 = Open              hash = ABC   → revision 7
09:15 safety-lag   O1001 = Open, identical   hash = ABC   → NO revision 8
                                                          → no current/ rewrite
                                                          → no reference invalidation

03 Sep source update:
      O1001 EndUtc NULL → 2026-09-03 14:20   hash = XYZ   → revision 8
```

The failure mode this prevents is not merely wasteful, it is pathological:

```text
safety-lag duplicate
        ↓
event month revision++
        ↓
every daily projection citing that month considered stale
        ↓
SummaryWorkItems
        ↓
needless Blob writes + Synapse projection churn on every cycle
```

As in Section 8.4, the hash is computed over the canonicalized logical rows — `EventId`, business columns, tombstone flags, in a deterministic sort order — never over Parquet bytes.

For curtailment, the canonical logical event is the result of the adjacent-10-minute merge defined in Section 6.8. The Item-Day `curtailment-aggregated.parquet` remains the calculation input; the event-serving projection provides the logical event shape needed by the UI.

For adjustment, Azure SQL remains the transactional authoring truth. The Harmonized event projection is a lake-serving/replay representation only and does not replace the SQL Adjustment/Audit/Outbox model in Section 7.

### 6.10.2 Daily event-reference projection

A monthly event-start partition alone cannot answer a day-range UI query safely. An August outage can continue into September, and an open August event can remain active for many later months. The query engine must not be expected to infer this by scanning only the requested month's event-start folders.

Each Windfarm-Day serving revision therefore contains a lightweight event-reference file:

```text
/harmonized/windfarm/
  windfarm=WF01/
    date=2026-09-01/
      current/
        daily-item-event-references.parquet
```

Suggested grain and fields:

```text
WindfarmId
ItemId
UtcDate
LocalDate
EventType
EventId
LifecycleStatus          Open | Closed | Deleted (Section 6.11)
EffectiveStartUtc
EffectiveEndUtc          bounded to this UTC Item-Day; NULL when the event is
                         still open at the end of that day
EventStartYear
EventStartMonth
EventRevision
```

`LifecycleStatus` lets the UI distinguish "this outage ran all day and is finished" from "this outage is still running" without a second lookup. For an event still open at the end of the referenced day, `EffectiveEndUtc` is NULL rather than clamped to midnight: clamping would assert an end time that no source has confirmed, which is exactly the fabrication the open-event rules below prohibit.

The same `EventId` appears once for every Item-Day that it overlaps. Only the reference is repeated; the full event attributes remain stored once in the canonical monthly event dataset.

For example:

```text
O1001  28 Aug 10:00 → 3 Sep 14:00

Daily references:
28 Aug → O1001
29 Aug → O1001
30 Aug → O1001
31 Aug → O1001
01 Sep → O1001
02 Sep → O1001
03 Sep → O1001
```

An open event continues to receive a reference for each newly processable day until a committed source revision closes or cancels it. The event's `OriginalEndUtc` remains NULL in the canonical event dataset while open; the daily reference carries only the bounded overlap for that Item-Day.

#### Processable day, defined

"Processable" is not "today". All physical `date=` partitions are **UTC** dates (Section 6.4.3), so the bound must be expressed in UTC or it will disagree with the storage grain:

```text
CalculationProcessableDate
= the latest fully elapsed UTC date whose configured
  source-completeness delay has also elapsed
```

The delay is a per-source ingestion-completeness setting. It is deliberately **not** the extraction `SafetyLagInterval` of Section 4.4.1, which is a cursor re-read window rather than a statement about when a day's data is settled; reusing one for the other couples two independent tuning decisions.

#### The current UTC day is served, but not calculated

An open event genuinely changes every cycle, because its bounded overlap keeps extending. If the current day were materialized as an Item-Day component, `CanonicalContentHash` could not suppress it — the content really has changed — so every sweep would mint a component revision, a package revision and a recalculation for every open event on every windfarm.

The two concerns are therefore split at `CalculationProcessableDate`:

```text
UTC day <= CalculationProcessableDate
        → materialized as Item-Day components
        → feeds calculation
        → event references derived from those committed components

Current UTC day, open events only
        → NOT materialized as Item-Day components
        → no calculation
        → event references projected directly from EventMaterializationState
          plus the canonical current event (Section 6.11)
```

This keeps calculation inputs stable and cheap while the UI can still show that an outage is happening right now. It also means the daily reference projection has **two** build sources, and the Summary Worker must not assume the first one alone (Section 8.6).

The consequence for the current day is that its reference rows carry `LifecycleStatus = Open` and `EffectiveEndUtc = NULL`, and the UI resolves full detail from the canonical monthly event where `OriginalEndUtc` is likewise NULL. Nothing anywhere claims the event has ended.

#### Open events: unbounded by design, monitored operationally

There is deliberately **no technical maximum** on how long an open event may keep generating daily references. A genuine outage can remain open for weeks, and truncating it would change business truth. Correctness and anomaly detection are therefore separated:

```text
Correctness            keep emitting a daily reference every processable day
                       until an authoritative source revision closes or cancels

Operational anomaly    OpenEventAgeThreshold, configurable per EventType
```

```text
Status = Open
AND StartUtc < now − OpenEventAgeThreshold(EventType)
        ↓
OpenEventAnomaly
        ↓
alert / data-quality dashboard (Section 14.7)
```

Indicative thresholds — alarms are expected to be short-lived, outages are not, and an adjustment may be required to carry an explicit end:

```text
Alarm         warn after 24–72 h depending on alarm type
Outage        warn after the configured operational threshold
Adjustment    may require EndUtc, or carry its own maximum-duration policy
```

The alert is strictly advisory:

```text
the alert does NOT auto-close the event
the alert does NOT stop daily reference generation
```

Either behaviour would silently fabricate a business fact.

The daily reference is a **serving index**, not a calculation result. It must not contain:

```text
TotalLostProductionKWh
TotalOtherLossKWh
LossPercentage
```

Those values belong to the Calculated Zone.

### 6.10.3 Publication and invalidation

The Windfarm-Day event-reference projection follows the same immutable-revision/current-projection pattern as the production aggregate. A late event, corrected event, newly closed open event or changed adjustment that affects a previously published day invalidates that day's event-reference projection and requests a new Windfarm-Day serving revision.

### Invalidation is keyed by event identity, not by month revision

The monthly partition is far too coarse to be the dependency boundary. If August holds 10,000 events and only `O9000` changes — affecting `T900` on 4 August — then invalidating every daily projection that cites "outage August revision 8" would rebuild an entire month of serving days for a single-day change.

Two boundaries are therefore distinguished and must not be conflated:

```text
EventMonthState (WindfarmId, EventType, StartYear, StartMonth)
    = physical publication and concurrency boundary

EventId + EventRevision
    = semantic dependency identity
```

The contributor manifest therefore records the exact **event-level** contributors, not the month revision:

```text
daily-item-event-references contributor manifest

O1001  eventRevision 2
O1007  eventRevision 4
A3001  eventRevision 1
```

Invalidation then propagates only as far as the change actually reaches:

```text
event O1001 changes
        ↓
derive affected Item-Days from the union rule below
        ↓
re-request ONLY those Windfarm-Day event-reference projections
```

The union rule is the same one used for event corrections in Section 15.2: shrinking an event must invalidate the days it no longer covers, not only the days it still does.

### One trigger, two records

There must be a single invalidation path, or two mechanisms can disagree about whether a day is stale. The derivation of affected Item-Days belongs to the **Event Lifecycle Coordinator** (Section 6.11), which is already computing exactly that set in order to rebuild the calculation components. The event-reference projection then follows the resulting Item-Day component revisions like any other consumer:

```text
Item-Day event component revisions   build inputs, fencing, invalidation trigger
EventId + EventRevision              lineage and audit
```

Both belong in the manifest, but only the first drives rebuilds. The event-level list answers "what produced revision 12", not "is revision 12 stale".

The one case that does not reduce to a committed Item-Day component is the current UTC day, which is intentionally never materialized. Those rows are invalidated by `CurrentEventRevision` on `EventMaterializationState` instead.

### The union spans membership as well as dates

A date-only union is insufficient. An adjustment's item membership can change with its interval untouched:

```text
ADJ100 revision 7   items = T001, T002, T005
ADJ100 revision 8   items = T001, T002
```

`union(old interval, new interval)` is empty here, yet T005 has stale segments on every day the adjustment covered. The correct rule is two-dimensional:

```text
affected =
    (old items × old materialized dates)
  ∪ (new items × new authoritative dates)
```

Because `EventMaterializationState` is keyed by `(EventId, ItemId)`, T005 already carries its own materialized range and is cleaned up as an ordinary `Deleted` row.

Publication is fenced by the Windfarm-Day serving revision so an older summary build cannot overwrite a newer requested state.

This does not change the Item-Day consistency boundary: calculation components and event references may advance day by day during recovery, but no published Windfarm-Day revision may contain an ambiguous mixture inside one day.

### 6.10.4 Adjustment is one logical event applied to many items

Outage, alarm and curtailment events are naturally single-item facts. An adjustment is not: the user ticks several turbines in one dialog and expects one adjustment, not several. The event model must reflect that, so an adjustment is stored as **one parent with N item children**.

```text
Adjustment
    1
    ↓
AdjustmentItem
    N
```

Example:

```text
ADJ100
Start  = 2026-09-01 10:00
End    = 2026-09-03 18:00
Reason = Manual correction
Items  = T001, T002, T005, T010
```

**Each `AdjustmentItem` is not its own event.** All rows retain:

```text
EventType = Adjustment
EventId   = ADJ100
```

Duplicating the full adjustment once per item would fabricate four adjustments where the user created one, and would make "show me ADJ100" ambiguous.

The canonical monthly event partition therefore holds two files rather than one:

```text
/harmonized/events/adjustment/windfarm=WF01/startyear=2026/startmonth=09/current/
  adjustments.parquet         AdjustmentId, WindfarmId, OriginalStartUtc,
                              OriginalEndUtc, AdjustmentType, Value, Reason,
                              Status, AdjustmentRevision, …
  adjustment-items.parquet    AdjustmentId, ItemId
```

```text
adjustments.parquet
ADJ100 | WF01 | Sep01 10:00 | Sep03 18:00 | Manual | Active | rev 7

adjustment-items.parquet
ADJ100 | T001
ADJ100 | T002
ADJ100 | T005
ADJ100 | T010
```

Both files are published together under the same `EventMonthState` lease and the same revision, so membership can never be committed without its parent.

A turbine-scoped UI query resolves through the child table:

```sql
SELECT a.*
FROM adjustments a
JOIN adjustment_items ai ON ai.AdjustmentId = a.AdjustmentId
WHERE ai.ItemId = @itemId
  AND a.OriginalStartUtc < @rangeEnd
  AND (a.OriginalEndUtc IS NULL OR a.OriginalEndUtc > @rangeStart);
```

### The daily reference stays flat

The daily event-reference projection exists specifically for cheap Item-Day lookup, so it **is** flattened — one row per affected item per day, all carrying the same `EventId`:

```text
WF01 T001 Sep01 Adjustment ADJ100
WF01 T002 Sep01 Adjustment ADJ100
WF01 T005 Sep01 Adjustment ADJ100
WF01 T010 Sep01 Adjustment ADJ100
WF01 T001 Sep02 Adjustment ADJ100
…
```

This is not harmful duplication: the daily file is a lightweight derived index carrying only identity and bounded overlap, while the attributes remain stored once in the parent.

```text
Canonical event store      parent + item children, normalized
Daily serving index        flattened by ItemId + Date + EventId
```

### Calculation expands the parent

The Harmonizer expands the parent across its item membership and day span into ordinary Item-Day components:

```text
ADJ100 × {T001, T002, T005, T010} × {Sep01, Sep02, Sep03}
        ↓
/harmonized/item/windfarm=WF01/item=T005/date=2026-09-02/
  adjustment/revision=<n>/adjustment-segments.parquet
```

Every segment row retains `AdjustmentId = ADJ100` so lineage from a calculated loss row back to the single authoring action stays intact.

The resulting semantics:

```text
AdjustmentId                 logical event identity
AdjustmentRevision           complete parent + complete item membership
AdjustmentItem               child applicability
Item-Day adjustment segment  derived calculation representation
```

which answers both questions the UI asks:

```text
"show me ADJ100"                 → one logical event
"show adjustments affecting T005" → resolve through AdjustmentItem / daily reference
```

---

## 6.11 Event materialization state and the Event Lifecycle Coordinator

Sections 6.5 and 6.10.2 both depend on a fact the rest of the design does not record anywhere: **how far each event has actually been projected into Item-Day components**. Without it, an open event stops being materialized the moment its source stops changing, and a shrunk or deleted event cannot know which days it must clean up.

`EventMaterializationState` is that record. Open, closed and deleted are three states of one lifecycle rather than three separate mechanisms, so they share one table.

### Why this is SQL, not Parquet

This is mutable, small, frequently updated coordination state, which Section 2.8 places in Azure SQL. Three properties make Parquet the wrong choice:

```text
write pattern    per-event point updates; the watermark moves daily per open
                 event. Parquet is whole-file rewrite, so every update would
                 rewrite the windfarm's entire event-state file.

concurrency      a lake file needs a single-writer owner per windfarm to stay
                 safe, serializing all event updates for that windfarm and
                 discarding the per-Item-Day sharding the design relies on.

durability       the state is derivable by scanning the canonical monthly event
                 store, so it does not need lake immutability to be replayable.
```

Section 24.4 states the governing principle directly: Azure SQL is the single authority, and Blob layouts are derived and rebuildable. Putting authoritative lifecycle state in Parquet inverts that.

```text
EventMaterializationState
-------------------------
WindfarmId
EventType
EventId
ItemId

CurrentEventRevision            latest canonical event revision known
MaterializedEventRevision       latest revision fully propagated to Item-Day state

LifecycleStatus                 Open | Closed | Deleted

EffectiveStartUtc
EffectiveEndUtc                 NULL while open

MaterializedFromDate            earliest Item-Day currently materialized
MaterializedThroughDate         contiguous committed watermark (Section 6.5)

AttemptCount
LastError
UpdatedAtUtc

PRIMARY KEY (EventId, ItemId)
INDEX (WindfarmId, EventType, LifecycleStatus, MaterializedThroughDate)
```

The grain is `(EventId, ItemId)`, not `EventId`. A single-item outage produces one row; an adjustment parent produces one row per selected turbine. That is what allows a removed adjustment item to be cleaned up independently (Section 6.10.3) and lets each item's Item-Day commits advance their own watermark.

### Two revisions, because they answer different questions

```text
CurrentEventRevision      = 9
MaterializedEventRevision = 8
        ↓
the canonical event is newer than the Item-Day projections
```

That comparison is the reconciliation signal. A single `LastProcessedRevision` cannot express it, because it conflates "what we know" with "what we have applied". The pair follows the same desired-versus-achieved idiom as `RequestedPackageRevision`/`CommittedPackageRevision` and `RequestedSummaryRevision`/`PublishedSummaryRevision`.

`MaterializedThroughDate` is a **contiguous** committed watermark, not the highest date attempted:

```text
Sep01 ✅   Sep02 ✅   Sep03 ❌   Sep04 ✅   Sep05 ✅

MaterializedThroughDate = Sep02        not Sep05
```

`ItemDayInputState` remains the per-day authority; the watermark is only a cheap scheduling hint, and a hole is refilled by the reconciliation sweep. A range pair cannot represent a gap, so it must never be advanced past one.

### Driver 1 — clock-driven continuation

```text
WHERE LifecycleStatus = Open
  AND MaterializedThroughDate < CalculationProcessableDate
        ↓
emit the missing UTC Item-Days for (EventId, ItemId)
        ↓
harmonization-processing SQS
        ↓
Harmonizer writes the component revision
        ↓
fenced ItemDayInputState commit
        ↓
advance MaterializedThroughDate
```

No new event revision is created: nothing about the event changed, only the calendar. `CurrentEventRevision` and `MaterializedEventRevision` stay equal throughout.

This is the only trigger in the platform not caused by an arriving source row, which is precisely why the gap in Section 6.5 existed.

### Driver 2 — revision-driven correction

A close, shrink, extension, deletion or membership change dispatches the union set from Section 6.10.3 and rebuilds each affected Item-Day, publishing explicitly empty components where the event no longer applies.

A close is the instructive case, because the days do not all get the same treatment:

```text
old   Sep01 10:00 → NULL,        MaterializedThroughDate = Sep05
new   Sep01 10:00 → Sep03 14:20

affected = Sep01 … Sep05

Sep01   keep
Sep02   keep
Sep03   rebuild as a partial day
Sep04   rebuild as explicitly empty
Sep05   rebuild as explicitly empty
```

Once every affected day has committed, `MaterializedEventRevision` catches up to `CurrentEventRevision`, `MaterializedThroughDate` becomes Sep03 and `LifecycleStatus` becomes `Closed`, which withdraws the row from the continuation sweep.

A deletion is the same flow with every day in `MaterializedFromDate … MaterializedThroughDate` rebuilt as empty.

### Retention

A terminal row may be pruned to a minimal tombstone only after its cleanup has committed **and** the affected event-reference revisions have been republished. Pruning earlier discards the only record of what still needs cleaning. The continuation sweep's working set is just `LifecycleStatus = Open`, which stays small regardless of history.

### Resulting flow

```text
Raw event change
        ↓
Harmonizer ──→ canonical monthly event (EventMonthState fenced)
        ↓
EventMaterializationState
        │
        ├──────────────────────────┬───────────────────────────┐
        ↓                          ↓                           ↓
clock-driven continuation   revision-driven correction   current-day open events
Open AND watermark <        close / shrink / delete /    reference-only, no
CalculationProcessableDate  membership change            calculation
        │                          │                           │
        └────────────┬─────────────┘                           │
                     ↓                                         │
       harmonization-processing SQS                            │
                     ↓                                         │
                 Harmonizer                                    │
                     ↓                                         │
     Item-Day components → ItemDayInputState                   │
                     ↓                                         │
        ┌────────────┴────────────┐                            │
        ↓                         ↓                            │
Package Coordinator      SummaryWorkItem ←──────────────────────┘
        ↓                HarmonizedEventReference
   Calculator                     ↓
                            Summary Worker
```

The Event Lifecycle Coordinator owns both drivers. It does not write Item-Day components itself; it derives affected Item-Days and dispatches them onto the existing `harmonization-processing` queue, so the Harmonizer, its per-item isolation, its idempotent keyed merge and the reconciliation sweep all apply unchanged.

---

## 7.1 Responsibility

Treat user adjustment as a first-class fifth input source and ensure every change reliably enters the calculation flow.

---

## 7.2 Adjustment storage

Azure SQL stores:

```text
Adjustment
AdjustmentItem
AdjustmentVersion
AdjustmentAudit
```

The authoritative authoring model is **one parent adjustment with N selected items** (Section 6.10.4), never one adjustment row per turbine:

```text
Adjustment
----------
AdjustmentId
WindfarmId
AdjustmentType
StartTime
EndTime
Value
LostProductionTypeId
OtherLossTypeId
Reason
Status            (Active | Cancelled)
Version
CreatedBy / CreatedAt / UpdatedBy / UpdatedAt

AdjustmentItem
--------------
AdjustmentId
ItemId

UNIQUE (AdjustmentId, ItemId)
```

A windfarm-wide adjustment is expressed as membership covering every item active for that windfarm over the adjustment's range (Section 8.6 effective-dated membership), not as a magic `NULL` item.

Item membership is part of the parent's `Version`: adding or removing a turbine is an edit of the adjustment, not an independent child mutation.

---

## 7.3 Adjustment flow into the lake

```text
UI
 ↓
Adjustment API
 ↓
Adjustment row + AdjustmentItem + Audit + Outbox committed in ONE transaction
 ↓
Outbox processor (drains at-least-once)
 ↓
Write /raw/adjustment/windfarm=<w>/date=<d>/extraction=<outboxToken>/adjustment.parquet
 ↓
raw-fanout SQS
 ↓
Raw Fan-out Coordinator
 ↓
harmonization-processing SQS
 ↓
Harmonizer
 ↓
/harmonized/item/windfarm=<w>/item=<i>/date=<d>/adjustment/revision=<n>/adjustment-segments.parquet
 ↓
For a turbine-specific adjustment: new package requested for every affected Item-Date
For a windfarm-wide adjustment: resolve affected items, then new package requested for every affected Item-Date
 ↓
Calculation SQS
```

The outbox is what makes this reliable, and it is **retained unchanged** by the ingestion simplification. The adjustment and the intent to publish it are committed atomically, so an API crash after `COMMIT` cannot lose the lake push.

The extraction cursor cannot substitute for it. For source extraction the upstream system is the durable record and the cursor re-derives lost work; for adjustments Azure SQL *is* the source, so a lost push is unrecoverable and silent.

The outbox row supplies the monotonic token used as the Raw path discriminator, playing the same role the extraction lease token plays for source extraction (Section 4.9).

### Adjustment raw payload

The raw adjustment file carries the **complete parent plus the complete current item membership** for every adjustment affecting that windfarm-date — never a delta, and never "T005 added" / "T002 removed" as a standalone child change. This keeps the raw partition self-describing and makes reprocessing idempotent:

```json
{
  "adjustmentId": "ADJ100",
  "adjustmentRevision": 7,
  "windfarmId": "WF01",
  "adjustmentType": "Manual",
  "startTimeUtc": "2026-09-01T10:00:00Z",
  "endTimeUtc": "2026-09-03T18:00:00Z",
  "value": 1250.0,
  "lostProductionTypeId": 4,
  "otherLossTypeId": null,
  "reason": "Manual correction",
  "status": "Active",
  "itemIds": ["T001", "T002", "T005", "T010"]
}
```

Because the payload is a complete membership snapshot, Harmonization can diff deterministically:

```text
revision 7 membership   T001 T002 T005 T010
revision 8 membership   T001      T005 T010
        ↓
T002 removed
        ↓
clear/regenerate T002's Item-Day adjustment components for the affected dates
```

Processing child changes independently would leave a removed turbine silently carrying a stale adjustment, because nothing would ever instruct the pipeline to remove it.

Each row/record also retains:

```text
AdjustmentId
AdjustmentRevision
WindfarmId
AdjustmentType
StartTimeUtc
EndTimeUtc
Value
LostProductionTypeId
OtherLossTypeId
Reason
Status           (Active | Cancelled)
CreatedBy / CreatedAt / UpdatedBy / UpdatedAt
```

Because `Status = Cancelled` rows are still written, a cancellation is an ordinary new revision rather than a deletion, and the calculation simply ignores cancelled rows.

### Why adjustments must reach the lake

- The calculation worker reads **only** the Harmonized Zone. An adjustment that stayed in Azure SQL would be invisible to it, forcing a cross-cloud SQL read on every work item.
- Reproducibility: replaying a historical revision must reproduce the adjustment state *as it was*, which requires an immutable versioned copy.
- The Calculated summaries must attribute adjustment-driven loss to a `LostProductionTypeId` / `OtherLossTypeId` like any other cause.

---

## 7.4 Adjustment update and cancellation

Editing an adjustment creates a new version.

Cancellation should normally create a newer version with:

```text
Status = Cancelled
```

instead of physically deleting history.

For an adjustment change, the affected range is:

```text
union(old range, new range)
```

and the affected item set is:

```text
union(old membership, new membership)
```

Both unions are required. Using the new values alone would leave a de-selected turbine, or a day the adjustment no longer covers, holding a stale adjustment component that nothing would ever clear.

All affected Item-Date partitions are harmonized/recalculated.

---

# 8. Feature / Zone 4 — Calculation Orchestration Zone

## 8.1 Responsibility

Track what calculation work is required, dispatch it, and know when it has completed.

---

## 8.2 Work grain

Normal processing:

```text
ItemId + CalculationDate
```

The calculation worker grain is **always Item-Day**. Large historical requests may be grouped by `ItemId + Month` for request tracking and dispatch batching, but are expanded into Item-Day calculation work items before execution.

Windfarm-wide triggers do not change the work grain. They are expanded into the affected set of `ItemId + CalculationDate` work items before calculation is dispatched.

---

## 8.3 Active Calculation Work Ledger

`CalculationWorkItem` is an **active-work table**, not a permanent history of every Item-Day ever calculated.

Recommended model:

```text
CalculationWorkItem
-------------------
WindfarmId
ItemId
CalculationDate

RequestedPackageRevision
RunningPackageRevision

Status
AttemptCount

ManifestPath
LeaseOwner
LeaseExpiresAt

CreatedAtUtc
StartedAtUtc
LastError
```

Typical statuses:

```text
Pending
Running
Retrying
Failed
```

Completed work should be removed after a short retention period or moved to an optional history/audit table.

A separate narrow table may store the current published result pointer:

```text
CalculatedPartition
-------------------
WindfarmId
ItemId
CalculationDate
CommittedPackageRevision
CalculatedPath
CalculationVersion
PublishedAtUtc
```

This separation keeps orchestration state small while still allowing millions of historical Item-Day partitions to be indexed efficiently if required.

## 8.4 Package revision semantics and late-arriving data

The package revision represents the exact combination of component revisions used for a calculation.

Example:

```text
Package 18
Production   2
Outage       1
Curtailment  3
Alarm        2
Adjustment   4
```

If a delayed outage arrives several days later, only the outage component changes.

The Harmonizer reads the current complete outage snapshot when the new source input is incremental, merges/deduplicates the delayed event, and writes a new immutable outage snapshot:

```text
outage revision 1:
O001
O002
O003

+ delayed O004

→ outage revision 2:
O001
O002
O003
O004
```

The other component files are not copied or rewritten.

### Concurrency control on component revisions

The merge above is a read-modify-write of shared state. With at-least-once SQS delivery (Section 2.4) and multiple Harmonizer pods, two late events for the same Item-Day and input type can both read revision 1 and both write revision 2, losing one of them.

A component revision may therefore only be produced under an exclusive lease, and the lease must be **fenced**.

A lease can expire while its holder is still running — a paused pod, a long GC, a network partition. The zombie writer does not know it lost the lease. Without a fencing token it can still write files and still commit to SQL. Every lease acquisition therefore issues a monotonically increasing `LeaseToken`, and the candidate output is written to a token-scoped attempt directory rather than directly to the revision path:

```text
1. Acquire a lease on (WindfarmId, ItemId, DataDate, InputType)
   in ItemDayInputState, receiving LeaseToken = 584 and an expiry.
2. Read current component revision N from ItemDayInputState.
3. Merge and write the candidate to a token-scoped attempt path:
      outage/revision=<N+1>/attempt=584/outage-segments.parquet
4. Commit in SQL, fenced on the token:
      UPDATE ItemDayInputState
         SET ComponentRevision = N+1,
             CommittedAttempt  = 584
       WHERE ... AND LeaseToken = 584 AND ComponentRevision = N
5. Release the lease.
```

Only the current token can win step 4. A zombie holding token 583 fails the `WHERE` clause, its attempt directory is never referenced by any manifest, and it is garbage-collected.

The attempt directory is what makes retry safe. Writing straight to `revision=N+1/` with a conditional create would leave an orphan from the zombie that the new lease holder can neither overwrite nor advance past — it would fail the conditional create, re-read N from SQL, and livelock. With token-scoped attempts each writer has a private path, so retries always make progress.

```text
Readers resolve:
  ComponentRevision + CommittedAttempt   → the one authoritative file
Everything else under revision=N+1/ is an abandoned attempt.
```

A writer that loses the fenced commit discards its work and retries from step 2.

### Merge is a keyed upsert, not an append

Redelivery and source corrections both require the merge to be idempotent and independent of arrival order:

```text
Merge key      = SourceEventId
Conflict rule  = highest SourceWatermarkUtc wins, tie-broken by SourceRevision
Deletes        = explicit tombstone rows, never silent omission
```

Consequences:

```text
Replaying the same source message produces an identical component revision.
A corrected event replaces the prior version of that EventId rather than duplicating it.
A cancelled event is representable, so it can be removed from the snapshot.
```

Only when the merge produces an unchanged **canonical content hash** may the Harmonizer skip writing a new revision. This keeps redelivery from inflating the revision counter and minting no-op packages.

The comparison must not be Parquet byte equality. Two semantically identical Parquet files can differ byte-for-byte because of writer version, encoding choice, dictionary state, row-group layout, or embedded metadata. Compute the hash over the canonicalized logical rows — the merge key, business columns, and tombstone flags, in a deterministic sort order — and store it on `ItemDayInputState` alongside `ComponentRevision`.

The Package Coordinator then creates:

```text
Package 19
Production   2
Outage       2    ← changed
Curtailment  3
Alarm        2
Adjustment   4
```

If package 18 is already running:

```text
RunningPackageRevision   = 18
RequestedPackageRevision = 19
```

package 18 may finish computation, but it must not become current if a newer requested package exists. Package 19 is then dispatched and becomes current only after successful validation/publication.

If the upstream source provides a complete authoritative snapshot rather than a delta, rebuild the component revision directly from that snapshot instead of reading the previous Harmonized Parquet.

## 8.5 Unified recalculation operation

All recalculation triggers perform the same logical operation:

```text
Identify affected Item-Date
        ↓
create/update changed Harmonized component revision
        ↓
Package Coordinator evaluates readiness
        ↓
create new package manifest if component combination changed
        ↓
RequestedPackageRevision moves forward
```

Triggers include:

- New source data
- Production backfill
- Outage correction
- Curtailment correction
- Alarm change
- Adjustment create/change/cancel
- Open event closure
- Configuration change
- Calculation code change
- User-requested recalculation
- Historical backfill

---

## 8.6 Item-Day readiness and Calculation Package Coordinator

A dedicated logical responsibility, **Calculation Package Coordinator**, determines whether an Item-Day has a calculation-ready combination of inputs.

The coordinator does **not** infer readiness from Parquet file existence.

A small SQL state table records source-pipeline completion explicitly. Since the removal of `RawFanOutWorkItem`, this table is **both** the component revision authority **and** the Item-Day processing/readiness state:

```text
ItemDayInputState
-----------------
WindfarmId
ItemId
DataDate
InputType

ComponentRevision
CommittedAttempt
CanonicalContentHash

Status
RecordCount
SourceWatermarkUtc

AttemptCount
LastAttemptAtUtc
LastError

LastRawPath                last Raw artifact that touched this Item-Day
LastExtractionToken

LeaseOwner
LeaseToken
LeaseExpiresAt

CompletedAtUtc

UNIQUE (WindfarmId, ItemId, DataDate, InputType)
```

`ComponentRevision` + `CommittedAttempt` together identify the one authoritative component file; `LeaseToken` fences the commit (Section 8.4).

`CanonicalContentHash` is the hash of the canonicalized logical rows of that component revision (Section 8.4). A merge whose result hashes to the stored value is a no-op: the Harmonizer skips writing a new revision, so redelivery and safety-lag re-reads cannot inflate the revision counter or mint no-op packages.

`LastRawPath` / `LastExtractionToken` let the reconciliation sweep (Section 5.8) re-dispatch a failed Item-Day without re-enumerating its parent artifact.

Typical statuses:

```text
Waiting
Processing
Complete
Failed
Unavailable
```

Example:

```text
WF01 T001 2026-08-30 Production    rev5 Complete 144
WF01 T001 2026-08-30 Outage       rev2 Complete   0
WF01 T001 2026-08-30 Curtailment  rev3 Complete 144
WF01 T001 2026-08-30 Alarm        rev4 Complete   3
WF01 T001 2026-08-30 Adjustment   rev1 Complete   0
```

`Complete + RecordCount = 0` explicitly means the source pipeline completed successfully and there are no records for that Item-Day.

File absence must never be interpreted as "zero events".

### `Failed` does not erase the last good revision

When new source input for an Item-Day is known to exist but fails to harmonize, the previously committed revision is **retained** and the row is marked failed:

```text
ComponentRevision    = 2          ← unchanged, still the last good snapshot
CanonicalContentHash = <previous> ← unchanged
Status               = Failed
AttemptCount         = 4
LastError            = ...
```

The semantics are precise:

> Revision 2 is the last successfully committed snapshot, but it is **no longer safe to use as calculation input**, because newer source information is known to exist and has not been incorporated.

After a successful retry, either outcome returns the row to readiness without inventing history:

```text
canonical content changed    → ComponentRevision = 3, Status = Complete
canonical content unchanged  → ComponentRevision stays 2, Status = Complete
   (a safety-lag replay or redelivery; no revision inflation)
```

### Readiness requires `Complete`, and absence is not readiness

```text
Readiness requires, for EVERY required InputType:

    a row EXISTS for (WindfarmId, ItemId, DataDate, InputType)
    AND Status = 'Complete'

NOT merely:  ComponentRevision IS NOT NULL
NOT merely:  no row saying otherwise
```

Both halves are load-bearing.

Requiring `Status = 'Complete'` is what closes the gap left by removing the fan-out ledger. Without it:

```text
new outage source data exists
        ↓
Harmonizer fails for T0345 / Sep01 / Outage
        ↓
ComponentRevision = 2 still looks usable
        ↓
Package Coordinator sees a ready input
        ↓
calculation runs on STALE outage data
        ↓
publishes an apparently valid, silently wrong result
```

That is strictly worse than failing visibly, and no Windfarm-Day barrier catches it: the barriers cover Production and published calculations, never event inputs.

Requiring the row to **exist** matters because a newly affected Item-Day may have no row at all \u2014 for example an outage that newly extends into `T0345 / Sep02`. The Harmonizer therefore **upsert-creates** the row on failure with `ComponentRevision = NULL, Status = Failed`, and a missing required-input row is treated as not-ready. This is the same rule already stated for data (`File absence must never be interpreted as "zero events"`), applied to state: absence is not readiness.

Readiness rule:

```text
all required inputs exist AND Status = Complete
        ↓
Package Coordinator creates immutable package manifest
        ↓
create/update CalculationWorkItem
        ↓
Calculation Dispatcher sends package reference to SQS
```

Source-specific pipelines own completeness:

- Production may use expected 10-minute interval counts.
- Curtailment may use expected 10-minute interval counts or source extraction completion.
- Outage and alarm use extraction completion/watermarks because event counts are not known in advance.
- Adjustment uses the resolved current adjustment snapshot/version.

A late event does not invalidate the earlier completion signal. It creates a new component revision and therefore a new package revision for recalculation.

### Windfarm-Day readiness

`ItemDayInputState` is per item and cannot express readiness of the Windfarm-Day aggregates in Section 6.9.

"Windfarm-Day completion" is **not one concept**. There are two barriers, and they advance independently:

```text
Harmonized production summary barrier
= all expected Production Item-Days complete

Calculated summary barrier
= all expected Item-Day calculations published
```

Production for all 1,500 turbines can be complete hours before the last calculation publishes, and a configuration-driven recalculation advances the second barrier without touching the first. Collapsing them into one counter would make either the production denominator or the loss numerator wait on the other. They therefore get separate state:

```text
WindfarmDayProductionState
--------------------------
WindfarmId
DataDate
ExpectedItemCount
CompletedItemCount
MissingItemCount
IsPartial
CompletenessDeadlineUtc
MembershipVersion
WindfarmDayRevision
RequestedSummaryRevision
SummaryWorkStatus
Status
LastUpdatedUtc
LastError
```

```text
WindfarmDayCalculationState
---------------------------
WindfarmId
CalculationDate
ExpectedItemCount
CompletedItemCount
MissingItemCount
IsPartial
CompletenessDeadlineUtc
MembershipVersion
CalculatedSummaryRevision
RequestedSummaryRevision
SummaryWorkStatus
Status
LastUpdatedUtc
LastError
```

What counts toward each barrier is defined narrowly:

```text
WindfarmDayProductionState.CompletedItemCount
  counts an Item-Day only where
      ItemDayInputState.InputType = Production
  AND ItemDayInputState.Status    = Complete

WindfarmDayCalculationState.CompletedItemCount
  counts an Item-Day only where its latest REQUESTED package revision has
  actually been published — CalculatedPartition.CommittedPackageRevision
  = CalculationWorkItem.RequestedPackageRevision
```

The second rule is deliberately stricter than "the worker finished". A worker that computed package 18 while 19 was already requested is `Superseded` (Section 14.5) and must not satisfy the barrier.

`WindfarmDayRevision` and `CalculatedSummaryRevision` are each the **authoritative** current revision for their summary; the Blob `current/` folders are derived projections of them (Section 6.4.1).

### Effective-dated membership determines the expected count

`ExpectedItemCount` must not be resolved from today's turbine list, or rebuilding 2019 would expect turbines commissioned in 2024. Membership is effective-dated configuration:

```text
WindfarmItemMembership
----------------------
WindfarmId
ItemId
EffectiveFrom
EffectiveTo
ConfigVersion
```

When a state row is created:

```text
ExpectedItemCount
= COUNT(items active for that WindfarmId on that date)

MembershipVersion
= the ConfigVersion used to resolve that count      ← stored on the row
```

Storing `MembershipVersion` is what makes historical rebuilding deterministic: a replay resolves the same denominator the original run did, and a membership correction is visible as a version change rather than as an unexplained count drift.

### The worker that trips the barrier does not generate the summary

The worker that happens to push the counter from `1,499 → 1,500` must **not** generate the summary inline. If it crashes mid-generation, the counter is already at 1,500 and no other worker will ever push it again — the summary is silently lost forever.

In the same transaction that detects the threshold, the worker only records the intent:

```text
Item-Day worker
    ↓
commit Item-Day state
    ↓
increment the correct Windfarm-Day barrier   (guarded, Section 5.10)
    ↓
if CompletedItemCount reached ExpectedItemCount:
        insert/upsert SummaryWorkItem = Pending
    ↓
commit
```

Generation is then an ordinary durable pipeline with its own retry semantics:

```text
Summary Dispatcher
    ↓
summary SQS
    ↓
Summary Worker
    ↓
read the relevant Item-Day outputs
    ↓
generate the Windfarm-Day summary
    ↓
publish revision
    ↓
mark SummaryWorkItem Completed
```

A crashed Summary Worker is recovered by visibility timeout and the ledger row, exactly like every other stage.

### One SummaryWorkItem table, discriminated by type

The barrier state is deliberately separate, but the execution pattern is identical, so a single queue-ledger table serves both rather than two near-duplicate tables:

```text
SummaryWorkItem
---------------
SummaryWorkItemId
WindfarmId
DataDate
SummaryType
RequestedRevision
Status
AttemptCount
LeaseOwner
LeaseExpiresAt
LastError

SummaryType:
HarmonizedProduction
HarmonizedEventReference
CalculatedLoss

UNIQUE (WindfarmId, DataDate, SummaryType)
```

The unique key makes the upsert idempotent under SQS redelivery: a repeated threshold detection refreshes `RequestedRevision` on the existing row instead of queuing a duplicate summary build.

### The Summary Worker branches on SummaryType

All three projections share one dispatcher, one queue, one lease/retry/DLQ pattern and one pod type. Only the read set and the output file differ:

```text
HarmonizedProduction
    → current Production Item-Day components
    → daily-windfarm-wtgs-production.parquet

HarmonizedEventReference
    → current Outage / Alarm / Curtailment / Adjustment Item-Day components
    → plus current-day open events from EventMaterializationState (Section 6.10.2)
    → daily-item-event-references.parquet

CalculatedLoss
    → current published Item-Day calculations
    → calculated Windfarm-Day summaries
```

A new revision is always a **complete snapshot** rebuilt from current authoritative inputs. It is never a delta against the previous revision, and the previous serving file is never copied forward — the same rule already applied to package revisions in Section 6.4.

The event-reference build must not resolve its rows by scanning `/harmonized/events/**` monthly partitions. Doing so would force the worker to scan unbounded prior months to find open events overlapping the day, which is the exact lookup cost the projection exists to eliminate.

### Event references are invalidation-driven, not barrier-driven

The `CompletedItemCount = ExpectedItemCount` barrier is wrong for this projection. Most items have no event on most days, and absence of an event is a legitimate, permanent state — an event barrier would never trip.

Generation is therefore requested by invalidation (Section 6.10.3), against its own state row rather than sharing `WindfarmDayProductionState`, because the inputs and readiness semantics differ:

```text
WindfarmDayEventReferenceState
------------------------------
WindfarmId
DataDate

RequestedSummaryRevision
PublishedSummaryRevision

LeaseOwner
LeaseToken
LeaseExpiresAt

Status
IsPartial
MissingItemCount
LastError

UNIQUE (WindfarmId, DataDate)
```

Dropping the barrier does **not** mean dropping completeness reporting. In this dataset an absent row is read as "no event", so a day published mid-harmonization renders an unprocessed turbine as a clean one. The build therefore still records `ItemDayInputState` coverage using the same `IsPartial` / `MissingItemCount` labelling as the production aggregate:

> A missing reference row means "no event" **only** when `IsPartial = false`.

Publication is fenced exactly as elsewhere: the commit succeeds only if `RequestedSummaryRevision` is unchanged and the lease token still matches, so a build superseded mid-flight can never overwrite a newer requested state.

### The counter is a first-readiness barrier, not a liveness invariant

If a late correction changes one Item-Day **after** the Windfarm-Day summary has already been published, do **not** reset the counter to 1,499. The barrier is already satisfied, and "uncompleting" the day would remove a published figure from the UI while the correction is reprocessed.

Instead, invalidate and re-request:

```text
T500 Production revision changes
        ↓
WindfarmDayProductionState already Complete
        ↓
increment RequestedSummaryRevision
        ↓
create/refresh SummaryWorkItem
        ↓
rebuild the summary from the LATEST Item-Day revisions
```

The same rule applies to `WindfarmDayCalculationState` when a recalculated Item-Day publishes a newer `CommittedPackageRevision`.

The resulting lifecycle:

```text
First build:
0/1500 → … → 1500/1500
                 ↓
            summary publish

Later correction:
one Item-Day revision changes
                 ↓
         summary becomes stale
                 ↓
        request new summary revision
                 ↓
              republish
```

A summary is stale, never absent, once the first barrier has been crossed. This is also what closes the Section 6.9 gap where a late turbine day would otherwise leave a stale windfarm denominator against an already-recalculated numerator.

### Partial publication

Waiting unconditionally for every item would let one permanently failing turbine blackhole a whole windfarm-day. Both barriers therefore carry a completeness deadline:

```text
CompletedItemCount = ExpectedItemCount
        ↓
SummaryWorkItem = Pending, Status = Complete, IsPartial = false

CompletedItemCount < ExpectedItemCount at CompletenessDeadlineUtc
        ↓
SummaryWorkItem = Pending built from the items that ARE complete
        ↓
Status = Partial, IsPartial = true, MissingItemCount recorded
        ↓
raise a data-availability alert
```

A partial summary is superseded by a `Complete` one as soon as the missing items arrive, through the same re-request path above (Section 6.9). Publishing partial rather than nothing keeps the windfarm-day visible; the `IsPartial` label keeps it honest.

### Non-data triggers

A configuration change or calculation code change alters no component revision. Because the manifest carries `configVersion` and `calculationVersion` (Section 6.4), the coordinator mints a new package revision with identical component references and advances `RequestedPackageRevision`. Readiness is unaffected: all inputs are already `Complete`.

The coordinator may be implemented as a small worker/service or initially as a distinct responsibility hosted in an existing orchestration process, but its architectural responsibility should remain separate from Harmonization.

---

### Calculation Dispatcher and SQL control-plane state

The Calculation Dispatcher uses Azure SQL as the durable control-plane source for work that still needs to be sent or retried.

```text
Package Coordinator
        ↓
upsert CalculationWorkItem = Pending
        ↓
Calculation Dispatcher
        ↓
claim Pending work in SQL
        ↓
send package reference to SQS
        ↓
update dispatch/work state
```

SQL is used only for small orchestration state. It does **not** contain the calculation facts or Harmonized time-series data.

This separation is intentional:

```text
Azure SQL
→ current input readiness
→ requested/running package revision
→ active calculation work
→ current published-result pointer

Azure Blob
→ Harmonized component Parquet
→ immutable package manifests
→ Calculated Parquet

AWS SQS
→ execution transport between dispatcher and stateless Calculation Workers
```

The active `CalculationWorkItem` table should retain only outstanding/recent work and may purge or archive completed rows after a short retention period.

## 8.7 Queue design

Recommended:

```text
calculation-main
calculation-backfill
summary-generation
calculation-dead-letter
```

Use Standard SQS unless strict ordering becomes a demonstrated requirement.

Revision checking protects against older messages publishing stale results.

Each SQS message should reference one immutable calculation package:

```json
{
  "windfarmId": "WF01",
  "itemId": "T001",
  "date": "2026-08-20",
  "packageRevision": 19,
  "manifestPath": "/harmonized/item/windfarm=WF01/item=T001/date=2026-08-20/packages/revision=19/manifest.json"
}
```

The Calculation Worker reads the manifest and then the exact component revisions listed in it.


---

## 8.8 Backfill capacity isolation

Normal and historical work should have independent KEDA scaling policies.

Example:

```text
Normal calculation workers:
min 10
max 100

Backfill workers:
min 0
max 500
```

Exact limits are determined through performance testing.

---

# 9. Feature / Zone 5 — Distributed Calculation Zone

## 9.1 Responsibility

Produce validated 10-minute loss results for each calculation partition.

---

## 9.2 Infrastructure

- AWS SQS Calculation Queue
- AWS SQS Backfill Queue
- KEDA
- EKS Calculation Pods
- Versioned C# Calculation Library
- Azure Blob Storage Calculated Zone

---

## 9.3 Calculation worker flow

```text
1. Receive package-reference queue message
2. Validate requested package revision
3. Acquire/renew work lease
4. Read immutable package manifest
5. Read the exact Harmonized component revisions referenced by the manifest
6. Resolve configuration version
7. Apply user adjustments
8. Join 10-minute production with event intervals
9. Resolve outage/curtailment classification
10. Calculate losses
11. Attach event/alarm attribution
12. Validate complete output partition
13. Write immutable calculated output revision
14. Re-check RequestedPackageRevision before commit
15. Publish only if this package is still the latest requested revision
16. Update current calculated-partition pointer
17. Delete SQS message
```

---

## 9.4 Base loss

```text
Loss = Possible Production - Actual Production
```

Negative published loss should not be allowed unless explicitly required by the business rule.

---

## 9.5 Partial overlap

For events that cover only part of a 10-minute interval:

```text
AttributedLoss =
BaseLoss × OverlapSeconds / 600
```

if proportional allocation is the approved business rule.

---

## 9.6 Calculation library design

The calculation engine should:

- Be deterministic
- Avoid database mutation during calculation
- Avoid shared global/process-wide locks
- Use interval segmentation rather than cursor-based state
- Return collections of result allocations
- Be versioned as a reusable C# package

Conceptual contract:

```csharp
CalculationResult Calculate(
    HarmonizedInput input,
    CalculationConfiguration configuration);
```

---

## 9.7 Result grain

At minimum, retain 10-minute output.

A flexible allocation model is recommended:

```text
(item, bucketStart, lossType, eventId)
```

This allows multiple contributing source events to be represented if the business rules require it. `EventId` is also the join key back to the Harmonized canonical event/event-reference model.

The daily WTG calculated summaries remain additive Item-Day results and do **not** need to copy event attributes. Calculated loss must not be copied into Harmonized merely to support event display.

---

# 10. Calculation Validation and Invariants

A complete partition must pass validation before publication.

Required invariants include:

- No invalid negative published loss
- Total allocations equal the expected calculated lost-production amount
- Sub-allocations do not exceed parent allocations
- Overlap values stay within the 10-minute bucket
- Allocation weights follow the selected business policy
- Repeating the same calculation returns the same result
- Input ordering does not affect output
- A complete partition can be recalculated without duplication

Rollup invariants, checked by the Publisher before the manifest commit:

- `DailyWindfarm*Summary` totals equal `SUM` of the corresponding `DailyWtg*Summary` rows
- Daily summary totals equal `SUM` of the underlying 10-minute allocations for that day
- `SUM(TotalOtherLoss)` per `LostProductionTypeId` does not exceed the matching `TotalLostProduction`
- UTC and Local groupings of the same row set produce identical grand totals exactly, not merely within tolerance (Section 6.4.3); genuine divergence occurs only at a range boundary, where the local year is offset from the UTC year
- `ValidIntervalCount + MissingIntervalCount = ExpectedIntervalCount`

These checks should exist in:

- Unit tests
- Property-based tests
- Runtime validation

---

# 11. Feature / Zone 6 — Calculation Completion and Calculated Summary Publication

## 11.1 Responsibility

Determine when calculation work is complete and publish calculation-derived detail and summaries into the Calculated Zone.

There is no mandatory Gold-publication step in the initial architecture.

## 11.2 Run completion

An empty SQS queue is not proof of business completion.

Track every expected work item.

```text
Preparing
Queued
Processing
Completed
Failed
Superseded
```

A run is complete when:

```text
CompletedWorkItems = ExpectedWorkItems
FailedWorkItems = 0
PoisonWorkItems = 0
ActiveWorkItems = 0
```

### Calculated Windfarm-Day summary barrier

Run completion above is scoped to a *request*. The Windfarm-Day calculated summaries have a separate, standing barrier that is not tied to any one run, because a day can be advanced by daily processing, by an adjustment, and by a backfill run at different times.

That barrier is `WindfarmDayCalculationState` (Section 8.6), and it is independent of the harmonized production barrier:

```text
Item-Day calculation publishes
        ↓
CommittedPackageRevision = RequestedPackageRevision
        ↓
increment WindfarmDayCalculationState.CompletedItemCount   (guarded)
        ↓
threshold reached → SummaryWorkItem (SummaryType = CalculatedLoss) = Pending
        ↓
Summary Dispatcher → summary SQS → Summary Worker
        ↓
build daily-windfarm-calculated-loss / other-loss
        ↓
advance CalculatedSummaryRevision, republish current/, mark item Completed
```

A `Superseded` result (Section 14.5) never satisfies the barrier, and a later recalculation of an already-summarized day re-requests the summary rather than decrementing the counter.

## 11.3 Calculated datasets

The Calculated Zone contains only data produced by calculation business logic.

```text
/calculated/
├── loss-allocation-10minute/
├── other-loss-allocation/
├── daily-wtg-calculated-loss/
├── daily-windfarm-calculated-loss/
├── daily-wtg-other-loss/
└── daily-windfarm-other-loss/
```

Recommended grains:

```text
Daily WTG calculated loss:
WindfarmId + WtgId + UtcDate + LocalDate + LostProductionTypeId

Daily windfarm calculated loss:
WindfarmId + UtcDate + LocalDate + LostProductionTypeId

Daily WTG other loss:
WindfarmId + WtgId + UtcDate + LocalDate + LostProductionTypeId + OtherLossTypeId

Daily windfarm other loss:
WindfarmId + UtcDate + LocalDate + LostProductionTypeId + OtherLossTypeId
```

The `UtcDate + LocalDate` split key is the same rule as Section 6.4.3 and must match the Harmonized Windfarm-Day aggregate exactly. If Calculated used a single date key while Harmonized used the split key, loss and production could not be joined on a common calendar and `LossPercentage` (Section 12.4) would silently mix two definitions of "day".

Windfarm-level calculated summaries must reconcile to the corresponding WTG-level summaries.

## 11.4 No duplicate production aggregates

The following datasets are **not** copied into Calculated:

```text
DailyWtgProductionSummary
DailyWindfarmProductionSummary
```

They already exist in Harmonized.

They are also not copied into Gold unless performance measurement later proves that a specific materialized serving dataset is justified.

## 11.5 Complete revision publication

Each Calculated revision is a complete replacement for its logical partition, not a delta.

Example:

```text
/calculated/daily-wtg-calculated-loss/
    windfarm=WF01/
        date=2026-01-15/
            revision=17/
            revision=18/
```

Revision 18 contains the complete current state for that logical partition.

Publication remains idempotent, and stale revisions must never replace the current requested revision.

---

# 12. Query and Serving Architecture

## 12.1 Responsibility

Serve UI requests by querying the minimum required datasets from Harmonized and Calculated without copying them into another mandatory serving store.

The query engine is responsible for:

- Predicate filtering
- Partition pruning
- Joining Harmonized and Calculated datasets
- Aggregation and grouping
- Ratio, percentage and average calculation
- Returning only the reduced result to the Application API

## 12.2 Initial query engine — Synapse Serverless

The initial implementation uses **Synapse Serverless SQL** over Azure Blob Parquet.

```text
Application.Api on EKS
        ↓
Synapse Serverless SQL
        ↓
Azure Blob Harmonized / Calculated
        ↓
filtered / aggregated result
        ↓
Application.Api
```

Large scans and aggregations therefore execute close to Azure Blob Storage. Only the reduced query result crosses back to EKS.

No Delta Lake or Iceberg table format is required.

## 12.3 Pluggable query-engine providers

The API must not be coupled to Synapse SQL syntax.

```text
Application.Api
    ↓
Business query services
    ↓
Analytics query provider
    ├── SynapseServerlessProvider
    ├── DuckDbProvider
    └── AthenaProvider
```

Engine-specific SQL, authentication and file/table access remain inside the provider implementation.

A future move from:

```text
Azure Blob + Synapse Serverless
```

to:

```text
S3 + Athena
```

therefore does not require redesigning the external API contract.

## 12.4 Windfarm range queries

Example windfarm metric inputs:

```text
Possible production
  → Harmonized daily-windfarm-wtgs-production

Calculated loss
  → Calculated daily-windfarm-calculated-loss

Other loss
  → Calculated daily-windfarm-other-loss
```

The query engine joins or aggregates these by:

```text
WindfarmId + <UtcDate | LocalDate>
```

The date grouping column is selected by the caller's requested calendar — `UtcDate` or `LocalDate` (Section 6.4.3). Both Harmonized production and Calculated loss carry the same split key, so the join is valid under either choice. The Application API therefore exposes an explicit `calendar` parameter and never leaves it implicit; a request must not mix calendars across numerator and denominator.

When `calendar = Local`, the provider must widen the UTC partition predicate by one day before applying the `LocalDate` filter (Section 6.4.3). Deriving the partition predicate directly from the requested local dates truncates the boundary days; filtering on `LocalDate` without a partition predicate destroys pruning. Both mistakes are silent, so this belongs in the provider implementation rather than in caller-written SQL.

Example:

```text
LossPercentage =
SUM(CalculatedLoss)
/
SUM(PossibleProduction)
```

No combined Gold file is required.

## 12.5 WTG range queries

For turbine-level UI metrics:

```text
Harmonized daily-windfarm-wtgs-production
          +
Calculated daily-wtg-calculated-loss
          +
Calculated daily-wtg-other-loss
          ↓
Query engine
          ↓
Application.Api
```

### 12.5.1 Event timeline and selected-range event queries

The UI may request the outages, alarms, curtailments and adjustments that overlap a selected WTG/date range, including events that started before the requested period or are still open.

The serving flow is deliberately two-stage:

```text
UI selects Windfarm / Item / date range
        ↓
Synapse reads /harmonized/windfarm/.../date=*/current/
        daily-item-event-references.parquet
        ↓
distinct EventIds + EventType + EventStartYear/Month
        ↓
query the bounded canonical monthly event partitions
        ↓
return full logical event details
```

This is how a September request discovers an outage that started in August: the September daily-reference rows contain the August event's `EventId` and start-month locator even though the full event record is physically stored under August.

The event-overlap rule is always half-open:

```text
EventStartUtc < RangeEndUtc
AND (EventEndUtc IS NULL OR EventEndUtc > RangeStartUtc)
```

The query provider may execute the reference resolution and event-detail lookup as two bounded queries. It must not fall back to wildcard-scanning every historical event month merely because one event can cross a month boundary.

### Combining events with calculated lost production

Harmonized event references contain no calculated loss. When the UI also needs the sum of lost production for the selected days, Synapse combines the event-serving rows with the **Calculated daily Item/Windfarm summaries** for the same selected calendar/range.

Conceptually:

```text
Harmonized daily event references
            +
Calculated daily WTG loss / other-loss
            ↓
Synapse
            ↓
selected-range event timeline + selected-range loss totals
```

The calculated measure should be aggregated independently at its native additive grain before it is associated with the event list. A naive join from one Item-Day calculated summary to multiple event-reference rows can multiply the same day's loss once per event and therefore overstate the total.

Correct pattern:

```text
1. Aggregate calculated loss by the requested Item/Date/LossType grain.
2. Resolve/deduplicate event references for the same requested range.
3. Combine the two result sets for presentation without re-summing duplicated loss rows.
```

If the business later requires **exact lost production per EventId** when multiple independent events overlap the same Item-Day, the daily Item summary alone is not sufficient. In that case use the existing 10-minute allocation detail carrying `eventId`, or materialize a derived Calculated daily-event-loss dataset only if performance measurements justify it. Do not solve that requirement by placing calculated loss into Harmonized.

## 12.6 Partial-day and 10-minute drill-down

Drill-down is a **bounded point read**, not a range scan, and it is the one case that touches 10-minute detail.

It is always scoped by the UI to a specific windfarm and a small date range — typically one day — and usually a specific turbine.

```text
Application.Api resolves the current component revisions
from ItemDayInputState / CalculatedPartition in Azure SQL
        ↓
explicit file paths for the requested Item-Days only
        ↓
Harmonized production-10minute (Item-Day)
        +
Calculated loss-allocation-10minute
        ↓
Query engine JOIN over the named paths
        ↓
Application.Api
```

Rules:

```text
1. Paths are resolved from SQL and passed to the query engine literally.
2. No wildcard, no revision-spanning scan, no external table over /harmonized/item/**.
3. The request must be bounded by windfarm + date range before paths are resolved.
4. Range and portfolio metrics never use this path; they use the day-grain
   aggregates in Sections 12.4 and 12.5.
```

Because the scope is a handful of Item-Day files, small-file overhead is irrelevant here and no compacted 10-minute serving copy is required.

Production is not duplicated into Calculated. The query engine is intentionally responsible for combining source-derived and calculation-derived detail.

## 12.7 Optional Gold materialization

Create a physical Gold dataset only when measurement proves that a repeated query is too expensive when derived from Harmonized + Calculated.

Potential examples:

- Expensive multi-dataset portfolio metrics
- Frequently requested long-range corporate rollups
- A serving dataset required to meet an agreed latency SLO

Until such a requirement exists:

```text
Gold Zone = optional / empty
```

---

# 13. Application API

## 13.1 One API deployable

The initial design uses one Application API Docker image for:

- Metrics queries
- Adjustment create/update/cancel
- User recalculation requests
- Processing/run status
- Operational status required by the UI

## 13.2 Adjustment path

Adjustments do **not** write directly to Harmonized.

```text
UI
 ↓
Application.Api
 ↓
Azure SQL Adjustment + AdjustmentItem + AdjustmentAudit + Outbox   (one transaction)
 ↓
Outbox Processor
 ↓
Raw adjustment Parquet
 ↓
raw-fanout SQS
 ↓
Raw Fan-out Coordinator
 ↓
harmonization-processing SQS
 ↓
Harmonizer
 ↓
Harmonized adjustment-day-segments
 ↓
Calculation Package Coordinator mints/requests a new package for affected Item-Dates
 ↓
Calculation SQS
```

SQL is the transactional system of record for authoring adjustments.

Raw is the immutable processing/audit representation.

Harmonized is the canonical calculation-ready representation.


## 13.2.1 Adjustment UI behavior

Adjustment saving and recalculation are two separate user-visible operations.

The UI must **not** block the whole page until recalculation finishes.

### Save interaction

When the user clicks **Save**:

```text
User clicks Save
      ↓
Save button shows a short loading spinner
      ↓
Application API commits:
Adjustment + AdjustmentAudit + Outbox
      ↓
API returns success quickly
      ↓
Adjustment dialog closes / returns to normal
      ↓
Page shows:
"Adjustment saved - recalculation in progress"
```

The Save-button spinner represents only the synchronous database transaction. It must not remain active for the full calculation pipeline.

A successful save response should return an identifier that the UI can use to track the resulting recalculation, for example:

```json
{
  "adjustmentId": "ADJ-1052",
  "recalculationId": "RC-88129",
  "status": "Processing"
}
```

### While recalculation is running

The UI continues to display the **last committed calculation result**.

Example:

```text
RequestedPackageRevision = 18
CommittedPackageRevision = 17
```

The page continues showing revision 17 and displays a non-blocking status banner such as:

```text
Adjustment saved - recalculation in progress.
Displaying the previous calculated values until the update completes.
```

The user remains free to navigate and continue working.

Do not expose partially calculated revision 18 values.

### Status monitoring

Initially, simple polling is sufficient.

For example:

```text
GET /recalculations/{recalculationId}
```

Possible response while processing:

```json
{
  "status": "Processing",
  "requestedPackageRevision": 18,
  "committedPackageRevision": 17
}
```

Polling every approximately 2-5 seconds is sufficient initially. SignalR/WebSockets are not required unless future UX requirements justify them.

### Completion

When calculation and publication complete:

```text
RequestedPackageRevision = 18
CommittedPackageRevision = 18
```

the UI:

1. Refreshes the affected metrics automatically.
2. Replaces the progress banner with a short-lived success message.

Example:

```text
Recalculation complete - data updated.
```

The success message may auto-dismiss after a short period.

### Long-running calculation

If recalculation takes longer than expected, the UI should remain non-blocking.

Example progression:

```text
Initial:
Recalculation in progress...

Longer than normal:
Recalculation is taking longer than usual.
You may continue working.
```

Do not leave the page in an indefinite global loading state.

### Recalculation failure

A recalculation failure must be distinguished from an adjustment-save failure.

If the SQL transaction succeeded but downstream calculation fails, display:

```text
Adjustment saved.
Recalculation failed.
```

Do **not** tell the user that the adjustment failed, because the adjustment remains committed in Azure SQL.

The UI may expose:

```text
Retry
View details
```

subject to operational permissions.

### Multiple users

Calculation status belongs to the affected data revision, not only to the browser session that created the adjustment.

If multiple users have the same page open:

```text
CommittedPackageRevision = 17
RequestedPackageRevision = 18
```

all users continue to see the last committed revision until revision 18 completes.

When the new revision becomes committed, subsequent polling/refresh causes each UI session to load the latest data.

The UI does not need to expose which user created the adjustment unless this is a business requirement.

### UX principle

> **Short Save-button spinner -> non-blocking recalculation banner -> continue showing the last committed values -> automatically refresh when the new revision commits.**


## 13.3 Query-engine interface

Prefer business-oriented query services over exposing generic engine SQL to controllers.

Example:

```csharp
public interface IWindfarmMetricsService
{
    Task<WindfarmMetricsResult> QueryAsync(
        WindfarmMetricsRequest request,
        CancellationToken cancellationToken);
}
```

Internally:

```text
IWindfarmMetricsService
        ↓
IAnalyticsQueryProvider
        ├── SynapseServerlessAnalyticsQueryProvider
        ├── DuckDbAnalyticsQueryProvider
        └── AthenaAnalyticsQueryProvider
```

The provider owns:

- SQL dialect differences
- External-file/table references
- Authentication
- Parameterization
- Query execution
- Provider-specific error translation

# 14. Feature / Zone 8 — Reliability, Backfill and Observability

## 14.1 Pod failure — Harmonizer

```text
harmonization-processing message
        ↓
Harmonizer processing
        ↓
Pod dies
        ↓
Visibility timeout expires
        ↓
Another Harmonizer Pod retries the batch
```

Harmonization must be idempotent. Item-Days already committed by the previous attempt re-merge to the same `CanonicalContentHash` and mint no new revision.

This is a **batch transient failure** in the taxonomy of Section 5.7 and requires no durable record.

---

## 14.2 Pod failure — Calculation

Use both:

```text
SQS visibility timeout
+
Work ledger lease
```

Fields:

```text
LeaseOwner
LeaseExpiresAt
AttemptCount
```

If the pod dies:

```text
Visibility timeout expires
Lease expires
        ↓
Another worker takes over
```

---

## 14.3 Pod failure — Calculated summary publisher

If a publisher dies:

```text
Calculated Summary Publication SQS
        ↓
Visibility timeout
        ↓
Retry
```

Publication must use an idempotent PublicationBatchId.

---

## 14.4 Dead-letter handling

Recommended DLQs:

```text
raw-fanout-dlq
harmonization-processing-dlq
calculation-dlq
summary-generation-dlq
```

Poison work should:

- Retain diagnostics
- Prevent successful run completion
- Trigger operational alerting
- Support controlled replay

`maxReceiveCount` is configurable per queue, with 5 as a starting point, and each queue's visibility timeout must exceed the p99 processing duration of its message type (Section 5.7).

Two DLQs carry additional obligations:

```text
raw-fanout-dlq
  → a consumer upserts a FanOutFailure row (Section 5.9)
  → because SQS DLQ retention expires and the affected Item-Days are unknown,
    the SQS message alone is NOT a durable recovery handle

harmonization-processing-dlq
  → always indicates a BATCH-WIDE infrastructure cause, never one bad row,
    because item failures are isolated by construction (Section 5.7)
```

Data-quality failures never appear on a DLQ. They surface as `ItemDayInputState.Status = Failed` and, for Production, as a Windfarm-Day barrier short of its expected count. Two signals, two runbooks, no overlap.

---

## 14.5 Stale result protection

Example:

```text
Package 18 processing
        ↓
New correction/backfill arrives
        ↓
RequestedPackageRevision becomes 19
        ↓
Package 18 finishes
```

Before publication:

```text
ComputedPackageRevision == RequestedPackageRevision?
```

If no:

```text
Mark result Superseded
Do not publish
```

---

## 14.6 Open event handling

Open outage/alarm/event intervals may produce provisional data.

Lifecycle tracking is owned by `EventMaterializationState` (Section 6.11), which replaces any ad-hoc `HasOpenEvents` flag: an open event is a row with `LifecycleStatus = Open`, and the indexed set of such rows is what the clock-driven continuation sweep consumes.

When an event closes or its effective end changes:

```text
Affected Item-Date partitions
        ↓
new Harmonized event component revision
        ↓
Calculation Package Coordinator requests a new package revision
```

The same package-driven recalculation path is used. Two rules from Section 6.11 apply and must not be dropped here: an open event keeps being materialized every processable UTC day even when no source row arrives, and a close or shrink must rebuild the days the event **no longer** covers as explicitly empty components.

---


## 14.7 Source Availability and Data Availability Monitoring

The solution must explicitly monitor both **source availability** and **data availability**.

These are different concerns:

```text
Source availability
= Can the upstream source be reached and queried successfully?

Data availability
= Did the expected data actually arrive and become usable?
```

A source can be healthy while expected data is missing, late, or incomplete.

### Source availability

Track health per upstream source/system.

Suggested logical state:

```text
SourceAvailability
------------------
SourceSystem
SourceType
LastSuccessfulContactUtc
LastSuccessfulDataUtc
CommittedCursorValue
CommittedCursorAdvancedAtUtc
Status
ConsecutiveFailures
LastError
CheckedAtUtc
```

`CommittedCursorValue` / `CommittedCursorAdvancedAtUtc` expose the Section 4.4 extraction cursor. A source that is `Healthy` and reachable while its cursor has not moved is a distinct failure mode from an unreachable source, and it has no other alerting signal.

Suggested statuses:

```text
Healthy
Degraded
Unavailable
Unknown
```

Typical checks include:

- Source database/API connectivity
- Authentication/authorization success
- Query/API response success
- Response latency
- Consecutive failure count
- Time since last successful data retrieval
- Time since the extraction cursor last advanced

Example alerts:

```text
Source API unreachable for 3 consecutive checks
Source database query failed repeatedly
No successful source contact for more than threshold
Source latency exceeds agreed threshold
Source Healthy but extraction cursor has not advanced beyond threshold
```

### Data availability

Track whether expected data exists for the logical dataset partition.

Suggested logical state:

```text
DataAvailability
----------------
Dataset
WindfarmId
WtgId
DataDate
ExpectedCount
ActualCount
LatestTimestampUtc
Status
SourceRevision
CheckedAtUtc
```

Suggested statuses:

```text
Expected
Partial
Complete
Late
Missing
Invalid
```

For 10-minute production, a complete normal day typically contains:

```text
ExpectedCount = 144
```

Examples:

```text
ExpectedCount = 144
ActualCount   = 144
Status        = Complete
```

or:

```text
ExpectedCount = 144
ActualCount   = 136
Status        = Partial
```

Item-Day production completeness is evaluated on **UTC** dates (Section 6.4.2) and therefore normally expects 144 intervals. DST and local-day variation applies only to reporting and query-time grouping over local-time columns, never to the readiness or completeness rule.

### Pipeline-stage availability

Operational monitoring should expose data freshness through the complete pipeline:

```text
Source reachable?
      ↓
Extraction cursor advancing?
      ↓
Expected Raw artifact written and published?
      ↓
Raw artifact enumerated?          (no open FanOutFailure)
      ↓
Harmonized Item-Days Complete?    (none Failed)
      ↓
Calculation completed?
      ↓
Committed revision available to query?
```

This makes it possible to distinguish:

```text
Source problem
vs
Ingestion problem
vs
Harmonization delay
vs
Calculation backlog
vs
Serving/query issue
```

### Example operational view

```text
Windfarm: WF01 / 2026-09-01 / Production

Production Source        Healthy
Latest Source Data       13:40 UTC
Extraction cursor        advanced 13:42 UTC
Raw artifact             extraction=41, published
Fan-out                  enumerated (no open FanOutFailure)
Harmonized               1499 / 1500 Complete, 1 Failed
                         T0831 — source row violates schema constraint
                         AttemptCount 4 of 6
DLQ batches              0
Calculation              Blocked for T0831, processing others
Latest Committed Data    13:20 UTC
```

This view is assembled from `WindfarmItemMembership` (expected count), `ItemDayInputState` (complete/failed/error), `FanOutFailure` (unenumerated artifacts) and DLQ depth. No fan-out ledger is required to produce it.

### Recommended alerts

Examples:

```text
No new production data for more than 30 minutes
Expected 144 intervals but received fewer than expected
Raw data available but Harmonized delayed beyond threshold
Harmonized available but Calculation delayed beyond threshold
Calculation queue oldest-message age exceeds threshold
Committed data freshness exceeds threshold
Source unavailable for consecutive checks
Event Open for longer than OpenEventAgeThreshold for its EventType (Section 6.10.2)
Any message arrives on a DLQ                                    (Section 14.4)
An unresolved FanOutFailure row exists                          (Section 5.9)
ItemDayInputState.Status = Failed beyond retry backoff          (Section 5.8)
An Item-Day exhausts MaxItemAttempts
Extraction lease held beyond expected run duration
```

Thresholds should be configurable by dataset/source because different inputs may have different expected arrival patterns.

### Monitoring implementation

Use the existing observability stack for operational metrics, logs and traces, while retaining compact relational state only where a current availability/status view is useful.

Recommended telemetry includes:

- Source health status
- Last successful source contact
- Last successful data timestamp
- Extraction cursor value and time since last cursor advance
- Expected vs actual interval count
- Raw arrival latency
- Harmonization latency
- Calculation latency
- End-to-end data freshness
- SQS queue depth
- Oldest-message age
- Retry/DLQ counts
- Reconciliation backlog
- Query success/failure and latency

The monitoring model should answer two operational questions quickly:

> **Is the source available?**

and

> **Is the expected data available and usable through the application?**


# 15. Backfill Handling

## Horizontal reprocessing model

Historical **source backfill** and pure **calculation reprocessing** are different operations.

Source backfill follows the bounded extraction path in Section 4, creates new Raw/Harmonized component revisions, then triggers calculation.

Pure calculation reprocessing does not pull the source and is covered in Section 16.

For calculation, large historical work is decomposed into many independent Item-Day executions rather than one monolithic dataset:

```text
Historical calculation request
        ↓
resolve Item-Day packages / inputs
        ↓
Backfill SQS
        ↓
many stateless Calculation Worker pods
        ↓
independent Calculated Item-Day outputs
```

The calculation time of one Item-Day is largely independent of the total historical size because the worker reads only the exact package for that Item-Day.

Idealized compute-bound scaling is approximately:

```text
elapsed compute time
≈ total work / parallel workers
```

Actual end-to-end time is bounded by the slowest shared resource:

```text
Total elapsed time
≈ max(
    total compute / parallel workers,
    total read I/O / storage throughput,
    total write I/O / storage throughput,
    orchestration/control-plane throughput
)
```

Increasing pod count can reduce duration substantially, but scaling is not unlimited. Azure Blob throughput, cross-cloud network bandwidth, SQL control-plane writes, node startup, source-system limits during extraction, and workload skew can become bottlenecks.

Backfill and normal processing must use separate queues and independent scaling policies so large historical work does not starve daily processing.

Capacity tests should measure at least:

```text
average seconds per Item-Day
bytes read per Item-Day
bytes written per Item-Day
SQS backlog / oldest-message age
Blob throughput
SQL control-plane throughput
pod count vs throughput curve
Harmonization batch size vs throughput
Raw file opens / bytes read per Harmonization batch
```

Use measurements to determine efficient parallelism and batch size rather than assuming linear scaling.

## 15.1 Production source backfill

Production source backfill uses the same bounded change-discovery and Windfarm-Day read-back strategy as normal extraction.

Example:

```text
Windfarm: WF01
Last committed source cursor: 2026-05-15
Current source horizon:       2026-08-30
```

Flow:

```text
acquire extraction lease
        ↓
split LastUpdated catch-up into bounded windows (normally ≤ 1 month)
        ↓
windfarm-level change discovery using (LastUpdatedDate, Id)
        ↓
derive affected ProductionDates
        ↓
re-read complete Windfarm-Day snapshots
        ↓
write Raw artifacts under extraction=<token>
        ↓
publish raw-fanout SQS messages
        ↓
fenced cursor advance
        ↓
Raw Fan-out Coordinator
        ↓
Item-Day batches → harmonization-processing SQS
        ↓
new Harmonized Production component revisions
        ↓
Calculation Package Coordinator
        ↓
new package revisions for affected Item-Days
        ↓
Calculation / Backfill SQS
        ↓
new Calculated results
```

A source backfill therefore does pull the upstream source. A user-requested pure recalculation does not.

---

## 15.2 Event correction and footprint reconciliation

An arriving event revision is not handled as a delta against yesterday's Parquet file. The platform treats the canonical event revision as authoritative and reconciles every affected `(EventId, ItemId)` against what was actually materialized.

The governing rule is:

```text
For each (EventId, ItemId):

OLD MATERIALIZED FOOTPRINT
= MaterializedFromDate ... MaterializedThroughDate

NEW REQUIRED FOOTPRINT
= UTC Item-Days overlapped by the new canonical event revision
  subject to CalculationProcessableDate for calculation materialization

AFFECTED ITEM-DAYS
= OLD MATERIALIZED FOOTPRINT
  UNION
  NEW REQUIRED FOOTPRINT
```

The old side is deliberately based on `EventMaterializationState`, not merely the old event's declared date range. The materialization watermark records what was actually projected into Item-Day components and may differ from the declared interval when a day failed, a watermark lagged, or an item's applicability changed later.

### 15.2.1 Overlapping old and new outage ranges

Example:

```text
Old revision 7:
O1001  Jan01 10:00 ───────────── Jan10 18:00

New revision 8:
O1001          Jan05 08:00 ───────────── Jan15 12:00
```

Affected dates are the union of both footprints:

```text
Jan01 ... Jan15
```

The treatment is:

```text
Jan01-Jan04
→ old-only footprint
→ rebuild outage component WITHOUT O1001

Jan05-Jan09
→ overlap
→ rebuild using the NEW canonical O1001 revision

Jan10
→ overlap boundary day
→ rebuild using the NEW exact interval

Jan11-Jan14
→ new-only footprint
→ rebuild WITH O1001

Jan15
→ new boundary day
→ partial-day segment ending at 12:00
```

Overlap does not mean the old component can be kept blindly. Start time, end time, classification, source revision, or other canonical fields may have changed. Every affected day is rebuilt from the complete current canonical event set.

### 15.2.2 Completely shifted / non-overlapping outage

Example:

```text
Old revision 7:
O1001  Jan01 10:00 ───── Jan05 12:00

New revision 8:
O1001                           Feb10 08:00 ───── Feb12 18:00
```

There is no overlap.

The Event Lifecycle Coordinator schedules both regions:

```text
OLD cleanup footprint:
Jan01-Jan05

NEW creation footprint:
Feb10-Feb12
```

The January Item-Days are rebuilt without O1001. The February Item-Days are rebuilt with O1001.

The event is not "moved" by deleting old files. It remains the same stable `EventId = O1001` with a new canonical revision, and both its old materialized location and new required location are reconciled.

### 15.2.3 Boundary days are partial, not keep-or-empty

A correction to:

```text
New EndUtc = Jan20 14:20
```

means:

```text
Jan01-Jan19   full-day event applicability where appropriate
Jan20         partial-day rebuild ending at 14:20
Jan21+        event no longer applies
```

The half-open `[start, end)` interval rule remains authoritative.

### 15.2.4 Rebuild the whole event-type component

For each affected Item-Day, the Harmonizer rebuilds the complete current component from the canonical event set.

Example:

```text
Before Jan25 outage component:
O1001
O2005

O1001 shifts away from Jan25.

Rebuilt Jan25 outage component:
O2005
```

If no outage remains, authoritative emptiness is recorded:

```text
ItemDayInputState
-----------------
InputType              Outage
Status                 Complete
RecordCount            0
CanonicalContentHash   <normal hash of the empty canonical row set>
Path                   NULL        ← default logical-empty representation
```

Absence of `ItemDayInputState` must never mean "empty".

A zero-row Parquet file may optionally be materialized for a correction where forensic visibility is useful, but logical-empty is the default because most Item-Days naturally have no outage/adjustment and creating physical zero-row files for every empty component would create millions of unnecessary small objects.

The component contract is therefore:

```text
RecordCount = 0
→ authoritative empty component
→ Path may be NULL (default) or point to an optional zero-row Parquet file

RecordCount > 0
→ Path is required
→ Path points to the immutable Parquet component
```

`CanonicalContentHash` always uses the same canonical-row hashing algorithm. Empty content is hashed as the empty canonical row set; no magic `EMPTY` sentinel is used.

### 15.2.5 No-op suppression

After rebuilding a candidate component:

```text
canonicalize rows
        ↓
deterministic sort
        ↓
CanonicalContentHash
        ↓
compare with committed component hash
```

If the hash is unchanged:

```text
no new ComponentRevision
no new PackageRevision
no recalculation
no summary republication
```

This is particularly important for overlapping event revisions, where many interior days may be byte-for-byte identical even though the overall event interval changed.

### 15.2.6 Fenced commit and out-of-order protection

A late older message must never overwrite a newer event revision.

Processing order:

```text
write candidate component
        ↓
validate candidate
        ↓
compare content hash
        ↓
fenced / optimistic ItemDayInputState commit
        ↓
only if the expected event/component generation is still current
```

A stale worker whose compare-and-swap affects zero rows discards its candidate and does not advance the materialization watermark.

### 15.2.7 Large blast radius

A backdated correction can affect hundreds or thousands of Item-Days. It is never dispatched as one synchronous operation.

```text
Event Lifecycle Coordinator
        ↓
enumerate affected (EventId, ItemId, DataDate)
        ↓
chunk into bounded batches
        ↓
harmonization-processing SQS
```

Large historical corrections should be throttled or routed through lower-priority/backfill capacity so current operational processing is not starved. A configurable impact cap should raise an operational alert when a single event revision affects an unusually large number of Item-Days.

### 15.2.8 Downstream propagation

A changed Item-Day component is only the beginning of the correction path:

```text
new committed ItemDayInputState revision
        ↓
Calculation Package Coordinator
        ↓
new PackageRevision where input identity changed
        ↓
Calculation Worker
        ↓
new Calculated revision
        ↓
WindfarmDayCalculationState
        ↓
SummaryWorkItem (CalculatedLoss)
        ↓
Summary Worker
        ↓
republish calculated current/
```

Event serving is updated independently:

```text
canonical event revision changed
        ↓
invalidate WindfarmDayEventReferenceState
        ↓
SummaryWorkItem (HarmonizedEventReference)
        ↓
Summary Worker
        ↓
republish daily-item-event-references.parquet
```

`CalculationProcessableDate` caps calculation materialization. A current UTC day may still be represented in the event-serving projection even when that day is deliberately not yet calculation-processable.

### 15.2.9 Failure and watermark rule

`MaterializedThroughDate` is a contiguous committed watermark.

```text
Jan21 ✅
Jan22 ✅
Jan23 ❌
Jan24 ✅
Jan25 ✅

MaterializedThroughDate = Jan22
```

The watermark must never jump over Jan23. The reconciliation sweep retries the hole.

Hard ordering rule:

```text
write candidate component
        ↓
fenced ItemDayInputState commit
        ↓
ONLY THEN advance EventMaterializationState
```

This applies equally to overlap, shrink, extension, deletion and completely shifted/non-overlapping revisions.

## 15.3 Alarm backfill

If alarm data only affects explanation:

```text
Alarm backfill
→ rerun enrichment / attribution only
```

If alarms influence numeric classification, full recalculation is triggered.

---

# 16. User-Requested Recalculation and Fresh Historical Refresh

The platform exposes two deliberately different operations:

```text
Recalculate
→ reuse existing authoritative Harmonized inputs
→ no source pull

Refresh Source & Recalculate
→ reload authoritative source data
→ rebuild/reconcile Harmonized inputs
→ then calculate
```

These operations must never be conflated.

## 16.1 ForceRecalculate — calculation only

Pure user-requested recalculation does **not** require source extraction, Raw, or re-harmonization.

```text
User selects windfarm/items + date range
        ↓
Create RecalculationRun
Mode = ForceRecalculate
        ↓
resolve affected Item-Days
        ↓
resolve existing immutable package/input references
        ↓
create CalculationWorkItems
        ↓
Backfill SQS
        ↓
many Calculation Workers
        ↓
read existing Harmonized Zone only
        ↓
write new Calculated result revisions
        ↓
run-level completion / publication
```

No new Harmonized component revision is created merely because the user pressed **Recalculate**.

If component revisions, `configVersion`, `calculationVersion`, and `tzdbVersion` are unchanged, the existing package manifest may be reused. A new package revision is required only when package identity changes.

```text
PackageRevision
= exactly WHAT inputs/config/code were calculated

RecalculationRun / ResultRevision
= WHEN and WHY that package was executed again
```

## 16.2 RefreshAndRecalculate — fresh source rebuild

Use `RefreshAndRecalculate` when the source data itself may be wrong or materially different.

Typical reasons:

- one year must be rebuilt from source,
- production has been corrected,
- outage history may be completely different,
- alarms may have changed,
- curtailments may have changed,
- events may have disappeared,
- an outage may move to a completely different date range,
- an old event may shrink, extend, close or be deleted.

Flow:

```text
User requests historical refresh
        ↓
Create RecalculationRun
Mode = RefreshAndRecalculate
        ↓
reload Production
reload Outages
reload Alarms
reload Curtailments
        ↓
write NEW Raw extraction artifacts
        ↓
Harmonized rebuild
        ↓
event snapshot / footprint reconciliation
        ↓
new ItemDayInputState revisions where content changed
        ↓
Calculation Package Coordinator
        ↓
new package revisions
        ↓
Backfill Calculation SQS
        ↓
Calculation Workers
        ↓
new Calculated revisions
        ↓
Summary publication
        ↓
publish new coherent range snapshot
```

Fresh recalculation does **not** delete old Harmonized or Calculated files.

```text
Harmonized:
revision=6   historical
revision=7   rebuilt authoritative state

Calculated:
revision=20  historical result
revision=21  rebuilt result
```

The authoritative SQL/current pointers move to the new committed revisions only after the relevant work succeeds.

## 16.3 Production refresh

For Production, the requested historical Windfarm-Day range is read again from the authoritative source.

```text
Production source
        ↓
new extraction lease/token
        ↓
new Raw Windfarm-Day artifact
        ↓
Raw Fan-out Coordinator
        ↓
Harmonizer
        ↓
new Production component revision if content changed
```

The old Raw and Harmonized revisions remain immutable.

## 16.4 Event snapshot reconciliation

Event refresh cannot be implemented as "process whatever rows the source returned", because events may disappear completely.

For each refreshed scope, compare:

```text
old canonical event state
        +
new authoritative source snapshot
        ↓
Added
Changed
Unchanged
Removed
```

Example:

```text
EventId   Old                    New                    Result
-------   ---------------------  ---------------------  --------
O1001     Jan01 → Jan30          Jan01 → Jan20          Changed
O1002     Mar01 → Mar05          absent                 Removed
O1003     Aug10 → Aug12          absent                 Removed
O2001     absent                 Apr02 → Apr07          Added
```

Processing then uses the Section 15.2 materialization-state reconciliation rules:

```text
Changed
→ reconcile old materialized footprint ∪ new required footprint

Removed
→ reconcile old materialized footprint, rebuilding without the removed event

Added
→ materialize the new required footprint

Unchanged
→ content hash no-op
```

A removed event is not represented by deleting old Parquet revisions. The affected Item-Day event-type components are rebuilt from current canonical state; days with no remaining event become authoritative logical-empty revisions.

## 16.5 RecalculationRun state

Suggested run state:

```text
RecalculationRun
----------------
RunId
RequestedBy
WindfarmId / selection scope
StartDate
EndDate
Reason
Mode              ForceRecalculate | RefreshAndRecalculate
Status
ExpectedWorkItems
CompletedWorkItems
FailedWorkItems
CreatedAtUtc
StartedAtUtc
CompletedAtUtc
PublishedAtUtc
```

Suggested `Status` values:

```text
Preparing
RefreshingSource
Harmonizing
Calculating
Publishing
Completed
Failed
Cancelled
```

## 16.6 Range publication boundary

Item-Day remains the calculation atomicity boundary, but a large user-requested range has a run/snapshot publication boundary so the UI does not unknowingly mix old and newly rebuilt days within one requested annual metric.

During execution:

```text
RecalculationRun: 34,200 / 36,500 complete
        ↓
keep serving the previously published consistent snapshot for that run scope
```

After the run's publication policy is satisfied:

```text
all required refresh/harmonization work complete
        ↓
all calculations complete and validated
        ↓
required summaries complete
        ↓
publish the new run/snapshot
        ↓
UI switches consistently
```

The exact snapshot implementation may reuse existing publication manifests/current pointers, but annual/range metrics such as Loss %, Yield, and Availability must never silently mix incompatible old/new states.

## 16.7 Operational semantics

A fresh historical rebuild can be large, so it should use backfill capacity and bounded batching.

The run must expose progress by phase:

```text
RefreshingSource
→ Harmonizing
→ Calculating
→ Publishing
```

A failure in one phase does not make older published data disappear. The previous published snapshot stays current until the new range satisfies publication policy.

# 17. Configuration Versioning

Calculation configuration should be versioned.

Examples include:

- Event precedence
- Effective-duration rules
- Classification rules
- Allocation coefficients
- Formula selection
- Display-related calculation configuration
- Per-windfarm reporting timezone, effective-dated (Section 6.4.3)

Each work item resolves one configuration version and stamps it into the calculated output.

A result is therefore reproducible from:

```text
Harmonized component revisions (ComponentRevision + CommittedAttempt per input)
Adjustment component revision
Configuration version
Calculation version
tzdb version
PackageRevision
ResultRevision / RecalculationRunId where applicable
```

`PackageRevision` identifies exactly *what* was calculated; `ResultRevision` / `RecalculationRunId` identify *when and why* that package was executed (Section 16).

A configuration change creates new package revisions for affected Item-Date partitions.

---

# 18. Formula Strategy

Avoid implementing every metric or loss formula as a separate stored procedure.

Use configuration to select from a controlled set of formula shapes.

Example:

```text
SUM
RATIO
AVERAGE
WEIGHTED_AVERAGE
PERCENTAGE
DURATION
```

The trusted formula implementations remain in versioned C# code.

This improves:

- Testability
- Auditability
- Versioning
- Reuse
- Maintainability

---

# 19. Observability

Use end-to-end:

```text
RunId
WorkItemId
CorrelationId
RequestedPackageRevision
CommittedPackageRevision
```

Recommended telemetry:

- Structured application logs
- OpenTelemetry traces
- Queue depth
- Oldest SQS message age
- DLQ count, and open `FanOutFailure` count
- `ItemDayInputState` rows in `Failed`, and reconciliation-sweep backlog
- Extraction lease acquisitions, expiries, and fenced-commit rejections (`@@ROWCOUNT = 0`)
- Harmonization duration
- Calculation duration
- Publication duration
- Blob read/write throughput
- Cross-cloud network traffic
- Range-publication snapshot commit duration and `RecalculationRun` completion lag
- Files read and bytes scanned per Application API analytical request
- DuckDB query duration, peak memory, and spill events
- Result-cache hit rate
- Compaction backlog (incremental files awaiting compaction)
- SQL query latency
- Backfill backlog
- Recalculation progress
- Stale/superseded result count

---

# 20. Migration / Delivery Strategy

## Phase 1 — Reliability Foundation

Introduce:

- Calculation Work Ledger
- RequestedPackageRevision / RunningPackageRevision / CalculatedPartition commit state
- SQS calculation queues
- Backfill queue
- DLQ
- Lease handling
- Retry and idempotency
- Basic observability

Initially, the existing calculation engine may continue behind this orchestration model.

---

## Phase 2 — Raw and Harmonized Zones

Build:

```text
ExtractionCursor + fenced lease
→ Raw Zone (extraction=<token>)
→ raw-fanout SQS
→ Raw Fan-out Coordinator
→ harmonization-processing SQS
→ Harmonizer
→ ItemDayInputState (revision authority + readiness)
→ Harmonized
```

plus the failure-path components, which are not optional and should not be deferred to a later phase:

```text
Harmonization Reconciliation sweep   (Section 5.8)
FanOutFailure + raw-fanout-dlq consumer (Section 5.9)
```

Run alongside the existing process.

Validate Harmonized data without changing the user-facing result.

---

## Phase 3 — Calculation Engine and Calculated Zone

Refactor the C# calculation library.

Implement:

```text
Harmonized
→ Calculation Workers
→ Calculated Zone
```

Add deterministic interval segmentation and calculation invariants.

---

## Phase 4 — Query Engine and Serving

Build:

```text
Harmonized + Calculated
→ Synapse Serverless SQL
→ Query-provider abstraction
→ Application API
```

Deliver in this order:

1. Synapse Serverless access to Harmonized and Calculated Parquet
2. Business-oriented query interfaces in the Application API
3. Synapse provider implementation
4. Windfarm daily-range queries
5. WTG daily-range queries
6. Partial-day/detail joins over 10-minute datasets
7. Performance benchmarks and scan-cost monitoring
8. Optional DuckDB/Athena provider proof-of-concept only if required

Do not materialize Gold datasets unless benchmarks prove that a specific query requires it.

---

## Phase 5 — Dual Run and Reconciliation

Run old and new solutions in parallel.

Compare by:

- Item
- Date
- Loss type
- Outage
- Curtailment
- Allocation
- Total loss
- Daily aggregate
- UI metric

Differences must be understood and approved before cutover.

---

## Phase 6 — Application API Query Migration

Gradually replace expensive UI-section-specific stored procedures with:

```text
Application API
+
Synapse Serverless over Harmonized + Calculated Parquet
+
provider abstraction for future DuckDB/Athena implementations
```

---

## Phase 7 — Cutover

After production validation:

- Switch user-facing reads to the Application API over the selected analytical query provider
- Remove obsolete calculation-specific SQL processing and serving tables
- Retain Azure SQL for configuration, adjustments, audit/outbox, orchestration and other small transactional/control state
- Preserve Raw data for replay and audit

---

# 21. Operational Acceptance Criteria

The redesign is production-ready when:

- A terminated Harmonizer Pod can be retried safely
- A terminated Calculation Pod does not leave work permanently Running
- Queue redelivery resumes failed work
- Poison work reaches the DLQ with diagnostics
- A zombie extraction pod whose lease expired cannot advance the cursor, because the fenced commit affects zero rows
- Two extraction pods cannot extract the same `Windfarm + SourceType` concurrently
- Killing an extraction pod between the Raw write and the SQS send loses no work, because the cursor has not advanced
- A later correction to an already-extracted business date creates a new Raw artifact and never overwrites the earlier one
- One poison Item-Day inside a batch of 50 does not fail the other 49, and does not drive the batch to the DLQ
- A failed Item-Day is retried automatically by the reconciliation sweep, and alerts once `MaxItemAttempts` is exhausted
- An Item-Day whose newer source input failed to harmonize **cannot** be calculated from its previous component revision, because `Status = Failed` blocks readiness
- A required input with no `ItemDayInputState` row is treated as not-ready, never as "nothing to wait for"
- A Raw artifact that reaches `raw-fanout-dlq` produces a durable `FanOutFailure` row that outlives DLQ retention and alerts until replayed
- A harmonization batch on the DLQ always indicates a batch-wide infrastructure cause, never a single bad row
- Replaying a parent Raw artifact re-enumerates and reprocesses safely, because already-committed Item-Days are content-hash no-ops
- Older revisions cannot overwrite newer revisions
- Recalculation of the same complete partition does not duplicate results
- Open events are recalculated when they close
- User adjustments reach the Raw Zone through the outbox and reliably trigger recalculation; an API crash immediately after `COMMIT` cannot lose the lake push
- Historical backfills do not starve current operational processing
- Calculated outputs pass invariants before publication
- Calculated publication is idempotent and commits atomically through the manifest
- Windfarm-grain summary totals reconcile to Wtg-grain totals, and both reconcile to 10-minute detail
- Killing the worker that trips a Windfarm-Day barrier still produces the summary, because the `SummaryWorkItem` survives and is redelivered
- A late correction to one Item-Day after summary publication republishes the summary and never removes the published windfarm-day from the UI
- Rebuilding a historical windfarm-day resolves the same `ExpectedItemCount` as the original run, from the stored `MembershipVersion`
- The UI never sees a partially published recalculation, because a range request is served from one published `RecalculationRun` snapshot boundary (Section 16)
- A one-year windfarm yield request reads single-digit file counts and returns within the interactive latency budget
- Arbitrary range metrics are computed from additive components without scanning 10-minute data
- Detailed UI drill-down remains available from the Calculated Zone
- A UI range query returns events that started before the requested range but overlap it, including open-ended events
- Event references never contain calculated lost-production totals; selected-range loss is derived from Calculated datasets by the query engine
- Joining multiple event references to one Item-Day calculated summary cannot multiply the day's calculated loss
- A DuckDB query cannot be influenced by unvalidated user input, and cannot address a file outside the manifest
- Every result can be traced to source, adjustment, configuration, and calculation versions

---

# 22. Azure DevOps Feature Structure

Recommended feature grouping:

1. **Extraction and Raw Data Zone**
2. **Transformation, Harmonization and Source Aggregation Zone**
3. **Event Lifecycle and Materialization** (Section 6.11)
4. **Adjustment and Input Revision Management**
5. **Calculation Orchestration**
6. **Distributed Loss Calculation**
7. **Calculated Summary Publication**
8. **Query Engine and Application API**
9. **Reliability, Backfill and Observability**

---

# 23. Final Architecture Summary

```text
Source Systems
      ↓
Extraction Pods
  ├─ ExtractionCursor + fenced LeaseToken
  ├─ windfarm-level incremental discovery
  ├─ composite cursor (LastUpdatedDate, Id) + safety-lag re-read
  ├─ bounded catch-up windows
  └─ Production: affected-date Windfarm-Day read-back
      ↓
Azure Blob Raw Zone
  └─ /raw/<source>/windfarm=/date=/extraction=<token>/
      ↓
raw-fanout SQS                       ── DLQ → FanOutFailure
      ↓
Raw Fan-out Coordinator
  └─ enumerate → emit Item-Day batches directly
      ↓
harmonization-processing SQS         ── DLQ → batch-wide incident
      ↓
Harmonizer Pods
  ├─ per-item isolation; Complete / Failed per Item-Day
  └─ Reconciliation sweep retries Failed Item-Days
      ↓
Event Lifecycle Coordinator
  ├─ EventMaterializationState (EventId, ItemId) — Open / Closed / Deleted
  ├─ clock-driven continuation up to CalculationProcessableDate
  ├─ revision-driven correction over (old items × old dates) ∪ (new items × new dates)
  └─ re-dispatches affected Item-Days onto harmonization-processing SQS
      ↓
Azure Blob Harmonized Zone
  ├── Item-Day calculation components
  │    ├── production-10minute
  │    ├── outage-segments
  │    ├── curtailment-aggregated
  │    ├── alarm-segments
  │    └── adjustment-segments
  ├── immutable package manifests
  ├── canonical monthly event partitions
  └── Windfarm-Day reusable aggregates
       ├── daily-windfarm-wtgs-production
       └── daily-item-event-references
      ↓
ItemDayInputState  (revision authority + readiness gate)
      ↓
Calculation Package Coordinator
      ↓
Calculation Work Ledger
      ↓
Normal / Backfill Calculation SQS
      ↓
Calculation Pods
      ↓
Azure Blob Calculated Zone
      ↓
Synapse Serverless through query-provider abstraction
      ↓
Application API
      ↓
UI
```

Azure SQL control plane, in full:

```text
ExtractionCursor            cursor + lease + fenced commit
Adjustment / AdjustmentItem / AdjustmentAudit / Outbox
ItemDayInputState           revision authority + readiness + Failed/AttemptCount/LastError
FanOutFailure               written only on raw-fanout DLQ
EventMonthState             canonical event publication authority
EventMaterializationState   event lifecycle + materialization watermark, keyed (EventId, ItemId)
WindfarmDayProductionState / WindfarmDayCalculationState / SummaryWorkItem
WindfarmDayEventReferenceState
CalculationWorkItem / CalculatedPartition / RecalculationRun
WindfarmItemMembership + configuration versions
```

Nothing publishes a calculation from an input that is known to be stale: a failed or missing required Item-Day input blocks readiness, and an unenumerated Raw artifact is held open by `FanOutFailure`.

Pure user-requested recalculation bypasses Source, Raw, and Harmonization and reuses existing authoritative Harmonized package inputs.

`RefreshAndRecalculate` is different: it reloads the requested source range, writes new Raw artifacts, reconciles old materialized event footprints against the refreshed canonical event set, creates new Harmonized revisions where content changed, recalculates, and only then publishes a new coherent range snapshot. Historical Harmonized and Calculated revisions are retained.

SQS is the current queue provider only. Harmonizer, Calculation and Summary workers depend on a provider-neutral queue port; SQS visibility timeout, receipt handles, redrive/DLQ and IAM/IRSA stay inside the SQS infrastructure adapter.

---

# 23.1 Queue Abstraction and SQS Portability

AWS SQS is the current execution-delivery technology, but SQS-specific concepts must not become business semantics.

The governing architecture rule is:

```text
Queue = execution delivery
Blob  = data
SQL   = authoritative coordination/business state where correctness requires it
```

SQS may say:

> Somebody should attempt PackageRevision 19.

SQL decides whether that work is requested, running, completed, failed or superseded.

## 23.1.1 Provider-neutral queue contract

Business workers depend on these semantics:

```text
at-least-once delivery
processing lease / lock timeout
retry
completion
abandonment
lease extension
dead-letter / quarantine
observable backlog
independent scaling
```

They do not depend on the names of AWS APIs.

Current implementation mapping:

```text
Provider-neutral concept      AWS SQS implementation
--------------------------    ------------------------------
Receive work                  ReceiveMessage
Complete work                 DeleteMessage
Processing lease              Visibility Timeout
Extend processing lease       ChangeMessageVisibility
Retry                         message becomes visible again
Poison quarantine             DLQ / Redrive Policy
Authentication                IAM / IRSA
Operational metrics           CloudWatch + application telemetry
```

A future Azure Service Bus implementation could map the same business contract to message locks and `CompleteMessage`, while leaving Harmonizer/Calculation/Summary business logic unchanged.

## 23.1.2 Queue adapter boundary

Business code must not scatter direct `AmazonSQSClient` calls through Harmonizer, Calculation Worker or Summary Worker logic.

Recommended boundary:

```text
BUSINESS WORKER
────────────────────────
Harmonizer
Calculation Worker
Summary Worker

          ↓

QUEUE PORT
────────────────────────
Receive
Complete
Abandon
ExtendLease

          ↓

PROVIDER ADAPTER
────────────────────────
SqsWorkQueue<T>

          ↓

AWS SDK / SQS
```

Illustrative interface:

```csharp
public interface IWorkQueue<T>
{
    Task<IReadOnlyList<QueueMessage<T>>> ReceiveAsync(
        int maxMessages,
        CancellationToken cancellationToken);

    Task CompleteAsync(
        QueueMessage<T> message,
        CancellationToken cancellationToken);

    Task AbandonAsync(
        QueueMessage<T> message,
        CancellationToken cancellationToken);

    Task ExtendLeaseAsync(
        QueueMessage<T> message,
        TimeSpan duration,
        CancellationToken cancellationToken);
}
```

The abstraction protects business logic. It does **not** try to make every cloud-specific configuration option identical.

The following may remain provider configuration:

```text
SQS visibility timeout
SQS long polling
SQS redrive policy
IAM / IRSA
CloudWatch integration
```

## 23.1.3 Processing-lease semantics

Do not specify the business rule as:

> The worker requires SQS Visibility Timeout.

Specify:

> A work item has a renewable processing lease. If the consumer disappears, the work becomes eligible for retry.

Mappings:

```text
AWS SQS            → Visibility Timeout
Azure Service Bus  → Message Lock
SQL queue           → LeaseExpiresAt
```

This keeps crash-recovery semantics portable.

## 23.1.4 Dead-letter semantics

Do not define the business requirement as:

```text
Move to an SQS DLQ after N receives.
```

Define:

```text
repeated / poison failure
        ↓
quarantine the work
        ↓
retain diagnostics
        ↓
alert operators
        ↓
allow controlled replay
```

SQS currently implements this with a Redrive Policy + DLQ. Another broker may implement it differently.

## 23.1.5 Portability principle

> Cloud-managed services may be used, but provider-specific semantics remain behind infrastructure adapters. Business correctness must not depend on SQS, Synapse, or any other vendor service being the system of record.

This is intentionally not a "lowest common denominator" abstraction. The platform may use AWS-specific capabilities where useful; those capabilities stay below the adapter boundary.

---

# 24. Design Rationale and Considered Alternatives

## 24.1 What kind of problem this is

Choosing the right platform depends entirely on classifying the workload correctly. This is **not** primarily a data-integration problem, and it is **not** a big-data problem.

```text
Unit of work        one Item-Day  ≈ 144 intervals across 5 small inputs
Work item count     hundreds of thousands to millions
Parallelism shape   embarrassingly parallel, no cross-item shuffle
Hard requirements   idempotency, fenced concurrency, deterministic replay,
                    per-Item-Day failure isolation, reproducible history
Business logic      a versioned C# library encoding regulated loss rules
```

The difficulty is concentrated in **correctness under concurrency and replay**, not in volume per task. Every alternative below is evaluated against that shape rather than against raw data size.

---

## 24.2 Alternatives considered

| Alternative | Verdict | Primary reason |
|---|---|---|
| Lakehouse table format (Delta / Iceberg) adopted via the organisation's Spark-based platform | Deferred, not rejected | That platform would require either a calculation-engine rewrite or an additional integration layer; the Item-Day layer gains no query benefit because it is never scanned as a table |
| Lakehouse table format written directly from the existing .NET workers | Deferred, not rejected | Technically possible — a table format is a storage choice, not a compute choice — but adds writer, catalog and runtime dependencies beyond the current serving engine; see the two-layer boundary below |
| Azure Data Factory / Synapse Pipelines as orchestrator | Rejected | Orchestrates datasets on a schedule, not millions of independently versioned work units; cannot execute the calculation; provides no work-ledger or revision semantics |
| Synapse Spark pools for harmonization/calculation | Rejected | Engine-rewrite risk; job startup dominates a millisecond-scale unit of work |
| Synapse dedicated SQL pool / stored procedures | Rejected | Reproduces the bottleneck this redesign exists to remove (Section 18); calculation and serving cannot scale independently |
| Single monolithic integration-and-calculation application | Rejected | The current state; one failure domain, no independent scaling, no replay isolation |
| DuckDB embedded in API pods as the serving engine | Deferred | Pulls raw Parquet across the cloud boundary instead of pushing predicates down; see Section 24.3 |
| **Synapse Serverless as the serving query engine** | **Adopted** | Executes filtering and aggregation next to the data so only the reduced result crosses to EKS |
| **Plain Parquet + explicit revisions + SQL control plane** | **Adopted** | Matches the workload shape; keeps the validated C# engine; concurrency control shards naturally per Item-Day |

### The two-layer boundary

This distinction is the core of the position and should be read before the detail below. The platform has two storage layers with **opposite** characteristics, and they warrant opposite decisions:

```text
Item-Day calculation-input layer      Published analytical / serving layer
-------------------------------      -----------------------------------
very high write concurrency           low write concurrency (one publisher
(hundreds of concurrent workers)      per windfarm-day)

addressed by explicit manifest path    actually queried, by range and by BI
never wildcard-scanned
never queried as a table               day grain, small row counts

behaves as an IMMUTABLE ARTIFACT       behaves as an ANALYTICAL TABLE
STORE                                  SURFACE

→ plain Parquet + manifest is the      → catalog, schema evolution, compaction
  correct fit                            and time travel would add real value
```

### On the lakehouse question specifically

The mechanisms in Sections 6.4 and 8.4 — immutable revisions, fenced leases, token-scoped attempts, content hashing, manifest-pinned inputs — are recognisably optimistic concurrency control, snapshot isolation and time travel. A table format provides these as a product, and that is a legitimate argument.

It is important to separate two things that are frequently conflated: **Delta and Iceberg are table/storage formats; Spark is a compute engine.** Adopting a table format does not by itself require abandoning the C# calculation engine. The costs below are therefore stated as they actually apply, not as a blanket objection:

```text
1. If adoption goes through the organisation's existing Spark-based platform, the
   calculation engine must be rewritten. That places correctness risk on the one
   component whose outputs are reported externally. Writing a table format directly
   from .NET avoids this but introduces its own writer/catalog dependencies.
2. Section 8.8 targets hundreds of independently completing Item-Day writers.
   Before adopting a table format for that layer, its transaction-log and catalog
   commit behaviour must be BENCHMARKED under the actual concurrency profile:
   a centralized commit path may become a serialization or metadata-contention
   point. This is a measurable design criterion, not an assumed defect.
3. The Item-Day layer is addressed only by explicit manifest path and is never
   wildcard-scanned or queried by BI, so a table format adds commit and metadata
   overhead there with no corresponding query benefit.
4. The current serving engine constrains the choice. Synapse Serverless queries
   Parquet natively and has limited Delta Lake READ support — reader version 1,
   no time-travel queries, and CETAS cannot write Delta. Iceberg is not a native
   Synapse Serverless query format. Adopting a table format today would therefore
   require additional writer, catalog and runtime components beyond the current
   serving engine, without unlocking the modern lakehouse feature set.
```

Points 1 and 4 are consequences of the current platform and deployment topology rather than of the formats themselves — see the revisit gates in Section 24.5.

> **Position.** Lakehouse technology is not rejected. Table semantics are applied deliberately only where the workload *consumes* table semantics. The Item-Day calculation layer behaves as an immutable artifact store; the serving layer behaves as an analytical table surface. If the organisation's platform or cloud topology changes, the serving layer is the first place to adopt Delta or Iceberg, and the calculation contract remains unchanged either way.

---

## 24.3 Deployment topology is a first-class constraint

```text
Calculation / harmonization compute   AWS EKS
Raw Zone                              Azure Blob (container operated by another team,
                                      file-lifecycle orchestration switched off)
Analytical storage                    Azure Blob
Company direction                     AWS
```

The analytical lake remains in Azure with no migration date, so every serving query crosses a cloud boundary for the foreseeable future. Three design decisions follow directly and should not be re-litigated without revisiting this constraint:

```text
1. Synapse Serverless is selected BECAUSE compute and storage are in different
   clouds. Pushdown at the storage side is what makes the seam affordable:
   a reduced result is kilobytes, the underlying Parquet is not.
2. The query engine reads only day-grain aggregates (Section 6.4.1). This is a
   bandwidth control as much as a small-file control.
3. Drill-down is a bounded point read (Section 12.6), never a scan.
```

Cross-cloud egress volume and round-trip latency are therefore tracked as first-class metrics with an agreed budget (Section 19), not as incidental infrastructure noise.

---

## 24.4 Accepted trade-offs

These are known, bounded, and mitigated. They are recorded so that reviewers can see they were chosen rather than overlooked.

| Trade-off | Why it is acceptable | Mitigation / bound |
|---|---|---|
| Revision and concurrency mechanics are implemented in-house rather than obtained from a table format | The surface is small and narrowly scoped: immutable directories plus authoritative SQL state | Azure SQL is the single authority; Blob layouts are derived and rebuildable; behaviour is covered by the Section 10 invariants and property-based tests |
| Observability is bespoke rather than supplied by a low-code platform | The monitoring that matters (source vs data availability, pipeline-stage freshness, cursor stall) is domain-specific and would have been custom regardless | Standard OpenTelemetry stack; Section 14.7 defines the model explicitly |
| Cross-cloud egress and latency on every read | Bounded by pushdown and day-grain serving; disappears entirely if the lake migrates | Measured against a budget; result caching keyed by `CommittedPackageRevision` |
| Fan-out read amplification (one Raw file read by several batch workers) | Batching already reduced it by ~50× versus per-Item-Day messages | Section 15 measures read amplification; row-group pushdown or coordinator sharding applied if it stays high |
| Large small-file count in the Harmonized Item-Day layer | Never wildcard-scanned; read only by explicit manifest path | Logical-empty is the default for zero-row components (`RecordCount = 0`, `Path = NULL`), avoiding millions of empty Parquet objects; retain/GC unreferenced non-empty revisions; serving never touches this layer |
| The SQL control plane arbitrates all concurrency | Rows are small and shard naturally by Item-Day key | Active tables retain only outstanding work; completed rows purged or archived; write throughput is a measured capacity limit |
| Larger operational surface (more queues, more worker roles) | Each stage fails and scales independently, which is the point | Uniform patterns across stages: lease, fencing, DLQ, reconciliation |
| No durable per-Item-Day record of fan-out attempt history | The fan-out ledger existed largely to drive an external Raw-file lifecycle that no longer exists; completeness is already covered by the Windfarm-Day barrier and readiness by `ItemDayInputState` | Remediation for an undeterminable batch is replaying the parent Raw artifact — ~30 batches, all no-ops except the missing Item-Days (Section 5.7) |
| The pre-fan-out window has no Item-Day identity to mark failed | It is genuinely unknowable before enumeration | `FanOutFailure` written on `raw-fanout-dlq` gives a durable handle that outlives DLQ retention (Section 5.9) |
| Raw retains one artifact per extraction attempt, not per business day | Raw is audit and replay storage; overwriting would destroy extraction history | Bounded by retention policy; abandoned attempts from lost fenced commits are garbage-collected |

---

## 24.5 Revisit gates

Each deferred decision has an explicit trigger. The architecture is deliberately reversible at these points, and the manifest indirection means the physical layer can change without altering the calculation contract.

```text
Analytical lake migrates to S3, or the serving engine changes
        ↓
evaluate Iceberg/Delta + catalog for the published serving datasets FIRST;
retain plain Parquet for the Item-Day calculation-input layer unless a
benchmark demonstrates a concrete operational or correctness advantage there

A table format is proposed for the Item-Day layer
        ↓
benchmark its transaction-log / catalog commit behaviour under the actual
concurrency profile (Section 8.8) before adopting

Serving latency or scan cost exceeds the agreed budget
        ↓
materialize a Gold dataset for the specific query (Section 12.7)

Measured read amplification stays materially above 1×
        ↓
sort Raw by ItemId for row-group pushdown, or shard in the Fan-out Coordinator

Control-plane write throughput becomes the binding constraint
        ↓
partition or shard the ledgers by windfarm or date range

Small-file count causes listing or GC pressure
        ↓
compact Harmonized component files behind the manifest, transparently

Per-Item-Day fan-out attempt history is repeatedly needed during incidents,
or parent-artifact replay proves too coarse in practice
        ↓
reinstate a durable child ledger — but scope it to the failure path only,
as FanOutFailure already is, rather than writing 1,500 rows per healthy Raw file

Company AWS migration reaches this solution's control plane
        ↓
decide the target SQL dialect explicitly; current mechanisms are T-SQL
(TVP bulk upsert, @@ROWCOUNT-guarded counters, MERGE) and port cleanly to
RDS SQL Server, or require documented rewrites for Aurora PostgreSQL
```

None of these gates require redesigning the processing architecture. That is the central property this design was optimised for: **the expensive parts — the deterministic engine, the revision model, the failure semantics — are independent of the storage format, the query engine, and the cloud.**

---

# 25. Architecture Decision Status

This document is the current baseline target architecture.

The following decisions are intentional:

- Standard Parquet files are used; no Delta Lake or Iceberg in the initial implementation
- Empty required Item-Day components default to logical-empty authoritative revisions (`Status = Complete`, `RecordCount = 0`, `Path = NULL`); zero-row Parquet is optional, never required for steady-state emptiness
- `CanonicalContentHash` is computed using the same deterministic canonical-row algorithm for empty and non-empty components; no sentinel hash is used
- Package manifests carry immutable component facts (`revision`, `recordCount`, `canonicalContentHash`, `path`) rather than being only file-pointer collections
- Event correction reconciles actual per-`(EventId, ItemId)` materialization state against the new canonical footprint, including overlapping and completely shifted event revisions
- `RefreshAndRecalculate` reloads source data and reconciles historical event snapshots; `ForceRecalculate` reuses existing Harmonized inputs
- AWS SQS is behind a provider-neutral queue adapter and is never authoritative business state
- Every revision is a complete replacement of its logical partition, not a delta
- Synapse Serverless SQL is the initial query engine
- The Application API uses a provider abstraction so DuckDB or AWS Athena can replace Synapse later
- Provider-specific SQL stays inside provider implementations, not controllers
- Azure SQL stores configuration, adjustments, adjustment audit/outbox, work-ledger and other small transactional/control state
- Large production, Harmonized and Calculated datasets remain in object storage
- The Platform Orchestration API is switched off for this solution's Raw container; no external service assigns a `New → Running → Completed` lifecycle to a Raw file, and nothing outside this solution waits on Raw-file completion
- `RawDispatch`, `RawFanOut`, `RawFanOutWorkItem` and the Harmonization Dispatcher are removed; the extraction pod writes Raw and publishes to `raw-fanout` itself, and the Fan-out Coordinator emits Item-Day batches directly
- The extraction cursor is the durability boundary for the ingestion handoff: it advances only after both the Raw write and the SQS send succeed, and never before
- `ExtractionCursor` carries the extraction lease, and the cursor commit is **fenced on `LeaseToken`**; a zombie whose lease expired updates zero rows and must discard its run
- Raw artifacts are addressed by `Windfarm + Date + extraction=<LeaseToken>`; the business date is the logical identity, the lease token is the physical extraction identity, and Raw is never overwritten
- Event Raw batches have no stable business identity by design; idempotency comes from keyed merge on `EventId` plus `CanonicalContentHash`, never from Raw path identity
- `ItemDayInputState` is both the component revision authority and the Item-Day readiness state, carrying `Status`, `AttemptCount` and `LastError`; there is exactly one place that answers whether an Item-Day is healthy
- A failed Item-Day retains its last good `ComponentRevision` but sets `Status = Failed`; readiness requires `Status = Complete`, so a known-stale input can never be silently calculated
- A missing required-input row is treated as not-ready; absence is never readiness, exactly as file absence is never "zero events"
- Failure is classified as item / batch-transient / poison-batch; item failures are isolated and acked, so a DLQ batch always indicates a batch-wide infrastructure cause rather than one bad row
- A Harmonization Reconciliation sweep retries `Failed` Item-Days under backoff until `MaxItemAttempts`, because acking a partially failed batch would otherwise orphan the failed item
- `maxReceiveCount` is paired with a visibility timeout exceeding p99 batch duration, so a healthy but slow batch is never dead-lettered
- `FanOutFailure` is written only when a Raw artifact reaches `raw-fanout-dlq`, giving the pre-fan-out window a durable recovery handle that outlives SQS DLQ retention
- The Adjustment `Outbox` is retained unconditionally, because Azure SQL is itself the source of truth for adjustments and a lost lake push would be silent and unrecoverable
- User adjustments are authored in SQL, published to Raw through the outbox, then processed through the normal fan-out and Harmonization path
- Harmonized Item-Day component revisions are complete snapshots; immutable package manifests reference their exact paths for Calculation SQS messages
- Production extraction uses windfarm-level `(LastUpdatedDate, Id)` change discovery with bounded catch-up windows and affected-date Windfarm-Day read-back where supported
- Every extraction cycle re-reads a configurable safety-lag window below the committed cursor, because a source row stamped at transaction start can become visible after the cursor has passed it
- Raw artifact grain, Item-Day correctness grain, and Harmonization SQS batch grain are intentionally different
- The Item-Day batch is only an execution envelope; the correctness boundary is always `Item + Date + InputType`
- User adjustments are authored in SQL, published to Raw, then processed through fan-out and the normal Harmonization path
- Harmonized Item-Day component revisions are complete snapshots; immutable package manifests reference their exact paths for Calculation SQS messages
- Raw event ingestion is append-only extraction batches carrying the full current state of each changed event plus explicit tombstones; Raw event files are never partitioned by event start month and never rewritten
- The monthly canonical event partition has an explicit SQL authority, `EventMonthState`, with a fenced lease and token-scoped attempts, identical in pattern to Item-Day component publication
- The `CanonicalContentHash` no-op rule applies to `EventMonthState` as mandatory behaviour, because the safety-lag re-read would otherwise advance the current month's revision on every cycle and churn every dependent serving projection
- Daily event-reference invalidation is triggered by the contributing Item-Day event component revisions, never by the monthly partition revision; the `EventId + EventRevision` contributor list is lineage, not a second invalidation mechanism
- Affected Item-Days for an event change are `(old items × old materialized dates) ∪ (new items × new authoritative dates)`; a date-only union misses a de-selected adjustment item
- Open events generate daily references indefinitely by design; `OpenEventAgeThreshold` raises an operational alert per `EventType` but never auto-closes an event or halts reference generation
- Open events are materialized into Item-Day components by a clock-driven continuation sweep, because source-driven fan-out stops when the source stops changing and would silently leave later days with no event and therefore an understated loss
- `EventMaterializationState` is keyed `(EventId, ItemId)` and is Azure SQL, not Parquet: it is small, mutable, frequently updated coordination state and is rebuildable from the canonical event store
- `MaterializedThroughDate` is a contiguous committed watermark advanced only after the fenced `ItemDayInputState` commit; it may lag, and must never lead, because a leading watermark turns a crash into a permanently skipped day
- Calculation materializes only UTC days up to `CalculationProcessableDate`; the current UTC day is served in the event-reference projection from `EventMaterializationState` but is never calculated, so open events do not churn a package revision every sweep
- A component emptied by a deletion, shrink or membership removal is republished with zero rows and `Status = Complete`; the file is never deleted, because absence is indistinguishable from unprocessed
- An adjustment is one logical event with N item children; `AdjustmentItem` is child applicability, never its own `EventId`, and the canonical month partition publishes `adjustments.parquet` and `adjustment-items.parquet` together under one revision
- The Raw adjustment payload carries the complete parent plus the complete current item membership, so a de-selected turbine is detectable by diff; an adjustment edit invalidates `union(old range, new range)` × `union(old membership, new membership)`
- Harmonized Windfarm-Day folders are reserved for reusable aggregates and can be extended later
- Windfarm-Day completion is modelled as two independent barriers — `WindfarmDayProductionState` for harmonized production and `WindfarmDayCalculationState` for published calculations — never as one shared counter
- `ExpectedItemCount` is resolved from effective-dated `WindfarmItemMembership` and the resolving `MembershipVersion` is stored on the state row, so historical rebuilds are deterministic
- The Item-Day worker that trips a barrier records a `SummaryWorkItem` in the same transaction and never generates the summary inline
- One `SummaryWorkItem` table, discriminated by `SummaryType` — `HarmonizedProduction`, `HarmonizedEventReference`, `CalculatedLoss` — serves all three projections so the execution, lease, retry and DLQ pattern stays uniform
- The event-reference projection is invalidation-driven rather than barrier-driven, because absence of an event is legitimate and a count barrier would never trip; it still records `IsPartial`, since a missing row otherwise reads as "no event"
- Every Windfarm-Day serving revision is a complete snapshot rebuilt from current authoritative inputs, never a delta and never a copy-forward of the previous serving revision
- The barrier counter is a first-readiness signal only; a correction after publication increments `RequestedSummaryRevision` and republishes rather than "uncompleting" the day
- Adjacent compatible curtailments are aggregated in Harmonized
- One daily Windfarm-Day production aggregate file contains one row per WTG per calendar-day fragment
- Daily aggregates in Harmonized and Calculated are keyed by `UtcDate + LocalDate`; local reporting is a grouping key, never a parallel sum column
- Rows are never duplicated across daily files; a local-calendar range widens the UTC partition predicate by one day instead, and the trailing local day is labelled partial until the next UTC partition publishes
- The reporting timezone is per windfarm and effective-dated, and the pinned `tzdbVersion` is part of package identity
- A separate daily windfarm-total file is not required initially; Synapse aggregates WTG rows on demand
- Production aggregates are not duplicated into Calculated or Gold
- Calculated contains only calculation-derived detail and summaries
- The query engine joins Harmonized production with Calculated loss where needed
- A physical Gold Zone is optional and created only for proven serving-performance requirements
- Normal and historical/backfill calculation capacity remain isolated
- RequestedPackageRevision / RunningPackageRevision plus published CalculatedPartition state protect against stale concurrent calculation work
