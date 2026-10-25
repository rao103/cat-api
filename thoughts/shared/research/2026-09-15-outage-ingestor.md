---
date: 2026-09-15
researcher: GitHub Copilot
git_commit: addaa2e645475f92c2b01c03ca91f013d288686c
branch: Ingestor
repository: cat-dl-backend-service
topic: "Building an Outage Ingestor following the Production Ingestor setup"
tags: [research, codebase, outage-ingestor, production-ingestor, raw-zone, extraction]
status: complete
last_updated: 2026-09-15
last_updated_by: GitHub Copilot
---

## Research Question

Using the existing `ProductionIngestor` implementation and the architecture explained in the
`explainationv2.html` deck (https://cat-dev.de-prod.dk/cat3redesign/explainationv2.html#s04),
document what exists today for the Production Ingestor and what the deck specifies for an
`OutageIngestor`, so a subsequent planning phase can scaffold it "following the production
ingester setup."

## Summary

The repository currently contains **one** extraction pod, `ProductionIngestor` (csproj name
`O.WP.Raw.ProductionIngestor`), plus its `ProductionIngestor.Contracts` message-schema project
and an as-yet-thin `ProductionIngestor.Api`. There is **no** Outage, Alarm, or Curtailment
ingestor code anywhere in the repo — only prose references in the architecture doc and the
slide deck describing them as siblings of `ProductionIngestor`.

The Production Ingestor's own plan document explicitly scopes outage/alarm/curtailment/adjustment
ingestors as **out of scope** ([thoughts/shared/plans/2026-09-14-production-ingester.md](thoughts/shared/plans/2026-09-14-production-ingester.md#L64)).

The slide deck (`explainationv2.html`) describes an `OutageIngestor` as one of several
per-source-type EKS ingestor deployments that share the exact same `ExtractionCursor` lease
mechanism as `ProductionIngestor`, but write a fundamentally different Raw shape: an
**append-only event batch** (`/raw/outage/windfarm=<w>/extraction=<token>/outage.parquet`)
instead of a **Windfarm-Day snapshot** (`/raw/production/windfarm=<w>/date=<d>/extraction=<token>/production.parquet`).
The architecture doc's §4.3.1 and §4.8–4.9 formalize this distinction and are the authoritative
source for the differences the code must encode.

## Detailed Findings

### 1. Current Production Ingestor structure (what exists to model on)

Project: [src/ProductionIngestor/O.WP.Raw.ProductionIngestor.csproj](src/ProductionIngestor/O.WP.Raw.ProductionIngestor.csproj) — `Microsoft.NET.Sdk.Worker`, `net8.0`, references `ProductionIngestor.Contracts`, uses `AWSSDK.SQS`, `Azure.Storage.Blobs`, `Npgsql`, `Parquet.Net`, `Microsoft.Extensions.Hosting`.

Folder responsibilities (all under `src/ProductionIngestor/`):

| Folder | Files | Responsibility |
|---|---|---|
| `Execution/` | [ProductionIngestionRunner.cs](src/ProductionIngestor/Execution/ProductionIngestionRunner.cs), [IngestionRequest.cs](src/ProductionIngestor/Execution/IngestionRequest.cs), [IngestionMode.cs](src/ProductionIngestor/Execution/IngestionMode.cs), [RunResult.cs](src/ProductionIngestor/Execution/RunResult.cs) | Single ordering rule: lease → discovery → extraction → Raw write → publish → fenced commit. One bounded pass, no poll loop. |
| `Cursor/` | [ExtractionLease.cs](src/ProductionIngestor/Cursor/ExtractionLease.cs), [PostgresExtractionCursorRepository.cs](src/ProductionIngestor/Cursor/PostgresExtractionCursorRepository.cs), [IExtractionCursorRepository.cs](src/ProductionIngestor/Interfaces/IExtractionCursorRepository.cs) | Fenced lease acquire/renew/commit/release against Postgres `extraction_cursor` table, keyed `(WindfarmId, SourceType)`. |
| `Discovery/` | [CatchUpWindowPlanner.cs](src/ProductionIngestor/Discovery/CatchUpWindowPlanner.cs), [ChangeDiscoveryReader.cs](src/ProductionIngestor/Discovery/ChangeDiscoveryReader.cs) | Splits `[effectiveLow, high)` into bounded half-open windows; keyset-pages changed rows per window and collects distinct affected dates. |
| `Extraction/` | [ProductionRow.cs](src/ProductionIngestor/Extraction/ProductionRow.cs), [WindfarmDayExtractor.cs](src/ProductionIngestor/Extraction/WindfarmDayExtractor.cs), [IProductionSourceAdapter.cs](src/ProductionIngestor/Interfaces/IProductionSourceAdapter.cs) | Source abstraction + complete Windfarm-Day read-back with dedup-by-natural-key. |
| `Raw/` | [ProductionParquetWriter.cs](src/ProductionIngestor/Raw/ProductionParquetWriter.cs), [RawPathBuilder.cs](src/ProductionIngestor/Raw/RawPathBuilder.cs), [RawWriteResult.cs](src/ProductionIngestor/Raw/RawWriteResult.cs), [AzureBlobRawObjectStore.cs](src/ProductionIngestor/Raw/AzureBlobRawObjectStore.cs), [IRawObjectStore.cs](src/ProductionIngestor/Interfaces/IRawObjectStore.cs) | Validates/quarantines rows, shards deterministically by size limit, uploads, verifies durability. |
| `Messaging/` | [SqsRawArtifactPublisher.cs](src/ProductionIngestor/Messaging/SqsRawArtifactPublisher.cs), [IRawArtifactPublisher.cs](src/ProductionIngestor/Interfaces/IRawArtifactPublisher.cs) | Publishes `RawArtifactAvailableV1` to SQS with bounded in-flight sends (semaphore). |
| `Refresh/` | [ProductionRefreshPlanner.cs](src/ProductionIngestor/Refresh/ProductionRefreshPlanner.cs), [RefreshRequest.cs](src/ProductionIngestor/Refresh/RefreshRequest.cs), [IRefreshRequestSource.cs](src/ProductionIngestor/Interfaces/IRefreshRequestSource.cs) | Planning-only: turns an explicit windfarm+date-range request into an `IngestionRequest` in `Refresh` mode. |
| `Monitoring/` | [IngestionTelemetry.cs](src/ProductionIngestor/Monitoring/IngestionTelemetry.cs), [IIngestionTelemetry.cs](src/ProductionIngestor/Interfaces/IIngestionTelemetry.cs) | Extraction-time observations only (no-op `NullIngestionTelemetry` today). |
| `Configuration/` | [IngestionOptions.cs](src/ProductionIngestor/Configuration/IngestionOptions.cs), [SourceOptions.cs](src/ProductionIngestor/Configuration/SourceOptions.cs) | Bound options: safety lag, window size, lease duration, shard size, concurrency caps, connection strings. |
| — | [Program.cs](src/ProductionIngestor/Program.cs) | DI wiring only; builds `Host`, registers all services, returns `0` without starting a poll loop. `IProductionSourceAdapter` itself is **not yet registered** — a real adapter is deferred (see [Program.cs](src/ProductionIngestor/Program.cs#L67-L69) comment). |

Contracts project: [src/ProductionIngestor.Contracts/RawArtifactAvailableV1.cs](src/ProductionIngestor.Contracts/RawArtifactAvailableV1.cs) — the external `raw-fanout` message schema: `SourceType`, `WindfarmId`, `DataDate`, `ExtractionToken`, `BlobPath`/`ShardPaths`, `RunId`, `TraceId`. `SourceType` is already a free-form string, not a Production-only enum — the message contract is already source-agnostic.

Solution wiring: [ProductionIngestor.sln](ProductionIngestor.sln) registers `O.WP.Raw.ProductionIngestor`, `ProductionIngestor.Contracts`, `ProductionIngestor.Api`, `ProductionIngestor.UnitTests`, `ProductionIngestor.IntegrationTests`. The two test projects currently contain **only** a `.csproj` — no test files exist yet ([tests/ProductionIngestor.UnitTests](tests/ProductionIngestor.UnitTests), [tests/ProductionIngestor.IntegrationTests](tests/ProductionIngestor.IntegrationTests)).

### 2. Execution flow specifics worth reusing exactly

- `ProductionIngestionRunner.RunAsync` ([Execution/ProductionIngestionRunner.cs](src/ProductionIngestor/Execution/ProductionIngestionRunner.cs#L54-L74)): acquires lease, dispatches to `RunIncrementalAsync` or `RunRefreshAsync` based on `IngestionRequest.Mode`, and always ends with an explicit `ReleaseLeaseAsync` or an early `RunOutcome.Zombie`/`LeaseHeldByOther`/`Failed` return — no implicit retry inside one pass.
- `RunIncrementalAsync` ([Execution/ProductionIngestionRunner.cs](src/ProductionIngestor/Execution/ProductionIngestionRunner.cs#L76-L150)): stops at the **first failing day** in a window and does not advance the cursor past it; a no-change window still advances the verified horizon (`TryCommitAsync` with `window.End`).
- The cursor fencing pattern ([Cursor/PostgresExtractionCursorRepository.cs](src/ProductionIngestor/Cursor/PostgresExtractionCursorRepository.cs#L46-L83)) uses `RETURNING` on an `UPDATE ... WHERE lease_token = @leaseToken` — this is source-type-agnostic already since `extraction_cursor` is keyed by `(windfarm_id, source_type)`, not by a Production-specific column.
- `ProductionParquetWriter.Validate`/`DetermineShards` ([Raw/ProductionParquetWriter.cs](src/ProductionIngestor/Raw/ProductionParquetWriter.cs#L92-L146)) implement dedup-contradiction quarantine and deterministic ItemId+timestamp shard ordering — this logic is Production-row-shaped (`ProductionRow`) and would need an event-row-shaped equivalent, not a reuse, for Outage.
- Serialization is currently JSON Lines, not real Parquet, called out explicitly as an interim format ([Raw/ProductionParquetWriter.cs](src/ProductionIngestor/Raw/ProductionParquetWriter.cs#L152-L161) comment) despite the `Parquet.Net` package reference already being present in the csproj.

### 3. What the architecture doc specifies for Event Raw (Outage falls under this)

- §4.3 ([Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md](Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md#L418-L438)): Production Raw grain is Windfarm-Day; **event sources (outage, alarm, curtailment, adjustment) are explicitly not forced into that shape** and instead use the grain defined in §4.3.1.
- §4.3.1 ([Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md](Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md#L442-L519)):
  - One extraction cycle = one immutable, **append-only** Raw batch per source+windfarm (never rewritten, not partitioned by event start month).
  - Payload carries the **full current state of each changed event**, not a delta: `EventId`, `WindfarmId`/`ItemId`, `EventType`, `OriginalStartUtc`, `OriginalEndUtc` (NULL while open), `Status` (Open|Closed|Cancelled), `SourceRevision`, `SourceWatermarkUtc`, `IsTombstone`, plus type-specific attributes.
  - Deletion is an explicit tombstone row, never silent omission.
  - The extraction lease still applies (prevents two pods pulling the same source concurrently) but there is **no Raw-side revision counter or read-modify-write** — content contention is deferred entirely to Harmonization.
  - Unlike Production, an event batch has **no stable business identity** (the changed-event set differs every cycle); idempotency comes from downstream keyed merge on `EventId` + `CanonicalContentHash`, not from Raw path identity.
- §4.8 ([Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md](Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md#L820-L833)): a long-running outage is stored once, in the batch that carried its latest state — not duplicated per day; daily segmentation is a Harmonizer concern, not an extraction concern.
- §4.9 ([Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md](Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md#L834-L862)): Raw path identity pattern generalizes as `Windfarm + LeaseToken` for events (no `date=` segment, since there is no Windfarm-Day grain), vs. `Windfarm + ProductionDate + LeaseToken` for Production.

### 4. What the slide deck (`explainationv2.html`) adds beyond the architecture doc

- Slide "Component responsibility map": table row —
  `Outage/Alarm/Curtailment Ingestor | Source API / DB | Raw event-batch Parquet + raw-fanout message | ExtractionCursor | Windfarm + SourceType` — i.e. same control-plane state table and same scaling axis as Production.
- Slide "Full end-to-end architecture diagram": names the EKS ingestion pods explicitly — `☸️ProductionIngestor ☸️OutageIngestor ☸️AlarmIngestor ☸️CurtailmentIngestor 📬AdjustmentOutbox Processor` — each writing to its own Raw subfolder (`/raw/outage/`, `/raw/alarm/`, etc.) and each publishing to the shared `raw-fanout` SQS queue.
- Slide "How ingestors pull from sources": example deployment replica counts — `outage-ingestor replicas: 0..N`, same 8-step source read pattern as Production (acquire lease → read cursor → query using EffectiveQueryLow → freeze HighWatermark → page by LastUpdatedDate,Id → write Raw → send raw-fanout → advance cursor fenced on LeaseToken). Confirms one `ExtractionCursor` row per `(WindfarmId, SourceType)` pair, so Outage gets its own row with `SourceType = 'Outage'`.
- Slide "Event Raw: outage, alarm, curtailment and adjustment batches": concrete example path `/raw/outage/windfarm=WF01/extraction=88/outage.parquet` and example rows — current-state rows for `O1001`/`O1007`, plus a tombstone row for `O1021` (`IsTombstone=true`). Confirms columns: `EventId | WindfarmId | ItemId | EventType | OriginalStartUtc | OriginalEndUtc | Status | SourceRevision | SourceWatermarkUtc | IsTombstone`.
- Slide "Raw write and queue publication ordering": same correct/unsafe ordering and failure-mode table applies uniformly to all ingestors (Raw write → verify → SQS send → fenced cursor advance → release lease).
- Later "Open outage" slides (continuation, materialization) describe **downstream** (Event Lifecycle Coordinator / Harmonizer) handling of an outage that stays open across days with no new source row — this is explicitly **not** an extraction-pod concern; the deck states "Open outage is not solved by Production Raw... explained later in the Event Lifecycle section because it is a clock-driven event materialization problem, not a Raw production problem," and the equivalent statement applies to Outage Raw: the ingestor's only job is to emit the current full-state batch, not to synthesize continuation days.
- The deck's zone diagrams show `raw-fanout` SQS and `IWorkQueue<T>` as a provider-neutral adapter — same abstraction boundary implied by the existing `IRawArtifactPublisher` interface.

### 5. Gaps / differences an Outage Ingestor must encode versus reusing Production code as-is

| Aspect | Production (existing) | Outage (per architecture doc + deck) |
|---|---|---|
| Raw grain | Windfarm-Day snapshot, full re-read of the day | Append-only batch of **changed events since last cursor position**, no day re-read |
| Raw path | `/raw/production/windfarm=<w>/date=<d>/extraction=<token>/production.parquet` | `/raw/outage/windfarm=<w>/extraction=<token>/outage.parquet` (no `date=` segment) |
| Row shape | `ProductionRow` (10-minute measurement) | Event row: `EventId, ItemId, EventType, OriginalStartUtc, OriginalEndUtc, Status, SourceRevision, SourceWatermarkUtc, IsTombstone` |
| Dedup/merge key | `(ItemId, ProductionTimestampUtc)`, keep highest `Id` | `EventId` is the stable identity; full current state replaces any prior state for that `EventId` — no per-field merge at extraction time |
| Idempotency source | Raw path identity (`Windfarm+Date+Token`) is itself stable/deterministic | Raw path is **not** a stable identity per batch; idempotency deferred to Harmonizer's `EventId` + `CanonicalContentHash` keyed merge |
| Discovery output | Distinct **affected UTC dates** (`DiscoveryResult.AffectedDates`) drive a second read-back pass | Changed **events themselves** (already carrying full current state) are the payload — no second "read back the day" pass needed |
| Deletion handling | N/A (production rows aren't deleted) | Explicit tombstone (`IsTombstone=true`) rows required |
| Extraction cursor | `(WindfarmId, SourceType='Production')` | Same table, `(WindfarmId, SourceType='Outage')` — reuse `IExtractionCursorRepository`/`PostgresExtractionCursorRepository` unchanged |
| Shard/quarantine logic | `ProductionParquetWriter.Validate`/`DetermineShards` operate on `ProductionRow` | Needs an analogous but event-row-typed writer; ordering key would plausibly be `EventId` (or `ItemId`+`OriginalStartUtc`) rather than `(ItemId, timestamp)` |
| Refresh semantics | `ProductionRefreshPlanner` takes explicit `(Windfarm, dates[])` and re-reads full days | An Outage refresh would need to re-pull the source's authoritative event set for a windfarm/date range and reconcile against previously materialized events (§15.2.1/§15.2.2, "How a one-year source refresh finds removed events" slide) — materially different logic, not a drop-in reuse of `ProductionRefreshPlanner` |

## Code References

- [src/ProductionIngestor/Program.cs](src/ProductionIngestor/Program.cs) — DI wiring and bounded-run-then-exit host setup
- [src/ProductionIngestor/Execution/ProductionIngestionRunner.cs](src/ProductionIngestor/Execution/ProductionIngestionRunner.cs) — lease→discovery→write→publish→commit ordering
- [src/ProductionIngestor/Cursor/PostgresExtractionCursorRepository.cs](src/ProductionIngestor/Cursor/PostgresExtractionCursorRepository.cs) — fenced Postgres cursor repository (source-type-agnostic)
- [src/ProductionIngestor/Interfaces/IExtractionCursorRepository.cs](src/ProductionIngestor/Interfaces/IExtractionCursorRepository.cs) — cursor repository contract
- [src/ProductionIngestor/Discovery/CatchUpWindowPlanner.cs](src/ProductionIngestor/Discovery/CatchUpWindowPlanner.cs) — half-open window planning
- [src/ProductionIngestor/Discovery/ChangeDiscoveryReader.cs](src/ProductionIngestor/Discovery/ChangeDiscoveryReader.cs) — keyset-paginated change discovery
- [src/ProductionIngestor/Extraction/ProductionRow.cs](src/ProductionIngestor/Extraction/ProductionRow.cs) — Production-specific row shape (would need an Outage equivalent)
- [src/ProductionIngestor/Interfaces/IProductionSourceAdapter.cs](src/ProductionIngestor/Interfaces/IProductionSourceAdapter.cs) — source abstraction (Production-shaped; not yet implemented for a real source)
- [src/ProductionIngestor/Raw/ProductionParquetWriter.cs](src/ProductionIngestor/Raw/ProductionParquetWriter.cs) — validation, sharding, upload, durability verification
- [src/ProductionIngestor/Raw/RawPathBuilder.cs](src/ProductionIngestor/Raw/RawPathBuilder.cs) — deterministic path construction
- [src/ProductionIngestor/Messaging/SqsRawArtifactPublisher.cs](src/ProductionIngestor/Messaging/SqsRawArtifactPublisher.cs) — bounded-in-flight SQS publish
- [src/ProductionIngestor.Contracts/RawArtifactAvailableV1.cs](src/ProductionIngestor.Contracts/RawArtifactAvailableV1.cs) — shared, already source-agnostic `raw-fanout` message schema
- [src/ProductionIngestor/Configuration/IngestionOptions.cs](src/ProductionIngestor/Configuration/IngestionOptions.cs) / [SourceOptions.cs](src/ProductionIngestor/Configuration/SourceOptions.cs) — bound options pattern
- [ProductionIngestor.sln](ProductionIngestor.sln) — current project registrations
- [thoughts/shared/plans/2026-09-14-production-ingester.md](thoughts/shared/plans/2026-09-14-production-ingester.md) — Production Ingestor's own plan; explicitly excludes Outage (line 64)
- [01-production-ingester.md](01-production-ingester.md) — Production-specific execution-sequence ticket (source contract for that ingester only)
- [Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md](Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md) §4.3, §4.3.1, §4.8, §4.9, §6.10 — authoritative Event Raw / event-serving model spec

## Architecture Insights

- The extraction-cursor/lease mechanism (`ExtractionCursor` table, `IExtractionCursorRepository`, `PostgresExtractionCursorRepository`) is already generic across `SourceType` — it can very likely be reused unmodified (perhaps moved to a shared library) rather than re-implemented per ingestor, since it's keyed by `(WindfarmId, SourceType)` and has no Production-specific columns.
- The `raw-fanout` message contract (`RawArtifactAvailableV1`) is already source-agnostic (`SourceType` is a plain string; `BlobPath`/`ShardPaths` don't assume a `date=` partition), so the same contracts project/message type can likely be shared between `ProductionIngestor` and a future `OutageIngestor` without a breaking schema change.
- Everything downstream of Raw write (row shape, validation/quarantine rules, sharding key, path builder, and the source adapter contract) is intentionally Production-shaped and would need new, parallel implementations for events rather than reuse — the deck and architecture doc are explicit that Production ("stable day identity") and Events ("no stable batch identity, `EventId`-driven idempotency") are different extraction models, not the same model with a different row type.
- The current repo has **no concrete source adapter** for Production yet either (`IProductionSourceAdapter` has no registered implementation in [Program.cs](src/ProductionIngestor/Program.cs#L67-L69)) — so an Outage Ingestor would face the same "Open Question: concrete source system" gap that Production's plan resolved via user confirmation (SQL Server) before implementation began.
- No `deploy/helm`, `Dockerfile`, or CI workflow files exist in the repo yet (per the plan's Phase 0 scaffolding, which does not appear to have been executed) — only source-code folders exist under `src/` and `tests/`.

## Historical Context (from `thoughts/`)

- [thoughts/shared/plans/2026-09-14-production-ingester.md](thoughts/shared/plans/2026-09-14-production-ingester.md) is the only prior plan in `thoughts/shared/plans/`. It explicitly lists "other source types (outage, alarm, curtailment, adjustment, event-shaped Raw per §4.3.1)" under **"What We're NOT Doing"** (line 64), confirming the Outage Ingestor was deliberately deferred to a separate future work item rather than overlooked.
- That same plan's "Open Questions" section (language/framework, source system, safety lag default, control-plane DB, queue/cloud topology, blob conventions, legacy dual-run target, windfarm/WTG config source, backfill trigger mechanism) were all resolved by the user for Production and are very likely to recur — with source-specific answers — when planning the Outage Ingestor (e.g., "what is the concrete Outage source system: API or DB?", which is still an open question per the deck's own "⚠️ Outage API" vs. architecture doc's "Outage system" phrasing).

## Open Questions

These cannot be answered from the codebase or the fetched slide content and must be confirmed by the user/developer before an implementation plan for the Outage Ingestor is written — do not guess:

1. **Concrete Outage source system and its capabilities.** The deck labels it "⚠️ Outage API" on architecture diagrams but "Outage system" / "Outage API / table" elsewhere — is the source a REST API, a direct DB table, or both? What are its change-timestamp column name(s), row key, and does it expose a full-current-state read (needed since batches carry full current state, not deltas)?
2. **Whether Outage reuses the exact same `extraction_cursor` Postgres table/schema** as Production (just a new row per windfarm with `source_type='Outage'`) or needs a variant — nothing in the code prevents reuse, but no decision has been recorded.
3. **Whether `ExtractionCursor`/cursor-repository code should be extracted into a shared library** consumed by both `ProductionIngestor` and a new `OutageIngestor` project, versus duplicated per the "no extra abstractions without a task-specific reason" repository guidance — this is a structural decision for the planning phase, informed by [.github/skills/csharp-architecture/SKILL.md](.github/skills/csharp-architecture/references/api-core-reference.md).
4. **Solution/project layout for the new ingestor** — a new `src/OutageIngestor/` worker project analogous to `src/ProductionIngestor/`, sharing `ProductionIngestor.Contracts` (rename? or keep Production-named contracts project shared across ingestors?), and whether it needs its own `.Api` project or reuses none (Production's own `.Api` project's actual purpose/contents were not investigated in this pass and should be reviewed before deciding).
5. **Outage event row schema specifics** — the deck's example columns (`EventId, WindfarmId, ItemId, EventType, OriginalStartUtc, OriginalEndUtc, Status, SourceRevision, SourceWatermarkUtc, IsTombstone`) are illustrative; the authoritative source system's actual column names/types for these fields are unknown and must be confirmed, same as Production's Open Question 2 was.
6. **SafetyLagInterval, lease duration, and other `IngestionOptions` defaults for Outage** — should it share `IngestionOptions`/`SourceOptions` types with Production, or does the append-only/no-day-reread model need different option shapes (e.g., no `ParquetRowGroupSize`-per-day concept, since there's no day grain)?
