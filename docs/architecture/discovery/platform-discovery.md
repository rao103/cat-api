# Loss Calculation Platform — Platform Discovery

**Updated:** 2026-10-09, Asia/Kuala_Lumpur  
**Coverage:** available architecture discussions through 2026-10-08 and the current source documents listed in §12.  
**Repository location:** `docs/architecture/discovery/platform-discovery.md`  
**Companion:** `docs/architecture/decision-log.md` (delivered as `loss-calculation-decision-log.md`; rename when adding to the repository).

## 1. Purpose and reading guide

This is the living explanation of the platform: the problem, the current design, how it evolved, and what still needs discovery. The companion Decision Log records choices, reasons, rejected alternatives, and verification gaps. Together they let another developer or AI account continue without the original private conversations.

This edition consolidates recoverable history; it is not a complete transcript. Dates identify known discussion or document checkpoints, not invented approval dates. “Accepted” describes agreed design intent. A source document reporting implementation or tests is identified as such; this document does not certify the current repository, deployment, or production behavior. No source code or infrastructure was changed while preparing these documents.

Read the current design first, then the history and open questions. Before implementing a task, inspect its actual contracts, migrations, configuration, and tests. Resolve disagreements explicitly rather than choosing whichever older document is easiest to implement.

## 2. Problem, goal, and boundaries

The platform ingests Production, Outage, Alarm, Curtailment, and Adjustment data, harmonizes them into reproducible calculation inputs, calculates losses, and publishes the latest results for applications and reporting. Historical changes, late events, deletions, retries, and user adjustments must produce correct recalculation with traceable origins.

The immediate goal is to finish a usable Loss Calculation Platform with consistent naming, paths, messages, and component responsibilities. New developers should be able to understand and extend it. Additional frameworks, component splits, and process documents need a concrete delivery reason.

Phase-one operation includes ingestion, Raw Fan-Out, Harmonizer with event lifecycle processing, Package Coordinator, Calculation, publication, and necessary control/monitoring. Historical Query Engine is a phase-two analytical capability. Its future needs influence immutable history and lineage now, but it is not a prerequisite for ordinary processing.

The separately discussed enterprise Agentic AI/MCP platform is outside this system's runtime scope. AI-assisted engineering practices apply to how this platform is built; they do not add a planner agent or MCP gateway to the data pipeline.

## 3. Current architecture and ownership

| Responsibility | Owner | Durable result / handoff |
|---|---|---|
| Pull source data, manage checkpoints and extraction leases | Source Ingestor | Immutable Raw files and an explicit ready notification; `extraction_cursor` and `extraction_run` |
| Accept Adjustment CRUD | Business Web API | Adjustment parent/WTG membership plus outbox in one transaction |
| Deliver Adjustment changes into ingestion | Outbox dispatcher / Adjustment ingress | Retriable notification and Raw creation; exact ingress contract still to be verified |
| Discover deterministic downstream work | Raw Fan-Out | Persisted dispatch plan; WTG-day work for time series, event/member work for lifecycle sources |
| Merge source data and rebuild affected daily inputs | Harmonizer Core/Worker | Immutable source revisions, lineage, committed pointers and coordination intents |
| Continue open events and unfinished event rebuilding | Event lifecycle processing inside Harmonizer Worker | Resumable bounded work and cleanup; no separate Event Lifecycle deployment |
| Check source readiness and assemble calculation inputs | Package Coordinator | Package manifest and calculation dispatch; integration remains to be completed/verified |
| Execute loss formulas against a package | Calculation Worker | Versioned calculation results; detailed formulas and contracts need their own discovery |
| Publish current results | Publisher / summary responsibility | Latest PostgreSQL serving data and publication evidence; final component grouping unresolved |
| Trigger and observe processing | Centralized API | Operation identifier, refresh/run-now controls, progress derived from existing component records |
| Reconstruct historical results | Query Engine, phase two | Selection of committed historical artifacts and lineage |

This is a responsibility map, not a requirement for one deployment per row. Preserve existing meaningful Core/Worker organization. Suggestions to merge Package Coordinator, lifecycle processing, and Publisher were discussed; only lifecycle placement inside Harmonizer is established in the reviewed checkpoint.

### 3.1 Source processing differs by data shape

| Source | Data / identity | Fan-Out work | Harmonizer behavior |
|---|---|---|---|
| Production | Ten-minute values per WTG | WTG-day | Merge affected daily source revision |
| Curtailment | Ten-minute WTG data, per agreed classification | WTG-day | Production-style daily processing; adapter evidence still needed |
| Outage | Classification belongs to one WTG; `OutageClassificationId` is identity | Event/member, exact Raw paths | Compare canonical old/new state, then rebuild affected WTG-days |
| Alarm | Event lifecycle source | Event/member design direction | Requires actual contract and fixtures before adapter implementation |
| Adjustment | One parent Adjustment ID, one-to-many child WTG memberships | Preserve event and member identity | Reconcile changed/deleted membership and affected dates; contract still needs implementation evidence |

An event may cover many days without belonging to many WTGs. Generic event/member messaging accommodates Adjustment's multi-WTG relationship; it does not imply that a single Outage classification simultaneously belongs to several WTGs.

### 3.2 Why event Fan-Out cannot determine all affected days

If an outage changes from September 27–October 5 to September 29–October 3, the new row alone identifies neither all old contributions nor the cleanup work. Raw Fan-Out forwards identities and source paths. Harmonizer reads prior canonical state and reconciles the union of old contributions, unfinished cleanup, and new applicability. Removed days need explicit empty output when no other events remain; unrelated overlapping outages must remain in the rebuilt day.

## 4. End-to-end processing scenarios

### 4.1 Incremental extraction

1. Select an eligible `extraction_cursor` for source and windfarm. Respect lease ownership, interval, and failure backoff; run-now may bypass eligibility time, not concurrent ownership.
2. Claim the lease and create an `extraction_run`. Propagate the trigger's `operation_id` when applicable.
3. Read changes after the committed checkpoint, using the source's tie-breaker and deletion rules. Write complete immutable Raw artifacts.
4. Publish the logical ready notification. Advance the committed checkpoint only at the durable handoff boundary supported by the implementation. Upload success followed by notification failure must remain recoverable.
5. Raw Fan-Out validates input, freezes its deterministic plan, and dispatches work. Retries reuse frozen identities and bodies.
6. Harmonizer commits source revisions or accepts canonical event state and resumable lifecycle work. Package Coordinator later evaluates current readiness, builds a package, and dispatches calculation.
7. Calculation and Publisher produce and expose the latest accepted result. Control records and telemetry retain operation correlation.

Ingestor completion means its extraction/handoff completed. It does not mean Harmonizer, Calculation, or publication completed.

### 4.2 Refresh

Centralized API supplies windfarm, source selection, and time range. Ingestor may split the extraction into several Parquet files while issuing one logical completion envelope with the complete part set. For snapshots, explicit scope and completeness evidence are necessary before interpreting missing events.

A Raw snapshot manifest is optional when the envelope provides a complete validated path list and all required authority metadata. This does not make package manifests or harmonized lineage optional; these artifacts serve different purposes. Never infer successful empty refresh from missing files or a failed source read.

**Unresolved integration issue:** the reviewed 6 October Fan-Out design requires a source-consistent `SourceVersionCutoffUtc` before authoritative absence removal. The current Harmonizer checkpoint instead describes scoped exclusions ordered by a shared monotonic extraction-token namespace. Host `SnapshotAsOfUtc` is observation provenance in the Fan-Out design, not a verified source cutoff. These are materially different contracts. Align producer, Fan-Out, and Harmonizer before claiming refresh absence handling is end-to-end ready; see Q-006 in the log.

### 4.3 Adjustment and recalculation

Business Web API commits Adjustment business changes and outbox intent together. Dispatcher publishes the change; Adjustment ingress creates Raw and processing-run evidence, then joins the shared pipeline. Retain the parent ID and all affected child WTG memberships, including removed memberships needed for cleanup.

The existing Decision Log records that Adjustment normally needs `extraction_run` but not a polling `extraction_cursor`. Earlier notes suggested reusing both. Use the later event-driven baseline and inspect actual implementation before altering schema. Automatic Adjustment generation is future work that should use the same write/outbox boundary.

### 4.4 Bring forward an earlier version

The agreed direction is a Centralized API operation that reads an earlier harmonized version, creates an import representation such as CSV, and sends it through ingestion as new data. It preserves historical truth instead of silently rolling back current SQL. Cross-environment restoration, including production to preproduction, was discussed; exact authorization, source-version handling, identity mapping, and import contracts remain to be designed.

### 4.5 Historical query

PostgreSQL serves the latest data. A future Query Engine selects committed historical artifacts and their lineage across sources, months, and WTG-days. Different source revisions may be combined in one historical view. A current pointer alone cannot answer arbitrary past-time queries; retain sufficient committed publication history to identify what was authoritative at the requested time.

“Data state as of a timestamp” and “what a user actually saw in a UI” are different requirements. The reviewed Harmonizer checkpoint explicitly does not prove the latter. Exact as-of time semantics and publication indexing remain open.

## 5. Identity, persistence, and storage

### 5.1 Identity vocabulary

| Term | Meaning / constraint |
|---|---|
| `windfarm_id`, `wtg_id` | Numeric catalog identifiers; canonical target vocabulary |
| `windfarm_name`, `wtg_name` | Text identities for source interoperability and readable paths |
| `operation_id` | Correlates a top-level trigger across components; created by one lightweight `operation` table |
| `run_id` | Particular extraction execution; stored in `extraction_run` |
| `ExtractionToken` | Fenced extraction identity; capture ordering requires an explicitly shared namespace |
| Event ID | Source's canonical business event identity; Outage maps classification ID, not descriptive Outage ID |
| Member key | Event's WTG membership identity; distinct from batch/transport identity |
| Revision | Logical artifact publication version |
| Attempt / fence token | Ownership of a publication attempt; prevents stale owners committing |

Replace ambiguous `item` with `wtg` in new domain contracts. Existing names such as `ProductionItemDayWorkV2/V3`, `harmonizer_batch_item`, and `wtgid` may remain compatibility/schema identifiers until migrated. Do not rename wire contracts or persisted bodies without a compatibility plan.

The reviewed Outage producer mapping is especially important: source `OutageClassificationID` becomes normalized `EventId`; source `OutageID` becomes descriptive `OutageId`; source `LastUpdatedUTC` becomes `LastUpdatedDate`. Some existing Raw fixtures have name strings in columns labelled `WindfarmId` and `WtgId`. Resolve those through the authoritative catalog; do not cast them to integers or confuse them with numeric envelope identifiers.

### 5.2 PostgreSQL responsibility

Each environment has its own database. Do not introduce an environment column to distinguish Dev/Test/PreProd/Prod inside domain/control tables.

| Records | Responsibility |
|---|---|
| `extraction_cursor` | Current checkpoint, eligibility, lease and source/windfarm control |
| `extraction_run` | Extraction execution audit and operation correlation |
| Lightweight `operation` table | Generate/identify the root operation; no duplicate operation-work ledger |
| `raw_artifact_dispatch`, `raw_artifact_batch` | Frozen Raw Fan-Out dispatch and batch identity |
| Fan-Out failure / dead-letter records | Recovery evidence; final retention policy unresolved |
| `wtg_day_input_state`, `harmonizer_batch`, `harmonizer_batch_item`, `harmonizer_outbox` | Existing committed input pointers, frozen work, receipts and coordination intent |
| `event_partition_state`, `event_materialization_state` | Current event catalog pointers/ownership and bounded resumable progress in the reviewed checkpoint |
| Serving tables | Current business results |

Canonical event business history remains in immutable files. The two event control tables do not reverse the decision to avoid a second operation-work ledger: their purpose is event publication ownership and recovery.

Use the repository's actual PostgreSQL migration mechanism. Earlier SQL Server/DACPAC practices are historical context, not proof that DACPAC deploys this PostgreSQL schema. The consolidated Harmonizer `0004` is for an unapplied baseline after prerequisites; deployed databases require forward migrations.

### 5.3 Artifact roles and illustrative paths

| Artifact | Example / role |
|---|---|
| Production Raw | `raw/production/windfarm=WOW02/mode=refresh/extraction=58/window-...-part-0001.parquet` |
| Outage Raw | `raw/outage/windfarm=WOW01/extraction=42/events/...parquet` and `deleted-events/...parquet` |
| Daily source output | `harmonized/wtg/windfarm=WF01/wtg=T001/date=2026-07-18/outage/revision=5/attempts/token=44/outage-segments.parquet` |
| Canonical monthly Outage | `harmonized/events/outage/windfarm=WF01/startyear=2026/startmonth=07/revisions/revision=7/attempts/token=42/outage-events.parquet` |
| Delete-first canonical state | Separate `harmonized/events/outage/.../deleted-events/...` branch |
| Source catalog | Immutable manifest candidate under the source's `_catalog/revisions/...` branch |
| Lifecycle progress | Immutable resumable plan under `harmonized/lifecycle/outage/...` |
| Calculation package | `packages/revision=N/manifest.json`, owned by Package Coordinator |

These examples preserve reviewed structure; they do not settle all source path/casing conventions. In the current event checkpoint, `snapshot.json` is normalized internal merge state, `lineage.json` records provenance, and committed database pointers select authoritative immutable candidates. Earlier descriptions calling every snapshot file a global current pointer are too broad. Publisher's serving/publication pointer is a separate boundary.

## 6. Event lifecycle and readiness

Outage end time may remain null. Order business mutations by source `LastUpdatedDate`; a newer tombstone removes the classification, stale live rows cannot resurrect it, and an equal-version content conflict is an error. An actually newer legitimate live version can replace a tombstone. Source processing order of events before deleted-events is not a guarantee of SQS delivery order.

The reviewed Harmonizer checkpoint retains old/new footprints, performs bounded daily rebuilds, and continues open/future events as later days become processable. Its default processable cutoff is yesterday UTC. Clock-driven continuation does not keep the original source operation open indefinitely; exact monitoring integration should follow the verified implementation.

For Outage, no active known event means no outage. There is no extra upstream completeness gate solely to prove absence. Nevertheless, accepted unresolved work, failed reads, source conflicts, and pending cleanup must prevent false ready/empty success. Package Coordinator must use the pending predicate even if no current Outage daily row exists. Do not demand five physical Complete rows as a universal readiness rule; define each source's readiness semantics.

The checkpoint differentiates SQS deletion after durable canonical/resumable handoff from final materialization completion and Raw artifact acknowledgment. The Fan-Out document describes a different batch receipt boundary. Align canonical receipts, final completion, barriers, and ACK meaning in the integration contract (Q-007).

## 7. Runtime and operational constraints

Workers run on EKS with workload identity and external durable state. Infrastructure creation follows approved App Bundle/Service Catalog routes; the EKS and RDS accounts differ. Connection and secret values belong in deployment configuration, not these documents.

Polling ingestors may be scheduled; queue consumers use long polling. Horizontal scaling does not eliminate same-key ownership. The reviewed event implementation serializes publication per windfarm/source with database leases, heartbeat and fenced commits, while allowing different farms to proceed concurrently. Bounded passes limit memory and improve fairness. Uploaded candidates from expired owners must remain uncommitted.

Queue-only scaling is insufficient for Harmonizer's autonomous lifecycle backlog. The checkpoint requires at least one replica, including `KEDA minReplicaCount=1`. Exact deployment limits and realistic load behavior remain to be validated. Queue-name spelling (`cat-harmonize-dev` versus `cat-harmonizer-dev`) is unresolved; configure from actual infrastructure.

Use OpenTelemetry logs, traces, and metrics. Correlate operation, run, batch/message, source, attempt and extraction identity where available. Keep high-cardinality IDs in logs/traces rather than metric labels. An operation identifier is not a W3C trace identifier. Dashboards derive progress from existing records; do not add synthetic database statuses or a duplicate execution ledger simply to fill a progress bar.

## 8. Design history and reasons for change

| Period / checkpoint | Direction at that time | Refinement and reason | Log |
|---|---|---|---|
| Earlier architecture, exact date not recovered | Raw → Harmonized → Calculation → publication; daily work and SQL serving | Established reproducible stages and queue-based recovery; older terms include `item` and Azure SQL | DL-001, DL-008 |
| 2026-09-28 to 2026-10-03 discussions | PostgreSQL current serving, Parquet history; incremental/refresh/history scenarios | Separate fast current reads from historical artifacts; restoring old data becomes new processing | DL-008, DL-018 |
| 2026-10-02 naming refinement | Replace generic item with WTG; carry numeric IDs and text names | Reduce ambiguous mappings across source/queue/storage boundaries | DL-001 |
| 2026-10-04 to 2026-10-05 event discovery | Universal WTG-day Fan-Out proved insufficient | Old event state is needed for shortened/moved/deleted cleanup; preserve event/member identity | DL-005 → DL-013 |
| 2026-10-05 responsibility refinement | Event Lifecycle considered a separate component | Bring lifecycle processing into Harmonizer; keep Production/Curtailment on daily path | DL-014 |
| 2026-10-06 delivered Harmonizer checkpoint | Canonical files, compact control state, bounded lifecycle processing | Package Coordinator owns package generation and calculation dispatch; committed pointers and fences isolate attempts | DL-006 → DL-015; DL-016 |
| 2026-10-06 integration documents | Fan-Out cutoff design and Harmonizer capture-token design coexist | Refresh absence authority and receipt boundaries need explicit reconciliation | Q-006, Q-007 |
| 2026-10-06 migration discussion | Separate unapplied Harmonizer migrations | Consolidate the unapplied baseline; retain forward migrations for deployed schemas | DL-019 |
| 2026-10-07 operation refinement | Extra operation execution/work tables proposed | User chose one lightweight operation identity table; existing component records remain authoritative | DL-002 |
| 2026-10-07 identity clarification | Generic event/member model could imply all events have many WTGs | Outage classification is one WTG; Adjustment parent may have many child WTGs | DL-004, DL-007 |
| 2026-10-08 engineering/handoff discussion | Decisions concentrated in private chats | Keep Discovery and Decision Log in the same repository; task-scoped Micro-RPI; reusable maintenance skills | DL-020 |
| 2026-10-09 consolidation | Earlier log contained universal daily-work and snapshot-pointer shorthand | Preserve prior entries, supersede those generalizations, and expose integration conflicts | This edition |

Historical September progress percentages are deliberately not used as today's delivery status. Generated code, a plan labelled implemented, and a deployed working pipeline are distinct evidence levels.

## 9. Implementation evidence and current gaps

The reviewed `Harmonizer_Event_Lifecycle_Implementation_Plan.md` version 3 reports a locked restore, Release build, Worker publish, 151 passing unit tests, 18 skipped Npgsql integration tests, and an embedded PostgreSQL smoke run. These are source-reported results, not tests rerun for this documentation task. The checkpoint calls out unresolved real PostgreSQL concurrency tests, provider permissions/load validation, common capture token assumptions, Package Coordinator integration, and other source adapters. Sample publishing is disabled pending coordination alignment.

The current `0004_create_harmonizer_schema.sql` was read as a schema source, not executed. No deployment or current repository inspection establishes completion of the downstream platform. Consequently:

- Treat Production/Outage code deliveries as historical implementation evidence with stated limits.
- Treat Alarm, Adjustment, and Curtailment adapter coverage as unverified unless actual contracts and tests demonstrate it.
- Treat Package Coordinator, Calculator, Publisher and Centralized API details as discovery/integration work where concrete code evidence has not been supplied.

## 10. Next discovery and implementation slices

These are proposed work slices, not new accepted architecture decisions.

| Priority | Concrete next outcome | Evidence that closes it |
|---|---|---|
| 1 | Align Outage producer/Fan-Out/Harmonizer snapshot authority and receipt boundaries | One contract plus tests for delayed snapshot, newer incremental update, empty complete scope, failed child, stale worker and retry |
| 2 | Define Package Coordinator readiness and package publication | Contract consuming actual Harmonizer pointers/pending predicate; behavior for no known Outage, pending cleanup, duplicate notification and source revision change |
| 3 | Implement a vertical path from package through loss calculation to serving | Agreed formula/version inputs, representative expected outputs, publication recovery evidence |
| 4 | Extend remaining source adapters | Real source fixtures and membership/lifecycle rules; no guessed Alarm/Adjustment schemas |
| 5 | Connect operation monitoring and control | API queries over actual extraction/batch/publication evidence; correlation demonstrated through a complete run |

Keep historical Query Engine and restore details visible as later work. Do not require them before ordinary production operation unless a concrete acceptance requirement changes.

## 11. How these documents fit existing engineering practice

The three lifecycle stages and three standards levels are separate concepts.

| Lifecycle stage | Working material | Purpose |
|---|---|---|
| Architecture | This Discovery + shared Decision Log | Understand the problem, evolve boundaries, record choices |
| Implementation | One task-scoped Micro-RPI under `docs/implementation/micro-rpi/` | Research relevant code, plan the slice, implement, record meaningful validation |
| Maintenance / operations | Existing reusable skills | Repeat established bug-fix and setup procedures, including windfarm/WTG onboarding once behavior is known |

The established standards remain Level 1 `docs/AI_RULES.md`, Level 2 `docs/standards/`, and Level 3 `docs/components/`. Existing `.github/skills/` contains reusable AI workflows; it is not GitHub Actions configuration merely because it is under `.github`. Do not create a second standards tree or predefine a Calculator skill before its behavior is understood.

For each task, read relevant standards, Discovery sections and decision IDs; inspect actual implementation; create/update its Micro-RPI; then update these two documents only if architecture knowledge changes. Capture a discovered architecture issue before silently expanding implementation scope. Separate ADRs are optional for substantial decisions, not mandatory additional paperwork.

## 12. Sources and handoff

| Source | Evidence used |
|---|---|
| `loss-calculation-decision-log.md`, initial compilation 2026-10-08 | Existing DL-001–DL-012 and Q-001–Q-005; preserved and amended in the companion |
| `Harmonizer_Event_Lifecycle_Implementation_Plan.md`, version 3, updated 2026-10-06 | Delivered design checkpoint, paths, database footprint, readiness, ownership, acknowledgment and reported validation limits |
| `2026-10-06-outage-raw-fanout-v3-design(1).md` | Producer field mappings, snapshot manifest contract, source cutoff requirement, frozen planning and receipt design |
| `0004_create_harmonizer_schema.sql`, version 1 | Current saved schema and readiness function/view; read only |
| Recovered discussions through 2026-10-08 | User decisions on operations, event membership, serving/history, component organization, and repo-portable engineering practice |

The governance archive was identified but not inspected in this task; its precise files must be checked before modifying rules. Earlier archives and plans are not assumed to reflect the live repository.

**Handoff instruction:** Read this file and the companion Decision Log, identify relevant accepted decisions and unresolved questions, then inspect the actual code and tests for the requested slice. Respect established constraints. Record new evidence and changed choices in the same repository change. Never infer current deployment success from conversational acceptance or a historical build report.
