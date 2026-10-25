# Loss Calculation Platform — Architecture Decision Log

> **Portable project context.** This document is intended to live in the team's Git repository at `docs/architecture/decision-log.md` so that developers, reviewers, and AI assistants using **different accounts** can continue the work without access to any private chat history.
>
> **Originally compiled:** 2026-10-08. **Updated:** 2026-10-11 (Asia/Kuala_Lumpur). **Provenance:** prior design discussions plus the saved source documents identified in §8; no inspection of the current live repository or deployed infrastructure. Decision dates below refer to discussion dates when known; the compilation date is not necessarily the original decision date.
>
> **IMPORTANT:** “Accepted in discussion” means design intent agreed in conversations. It **does not mean implemented, tested, deployed, or formally approved by the company**. Verify against code, migration scripts, and environment configuration before treating any entry as the live system's behavior.

**Companion:** `docs/architecture/discovery/platform-discovery.md`. This edition preserves existing decisions/questions and adds DL-022–DL-023, source evidence and topic specifications. DL-023 amends the file organization in DL-020; design intent remains distinct from verified deployment. Repository target remains `docs/architecture/decision-log.md`; the delivered filename retains its existing identity.

## 1. How to use this file

1. **Start here:** before proposing design changes or generating code, read this file and the relevant source code, migrations, component configuration, and tests. This log is a guide, **not a replacement** for repository evidence.
2. **During Discovery:** capture proposed decisions and alternatives in a short draft entry. Update the draft as understanding changes. Do not invent decisions from suggestions.
3. **At a decision point:** record *what was chosen, why, what was rejected, what it affects, and what remains unverified*. Move the entry to **Accepted in discussion** only after a human decision-maker confirms it.
4. **When implementation/validation completes:** append evidence, such as a PR link, migration identifier, test run, and observed result. Do not confuse a passing unit test with full end-to-end or production verification.
5. **For high-impact decisions:** promote the entry to a separate ADR under `docs/architecture/adr/ADR-NNNN-<title>.md`; link it here. Small decisions can remain in this log.
6. **On changed decisions:** do not silently overwrite an accepted rationale. Create a new entry, mark the old one **Superseded**, and cross-link. Git history and PR review preserve accountability.
7. **Ownership:** update this file in the same pull request as the corresponding architecture change. Reviewers check it along with code and schema.

**Suggested status vocabulary:** `Proposed`, `Accepted in discussion`, `Implemented (evidence linked)`, `Validated (evidence linked)`, `Superseded`, `Needs confirmation`.

**Entry format:** ID · Status · Decision · Why/alternatives · Scope/constraints · Evidence/next check · Superseded by (if any).

## 2. Architecture context (orientation, not a decision)

The platform processes **Production, Outage, Alarm, Curtailment, and Adjustment** data. A typical path is **Ingestor → immutable Raw Parquet → Raw Fan-Out → Harmonizer (daily inputs and event lifecycle) → Package Coordinator → Calculation → Publisher**, with PostgreSQL serving the latest business view. A Centralized/Operations API provides control and monitoring. Adjustment enters through a Business Web API and transactional outbox rather than a standard polling source. Components use queues for delivery and OpenTelemetry for diagnostics.

**Terms:** `windfarm` = wind farm; `wtg` = wind turbine generator; `operation_id` = end-to-end business-trigger correlation; `run_id` = a specific execution attempt/run. Prefer `wtg` over legacy generic `item` naming.

## 3. Recorded decisions

### DL-001 — Canonical wind-farm and turbine naming
- **Status:** Accepted in discussion · Recorded 2026-10-08
- **Decision:** Use `windfarm_id` (integer), `windfarm_name` (text), `wtg_id` (integer), and `wtg_name` (source identity such as `WtgRdsppId`) where applicable. Replace ambiguous `item` naming with `wtg` across messages, schema, and code boundaries. Messages/queries that need names for source interoperability carry both identifiers and names.
- **Why:** Readable, consistent cross-component contracts and less mapping ambiguity.
- **Validation needed:** Verify Parquet columns, SQS message contracts, database migrations, and downstream joins; note any legacy compatibility adapters.

### DL-002 — Separate `operation_id` from execution `run_id`; do not add a second operation-work ledger
- **Status:** Accepted in discussion (latest refinement 2026-10-07) · Recorded 2026-10-08
- **Decision:** One top-level trigger receives a distinct `operation_id` propagated through the existing processing chain; individual ingestion executions continue using `extraction_run.run_id`. A **single lightweight `operation` table** is sufficient to create/identify the operation; **do not introduce an extra `operation_work`/`operation_execution` table merely to duplicate scope or statuses**. Read scope and progress from `extraction_run`, `extraction_cursor`, and relevant existing component batch/publication records. Retries for the same business operation retain the correlation ID.
- **Why / rejected alternative:** A suggested two-new-table operation model duplicated information already owned by component-specific run/control tables. The platform should expose actual evidence of component progress rather than manufacture health statuses or heartbeats solely to make a dashboard look complete.
- **Validation needed:** Compare the **latest** agreed migration with older v13/v14 documents that may still mention `operation_execution`; inspect actual correlation propagation and any consumer monitoring queries. Do not treat older schema drafts as newer decisions.

### DL-003 — Pull-source checkpoint ownership
- **Status:** Accepted in discussion · Recorded 2026-10-08
- **Decision:** For the pull-based sources (Production, Outage, Alarm, Curtailment), `extraction_cursor` remains the authoritative per-source/per-windfarm checkpoint and lease/control state; `extraction_run` records each execution and its results/status. Advance committed checkpoints only after durable Raw publication and successful handoff, using appropriate fencing/transactional checks. `operation_id` references the broader operation, not a replacement for these tables.
- **Why:** Distinguish current resumable state from the audit history of runs; preserve failure recovery and idempotency.
- **Validation needed:** Resolve the exact current composite key and per-source checkpoint columns from the latest migration (earlier iterations differed); verify lease expiry/retries and commit fencing with integration tests.

### DL-004 — Adjustment is event-driven; preserve one-to-many WTG membership
- **Status:** Accepted in discussion (refined 2026-10-04 and 2026-10-07) · Recorded 2026-10-08
- **Decision:** Business Web API writes Adjustment and an outbox entry in the **same database transaction**. An outbox dispatcher publishes to a dedicated Adjustment queue; Adjustment Ingestor creates/uses `extraction_run`, emits Raw, then enters the shared Fan-Out → Harmonizer → Calculation → Publisher pipeline. Unlike polling sources, Adjustment **does not normally need an `extraction_cursor`**. One Adjustment parent ID may be associated with **multiple WTGs** via child membership; do not force a one-adjustment–one-WTG source model.
- **Why:** Reliable event publication, reuse of common processing, accurate source-domain relationships.
- **Validation needed:** Outbox idempotency, child-table mapping, adjustment fan-out per WTG, and final deletion/update semantics.

### DL-005 — Raw is immutable; Fan-Out creates WTG-day work
- **Status:** Superseded in part by DL-013 · Original record 2026-10-08; correction recorded 2026-10-09
- **Historical correction:** The statement below overgeneralized Production behavior. Immutable Raw and durable handoff remain valid. Event sources use event/member work; they do not use universal WTG-day Fan-Out. Retained for history, not as current implementation guidance.
- **Decision:** Raw Parquet represents the source extraction/window, with immutable artifacts and lineage. Raw Fan-Out decomposes those artifacts into WTG-day tasks; Harmonizer processes these consistently. An extraction that generates multiple Raw files may publish one logical ready message that enumerates parts, depending on source/mode. The pipeline is at-least-once; workers require idempotent and stale-order-safe handling.
- **Why:** Avoid reorganizing source extraction into business dates prematurely; allow deterministic replay/recalculation and bounded execution.
- **Validation needed:** Exact Raw path conventions, batching rules for incremental versus refresh, and actual Fan-Out message schema.

### DL-006 — Harmonized output has revisions, packages, snapshot, and lineage
- **Status:** Superseded in part by DL-015 · Original record 2026-10-08; correction recorded 2026-10-09
- **Historical correction:** Daily source revisions and lineage remain valid. The current event checkpoint adds canonical event storage, assigns packages to Package Coordinator, and treats snapshot files as internal merge state selected through committed database pointers. Existing meaningful source folders are allowed; the constraint is against redundant layers.
- **Decision:** Organize Harmonized data by `windfarm`, `wtg`, and `date`; maintain per-source revisions, source lineage (`lineage.json`), a current-revision pointer (`snapshot.json`), and package manifests under a `packages` directory. Preserve the existing meaningful hierarchy; avoid adding redundant `SourceTypes`/`Adapters`/duplicate `Operations` levels.
- **Why:** Traceability, historical as-of reconstruction, and maintainable component organization.
- **Validation needed:** Exact revision/publish atomicity, path casing/filenames, and consistency after retries or concurrent updates.

### DL-007 — Outage event identity and deletion semantics
- **Status:** Accepted in discussion · Recorded 2026-10-08
- **Decision:** Outage classification identity is `OutageClassificationId`; processing output is WTG-specific, **not one event spanning multiple WTGs**. `OutageClassificationEndUTC` may be null for ongoing outages. Carry a separate deleted-events feed/artifact; a later deletion timestamp must be respected. Ingestor ordering is **events first, deleted-events second**. Raw examples use `/raw/outage/windfarm=<name>/extraction=<id>/events/` and `/deleted-events/`.
- **Why:** Preserve exact event lifecycle and support deletion/replay without confusing tombstones with normal classifications.
- **Validation needed:** End-to-end tests for open outages, later deletes, duplicates, stale updates, and WTG-day slicing; don't generalize this identity rule to Adjustment's parent/child relationship.

### DL-008 — PostgreSQL current serving; Parquet historical/as-of
- **Status:** Accepted in discussion · Recorded 2026-10-08
- **Decision:** PostgreSQL is the **current/latest** serving store, while immutable Parquet data plus manifests/lineage provide historical as-of views and revisions. Publisher updates the current serving state and appropriate pointers only after successful publication. Reinstating older content is treated as a new operation/republication, not a silent SQL rollback.
- **Why / alternative:** Avoid retaining every row-level historical revision in the live SQL serving schema. Keep performant latest queries while preserving reproducible history.
- **Validation needed:** As-of selection across mixed source revisions and days; commit/pointer consistency; recovery when publication partly fails.

### DL-009 — Business API versus Operations/Centralized API
- **Status:** Accepted in discussion · Recorded 2026-10-08
- **Decision:** Business Web API owns user-facing Adjustment writes plus outbox. Centralized/Operations API owns execution triggers (e.g., refresh/run-now), `operation_id`-based progress/monitoring, and administrative/historical recovery controls. Existing control tables remain the source of truth for execution evidence.
- **Why:** Separate business CRUD/transactional boundaries from orchestration and monitoring without an unnecessary new state-machine database.
- **Validation needed:** Final API contracts and authorization; handling partial/stuck operations without fabricated stage status.

### DL-010 — Dedicated transport queues and dead-letter handling
- **Status:** Accepted at topology level; **queue-name reconciliation needed** · Recorded 2026-10-08
- **Decision:** The target topology includes a dedicated Adjustment ingress queue, Raw Fan-Out, Harmonizer, Calculation Main, Calculation Backfill, and Summary Generation—**six primary flows**, each with a DLQ. SQS carries work notifications; immutable files carry data; PostgreSQL carries transactional run/business state.
- **Why:** Independent retries/scaling and fault isolation across pipeline stages.
- **Validation needed:** Infrastructure configuration and naming. Prior notes contain both `cat-harmonize-dev` and `cat-harmonizer-dev`; **do not treat either spelling as authoritative until verified**. Older queue lists omit the newly added Adjustment queue.

### DL-011 — Operational telemetry and correlations
- **Status:** Accepted in discussion · Recorded 2026-10-08
- **Decision:** Workers use OpenTelemetry logs, traces, and metrics with fields such as `operation_id`, `run_id` where available, `work_version`, `attempt`, `extraction_token`, and `message_id`. Track receive → parse → write → publish → ACK/fail. Operational drilldown should use telemetry and component-owned ledgers, not duplicate every trace transition in the business database.
- **Why:** End-to-end diagnosis while keeping persistent domain/control tables focused.
- **Validation needed:** Confirm trace propagation across queues and collectors/exports (e.g., Application Insights) with a real end-to-end operation.

### DL-012 — One deployment database per environment; no environment column
- **Status:** Accepted in discussion · Recorded 2026-10-08
- **Decision:** Dev/Test/PreProd/Prod have separate databases and environments; do **not** add an `environment` column to domain/control tables just to distinguish deployments. Keep schema changes migration-controlled.
- **Why:** Separation is already a deployment boundary; avoid redundant environment dimensions in every table.
- **Validation needed:** Check migrations/config against actual deployment boundaries and release pipeline.

### DL-013 — Split daily-series work from event lifecycle work
- **Status:** Accepted design direction; reflected in reviewed 2026-10-06 documents · Recorded 2026-10-09
- **Context:** A changed event can shorten, move, or disappear. The new Raw row does not identify every old daily contribution requiring cleanup.
- **Decision:** Production and Curtailment use WTG-day work. Outage, Alarm and Adjustment use stable event/member identity with exact contributing Raw paths. Harmonizer determines affected dates using old canonical state, new applicability and unfinished cleanup. Raw Fan-Out freezes deterministic batches and reuses their exact identities/bodies on retry.
- **Why / alternative:** Universal WTG-day Fan-Out cannot recover the old footprint from new rows alone. Keep lifecycle knowledge with its canonical state owner.
- **Constraints:** Outage classification belongs to one WTG. Adjustment may have multiple child WTGs. Preserve existing Production V2/V3 compatibility; do not silently rewrite persisted V2 work as V3.
- **Verification:** Reviewed Fan-Out design and Harmonizer checkpoint support this direction. Real deployed compatibility, other source adapters, and old frozen message redrive remain unverified.
- **Supersedes:** DL-005's universal WTG-day assumption; preserves its immutable Raw principle.

### DL-014 — Event lifecycle processing lives inside Harmonizer
- **Status:** Accepted in discussion; implementation reported by the 2026-10-06 checkpoint · Recorded 2026-10-09
- **Decision:** Event lifecycle processing runs inside Harmonizer Core/Worker using its configuration, provider clients and telemetry. It merges canonical events, reconciles old/new footprints, emits complete daily contributions, and continues bounded pending/open-event work.
- **Why / alternative:** These behaviors need the same canonical state and output ownership. A separately deployed Event Lifecycle Coordinator is not required by the current design.
- **Constraints:** Rebuild a day with all applicable known events; shortening/deletion must remove old contributions without deleting unrelated events. No known active Outage means no outage, but pending work, failed reads and conflicts prevent false ready/empty results.
- **Verification:** Source-reported implementation is limited to Production/Outage. Alarm/Adjustment require real contracts and fixtures. The checkpoint uses yesterday UTC as default processable cutoff; confirm operational requirements before changing it.

### DL-015 — Separate source publication from package and serving publication
- **Status:** Current documented design checkpoint, 2026-10-06 · Recorded 2026-10-09
- **Decision:** Harmonizer owns canonical and daily source artifacts, lineage, committed source pointers and coordination intents. Package Coordinator owns `packages/revision=N/manifest.json` and calculation dispatch. Publisher owns current serving publication. Do not infer that Harmonizer directly publishes CalculationWork.
- **Why:** Package readiness combines source inputs; successful source publication alone does not establish a complete calculation package.
- **Artifact clarification:** Event `snapshot.json` is internal normalized merge state; `lineage.json` carries provenance. Immutable catalog/month/day candidates become authoritative through committed database pointers. Package manifests and serving publication pointers are different boundaries.
- **Readiness:** Package Coordinator must check the pending predicate even when no current Outage day row exists. When no event is known and no work is pending, absent Outage may be valid empty input. Do not universally require five physical Complete source rows.
- **Verification:** Real coordination consumer/notification contract and package behavior remain integration work. Broader consolidation with Publisher remains undecided.
- **Supersedes:** DL-006's generic snapshot-pointer interpretation and ambiguous package ownership.

### DL-016 — Keep event business history in files and compact recovery state in PostgreSQL
- **Status:** Current documented implementation checkpoint, 2026-10-06 · Recorded 2026-10-09
- **Decision:** Use immutable canonical monthly event files, a separate delete-first branch, daily segments with exact canonical revision/path references, and durable bounded progress plans. Existing Harmonizer batch/receipt/outbox tables remain; `event_partition_state` and `event_materialization_state` hold current pointers, ownership and resumable progress.
- **Why / alternative:** Avoid an SQL event business-history store and an unbounded per-day lifecycle-job table while retaining durable recovery.
- **Constraints:** Revision and attempt are separate. Pointer publication must be fenced; old links remain readable. Month/WTG movement must not publish half of a canonical change. These two event-control tables are not a second operation execution ledger and do not conflict with DL-002.
- **Verification:** Checkpoint and saved consolidated schema were read; no migrations executed or live concurrency validation performed for this documentation task.

### DL-017 — Snapshot authority is explicit; Raw manifest is conditionally optional
- **Status:** Accepted direction for explicit scope/completeness; exact absence-ordering contract needs resolution (Q-006) · Recorded 2026-10-09
- **Decision:** A refresh mode flag alone does not authorize deletion by absence. Require complete declared input, validated scope and successful expected child receipts. A Raw manifest may be replaced by a complete direct path list plus required metadata; no prefix-listing guess of completeness. Empty incremental input never means delete existing events.
- **Why:** Missing source rows may mean a complete empty snapshot, a narrow source filter, an incomplete read, or a stale observation. These cases cannot be treated alike.
- **Contract conflict:** Fan-Out design requires verified source-consistent `SourceVersionCutoffUtc`; Harmonizer checkpoint describes scoped exclusions ordered by shared fenced capture tokens. Neither agreement nor live compatibility between these approaches has been established here. Do not substitute host `SnapshotAsOfUtc` for a source cutoff.
- **Verification:** Resolve Q-006/Q-007 and prove source query coverage, including `LegacyRetentionPredicate`, before claiming authoritative refresh integration.

### DL-018 — Historical querying is phase two; restoration creates new history
- **Status:** Accepted in discussion · Recorded 2026-10-09; original exact dates partly recovered from 2026-10-03 onward
- **Decision:** Keep Query Engine as a later analytical capability. Preserve artifacts/lineage now. Bringing forward an earlier version goes through Centralized API and new ingestion/publication rather than rewriting prior history or silently rolling SQL backward.
- **Why:** Prioritize operational processing while retaining a path to historical analysis and controlled restoration.
- **Constraints:** A current pointer cannot reconstruct arbitrary past state without retained commit history. Cross-month/source mixed revisions must be selectable. Historical artifact state is not evidence of what a user saw in a UI.
- **Verification:** Historical time semantics, publication index, restore import/version rules and cross-environment mapping remain open (Q-010).

### DL-019 — Consolidate only unapplied baseline migrations
- **Status:** Accepted in discussion; consolidation reported by 2026-10-06 checkpoint · Recorded 2026-10-09
- **Decision:** Combine the unapplied Harmonizer baseline into `0004` after Raw Fan-Out prerequisites, excluding superseded `0005/0006` baseline scripts from migration input. Already deployed databases require new forward migrations.
- **Why / alternative:** Reduce fragmented initial setup without rewriting a deployed database's migration history.
- **Constraints:** Verify actual deployment state and use the repository's PostgreSQL migration tooling. A bundled reference `0003` is not an instruction to recreate existing production prerequisites. Historical SQL Server/DACPAC use does not establish PostgreSQL deployment support.
- **Verification:** Saved baseline read; checkpoint reports schema-equivalence checks. Actual migration state and CI execution remain unverified.

### DL-020 — One repository, two living architecture documents, task-scoped Micro-RPI
- **Status:** Accepted working direction, 2026-10-08 · Recorded 2026-10-09
- **Decision:** Maintain one Platform Discovery and one shared Decision Log. File organization is amended by DL-023 to allow focused topic specifications alongside the overview. Use a task-scoped Micro-RPI document for Research, Plan, Implementation and Validation. Use reusable skills for established maintenance/setup procedures. Architecture issues discovered during implementation return to Discovery/Decision.
- **Why / alternative:** Decisions must survive changes of engineer or AI account. Avoid another governance hierarchy, mandatory ADR per minor choice, or speculative procedures before delivery.
- **Integration:** Level 1 is `docs/AI_RULES.md`; Level 2 `docs/standards/`; Level 3 `docs/components/`. Those levels are not the lifecycle stages Architecture/Implementation/Operations. Existing `.github/skills/` remains the skills location; it is not an Actions workflow directory.
- **Scope:** Keep present Core/Worker structure and meaningful component organization. Do not prebuild a Calculator skill before discovering Calculator behavior. Separate ADRs are optional when their rationale merits a standalone record.
- **Verification:** Recovered discussion; the existing governance archive was identified but not inspected in this task. No skills or repository rules were changed.

### DL-021 — Harmonizer scaling must include durable lifecycle backlog
- **Status:** Current documented implementation checkpoint, 2026-10-06 · Recorded 2026-10-09
- **Decision:** Use database lease/fence ownership per windfarm/source, bounded lifecycle passes and concurrent farms. Keep at least one Harmonizer worker replica so open-event continuation can run when SQS is empty.
- **Why / alternative:** Queue depth does not describe accepted-but-unfinished lifecycle work. The checkpoint replaced an earlier optimistic generation/advisory-lock proposal with serialized farm publication to simplify overlapping-event correctness.
- **Consequences:** Same-farm Outage publication throughput is serialized; different farms may run concurrently. Account for inbound plus lifecycle work when sizing connections and memory. Expired owners may upload candidates but cannot commit them.
- **Verification:** Actual KEDA/HPA configuration, multi-connection races, realistic memory/load and fairness remain unverified.

### DL-022 — Shared Outage refresh is receipt-only; authoritative absence needs source evidence
- **Status:** Observed in supplied aligned source delivery, 2026-10-11; runtime/integration validation pending. This is an evidence refinement, not a claim of a new company-approved decision.
- **Context:** Q-006 identified conflicting historical snapshot contracts based on source cutoff versus capture tokens. The latest FanOut.zip/Harmonizer.zip make the shared-manifest boundary explicit.
- **Observed behavior:** Shared RawSnapshotManifestV1 Outage completion is ReceiptOnly. Scope barriers wait for expected canonical receipts but do not remove omitted events. Authoritative reconciliation requires explicit verified SourceVersionCutoffUtc; the latest model preserves nullable cutoff metadata and the existing processor guards. Host observation time/extraction token do not replace source authority.
- **Why:** A delayed or partial refresh must not delete newer event state merely because it is omitted from a capture.
- **Scope:** No producer-provided cutoff was verified here. Production's supplied refresh uses removal reconciliation disabled. Package ownership remains DL-015. Existing canonical/transport/materialization/Raw ACK boundaries remain separate under Q-007.
- **Evidence:** Alignment READMEs and supplied OutageBatchProcessor/model in 11 October archives; query builder/adapter source. Latest alignment reports no .NET build/test execution; none rerun for this documentation task.
- **Follow-up:** Q-006 is narrowed at the supplied-source design level, not closed end to end. Prove producer authority, delayed/empty/overlapping snapshots and later event versions before enabling absence removal.

### DL-023 — Discovery overview plus one specification per responsibility
- **Status:** Accepted in discussion · 2026-10-11; user requested generation of the proposed topic arrangement.
- **Decision:** Keep one Platform Discovery overview and one shared Decision Log. Add seven focused Markdown specifications plus README under `docs/architecture/discovery/loss-calculation/`: Production source, Outage source, Raw ingestion, Raw Fan-Out, Harmonizer, Harmonizer event management and Operation tracking.
- **Why:** Developers can read/update one responsibility with its related queries, keys, relationships and flow. Event management is substantial enough for a dedicated file; a separate file per table would fragment the processing contract.
- **Boundary:** Specifications describe current design/behavior and open questions. Decision Log preserves rationale/history. Executable queries remain in source adapters/QueryBuilders; specifications contain contract explanations and selected excerpts. Existing standards, Micro-RPI, Core/Worker projects and skills hierarchy remain unchanged.
- **Path choice:** Reuse the existing `docs/architecture/discovery/` root rather than introduce a parallel `docs/discovery/` tree. [Topic index](discovery/loss-calculation/README.md).
- **Amends:** DL-020's literal one-Discovery-document arrangement; retains its one repository, shared rationale, task-scoped Micro-RPI and portable handoff intent.
- **Verification:** Topic documents generated and locally checked for internal links and evidence labeling. No repository commit, application tests, migration or deployment performed.

### DL-024 — Adjustment complete-aggregate history and leased outbox: recovered implementation evidence
- **Status:** Source-backed saved implementation proposal/checkpoint, 2026-09-29; recovered and documented 2026-10-11. Not a new approval of all package choices, nor proof of active deployment.
- **Sources:** `Adjustment-History-Outbox-SQL.zip` README, PostgreSQL schema/capture/outbox routines and VALIDATION.md; older `05-adjustment-publisher.md`. [Adjustment specification](discovery/loss-calculation/adjustment.md).
- **Observed design:** One numeric AdjustmentId has complete zero-to-many WTG membership within one farm. UTC half-open applicability permits null/open end. Current child links are replaceable; each accepted parent/member change creates an immutable complete aggregate revision and outbox row in the same explicit API transaction under expected-revision locking. Per-row child triggers are not this package's capture mechanism.
- **Observed keys:** Current parent PK id → current membership child FK (cascade delete). Retained change_stream(adjustment_id) → revision PK(adjustment_id,revision); unique MessageId and revision triple → outbox composite FK. Supporting history/outbox rows deliberately do not FK to the deletable current parent. Real catalog FKs/view binding require project integration.
- **Version/deletion behavior:** Stable MessageId for the same logical request; revision orders updates. Baseline existing parents before first replacement/delete. Delete retains higher tombstone revision, previous membership and parent lineage; never reuse parent identity. Empty membership remains valid Upsert. Older Upsert must not resurrect a newer Delete; equal-revision conflicting content is an error.
- **Outbox behavior:** Pending/Processing/Published/Suspended; atomic bounded claims, renewable UUID lease, fenced completion/failure, retry budget including crashes, audited redrive preserving UUID/revision/payload. Published is Raw/queue handoff evidence, not calculation success. Read immutable versions, never mutable latest state for an old pending revision. At-least-once duplicate/out-of-order processing remains intentional.
- **History:** Package keeps Azure SQL parent system versioning while authoritative and imports old parent periods separately with membership unknown. PostgreSQL new history is explicit full aggregate revisions; recorded timestamps are not SQL Server engine transaction-time equivalence. This refines earlier generic trigger-history discussion for this saved Adjustment package only.
- **Downstream:** Adjustment parent may cover multiple WTGs. Harmonizer reconciles old materialized memberships/dates plus new applicability and unfinished cleanup, preserving unrelated adjustments. Lifecycle location follows DL-014, packages DL-015. Actual Adjustment adapter/readiness remains Q-009/Q-015.
- **Conflict register:** Older plan's Active/Cancelled/value fields differ from supplied Upsert/Delete/current-parent DDL. SQL package's direct Raw exporter differs from later DL-004 dispatcher/ingress baseline. Package has no root OperationId despite its Upsert/Delete `operation` field. Do not silently change DL-004 or invent migrations to hide these differences; resolve Q-015 against actual code/requirements.
- **Verification limits:** Saved package reports parser plus single-session embedded PostgreSQL behavioral checks; no SQL Server, real multi-connection concurrency, API/catalog, Blob/SQS, RDS/Azure SQL or downstream verification. No tests/migrations were rerun for this documentation update.
- **Working organization:** Extends DL-023's topic list with `adjustment.md`; preserves one overview and one Decision Log. It records recovered detail rather than declaring new runtime components completed.

### DL-025 — Confirmed company cloud, Kafka, Data Lake and database constraints
- **Status:** Confirmed by user, 2026-10-11; reported company context, not independently reviewed corporate policy.
- **Context:** Company is moving application services away from Azure towards AWS. Existing Kafka is running and encouraged for cloud-agnostic integration; the team can request an AD group authorized to use a specific topic.
- **Data Lake:** Application is a customer of a company Data Lake service, currently backed by Azure Blob. The service is expected to migrate to AWS in 2027. The customer should be abstracted from the service provider's storage migration. Exact destination/access/reference/query contracts remain unverified; this does not promise zero adapter/configuration work.
- **Database:** PostgreSQL is the company-selected product, obtained only through the company product catalogue. The team cannot create its own instance. Exact product/version/RDS-versus-Aurora/features/ownership must come from its contract.
- **Consequences:** Do not justify SQS by assuming the team must deploy Kafka, or PostgreSQL by invented price/performance superiority. Do not assign company lake data migration to the application. AWS direction does not forbid an existing cloud-agnostic shared service.
- **Links:** [ADR-0001](adr/ADR-0001-aws-direction-and-data-lake-boundary.md), [ADR-0006](adr/ADR-0006-catalogue-postgresql.md). These confirm constraints without silently changing DL-010 or provider code.

### DL-026 — ADR drafts for messaging, runtime, artifacts and query providers
- **Status:** Proposed rationale / existing directions documented, 2026-10-11. No new SQS exception, engine selection or code/deployment change authorized by these drafts.
- **Artifacts:** [ADR index](adr/adr-index.md) links six focused ADRs: cloud/Data Lake boundary, SQS versus company Kafka, EKS runtime, Parquet artifacts, query-provider abstraction and catalogue PostgreSQL.
- **Messaging:** Current SQS topology remains DL-010 while the supplied company Kafka alternative is assessed. Kafka may meet work-delivery needs; availability/cloud-agnostic preference is a real advantage. Compare retry/offset/ACK, scaling/rebalance, replay, supported client conventions and actual costs before definitive rejection.
- **Runtime:** EKS/C# application direction remains; ADF orchestration, Synapse execution and Redshift warehouse processing are different alternatives requiring full behavior comparisons. Do not declare Kubernetes the only AWS option or claim unmeasured cost superiority.
- **Storage/query:** Parquet separates business artifact representation from engine execution. Proposed provider boundary isolates dialect/access/credential/execution mapping. Synapse Serverless, Athena and DuckDB are candidates with different storage/capability requirements, not interchangeable endpoints or three deployed providers. Latest PostgreSQL serving stays separate from historical file queries.
- **Evidence:** User clarification plus current saved Discovery/log and primary public documentation cited within ADRs. Application interfaces/diagram are illustrative design; no compiled code, live catalogue, Kafka proof of concept, cost benchmark or end-to-end provider/migration test was supplied.
- **Next checks:** Q-016–Q-019. Retain ADR status vocabulary; accepted company constraints do not establish acceptance of every proposed adapter/runtime detail.

## 4. Open questions / proposed decisions (NOT ACCEPTED)

| ID | Question | Why it matters | Owner / evidence needed |
|---|---|---|---|
| Q-001 | What is the exact queue name for Harmonizer in each environment? | Conflicting historic labels. | Verify infra / worker appsettings; capture in change PR. |
| Q-002 | What is the final extraction cursor key and checkpoint schema across source types? | Earlier designs iterated on source system ID, timestamps and numeric ID. | Compare latest migration + production ingestor code. |
| Q-003 | Which specific component-level states are persisted today, and what publisher journal evidence exists? | Operation dashboard must reflect real work, not invented progress. | Inspect actual control tables and publisher implementation. |
| Q-004 | Which additional decisions need standalone ADRs beyond the six current drafts? | Six focused ADRs now cover the requested technology/service choices; acceptance/evidence remain separate. | See DL-026 and ADR index; add further records only for concrete substantial choices. |
| Q-005 | Which architecture claims have end-to-end test evidence? | “Agreed” ≠ “implemented” ≠ “validated.” | Attach test/PR evidence incrementally. |
| Q-006 | Is source snapshot authority proven end to end? DL-022 narrows supplied code to ReceiptOnly shared refresh and verified source cutoff for authoritative absence. | Latest source design is explicit; no producer cutoff or delayed-snapshot runtime proof is available. | Verify source consistency, actual cutoff producer contract and delayed/empty/overlapping snapshot tests. |
| Q-007 | When do canonical receipt, SQS deletion, materialization completion, barrier completion and Raw ACK occur? | Fan-Out document and Harmonizer checkpoint describe different receipt boundaries. | Agree transaction boundaries; test failed children, long lifecycle work and crash recovery without circular waits. |
| Q-008 | What is the real Package Coordinator notification/readiness contract and package revision policy? | Existing checkpoint delegates packages and dispatch but reports integration pending. | Inspect actual consumer and demonstrate pending/absent Outage plus superseding source revisions. |
| Q-009 | What exact contracts and fixtures govern Alarm, Curtailment and Adjustment adapters? | Do not generalize Outage schema or lose Adjustment child removals. | Source contracts, representative fixtures and integration tests. |
| Q-010 | What does historical “as-of” mean, and what retained publication evidence supports it? | Current pointers and lineage alone may not identify a past committed view. | Define event/effective/publication time and commit history; phase-two Query Engine / restore design. |
| Q-011 | Which proposed component merges and final queue/deployment boundaries are accepted? | Lifecycle is inside Harmonizer; broader Publisher/Package merges are still proposals. | Review existing deployments; record only a concrete delivery-driven choice. |
| Q-012 | What retention period applies to dispatch, failure, orphan and historical artifacts? | Six months and one month were discussed; lineage and replay have retention dependencies. | Agree recoverability requirements before cleanup implementation. |
| Q-013 | What are the exact operation/Ingestor DDL, ExecutionId mapping and shared Production V2 additive metadata definitions? | Monitoring queries and build compatibility cannot be inferred from isolated supplied files. | Inspect active migrations, shared contracts and project references; demonstrate upstream-only and complete-operation monitoring. |
| Q-014 | Which source query boundaries are intentional and how do cursors avoid skips? | Production routes differ on updated-time inclusivity; Outage refresh source uses inclusive end overlap while aligned Fan-Out uses half-open overlap. | Verify runner route, equal-timestamp paging, retention-floor semantics, late commits and exact interval boundaries. |
| Q-015 | How do Adjustment history/outbox details integrate with the current API/ingress and event contracts? | Saved sources differ on cancellation versus Delete, direct Raw export versus dedicated ingress, business fields, source schema/readiness and effective-dated membership; SQL package lacks root OperationId. | Inspect actual Adjustment tables/API/dispatcher; choose write/delete semantics and deployment owner; define durable correlation, versioned Raw/member mapping, baseline and readiness; prove cleanup and crash recovery. |
| Q-016 | Does SQS materially improve delivery/recovery over the available company Kafka, and is its use acceptable under the Kafka preference? | Kafka is running/encouraged; broker-provisioning arguments do not apply. | Obtain topic/auth/client/retry support contract; compare representative crash/rebalance/poison-work scenarios and actual adoption/cost evidence; record accepted choice/exception. |
| Q-017 | What Data Lake customer contract shields access and artifact references during its 2027 AWS migration? | Current code uses Blob adapters; physical URLs/historical references and query access may not remain stable automatically. | Provider contract for reads/writes, stable identity, cutover consistency, credentials and engine-readable/query bindings; replay pre-migration references after cutover. |
| Q-018 | Which query provider/access binding is supported before and after lake migration? | Synapse/Athena/DuckDB differ in dialect, storage access, execution and committed revision binding. | Actual service capability, query subset, exact snapshot binding, type/UTC equivalence and performance/permissions evidence; no engine selected merely by endpoint swapping. |
| Q-019 | What are the exact PostgreSQL catalogue product/version/features and customer/provider responsibilities? | Company mandates catalogue PostgreSQL; independent provisioning is unavailable. | Product profile, provisioning/roles, authentication/network, extensions, backups/HA, connection limits and migration support; do not infer RDS/Aurora. |

## 5. Working rules for humans and AI assistants

- Treat **accepted decisions** as the current discussion baseline, **subject to repo verification**. Do not silently re-propose previously rejected complexity (e.g., a second operation-work table) without documenting new evidence and rationale.
- Before coding, identify which DL entries apply to the task. Cite their IDs in the task/PR description when appropriate.
- During each Discovery / Micro-RPI / Implementation / Validation cycle, **propose a small log update** if a decision changes. A human reviewer confirms it; AI-generated prose does not itself authorize a design change.
- Log *why* a decision was made and *what was ruled out*, not merely the winning implementation.
- Keep credentials, customer data, internal URLs, and sensitive infrastructure details out of any log intended for external publication.
- If you are another AI account with only this file, do **not** claim you have access to the originating chats; ask for only the particular missing repository evidence relevant to the task.

## 6. Example future entry template

```markdown
### DL-027 — <Short decision title>
- **Status:** Proposed | Accepted in discussion | Implemented | Validated | Superseded
- **Date / decision maker:** YYYY-MM-DD · <role/name if appropriate>
- **Context / problem:** ...
- **Options considered:** A / B / C, with key trade-offs.
- **Decision:** ...
- **Why:** ...
- **Consequences / affected components:** ...
- **Verification:** PR / test / migration / observed runtime evidence; or "not yet verified".
- **Supersedes / superseded by:** ...
```

## 7. Handoff prompt for a new engineer or AI account

> Read `docs/architecture/decision-log.md` first. Summarize the currently accepted decisions relevant to the task, then inspect the actual code and migrations to distinguish design intent from implemented behavior. Respect the recorded constraints, identify conflicts with older documents, and propose any new/changed decision as an update to this log before making architectural changes. Do not assume access to the original architect's private AI conversations.

---

**Repository handoff:** add the companion Discovery and commit this updated file at `docs/architecture/decision-log.md` in a reviewed PR. That Git commit—not the private conversation—is the durable team-shared record. Update it alongside future architecture PRs.

## 8. Evidence register and revision history

| Source | What was inspected / limits |
|---|---|
| Initial `loss-calculation-decision-log.md`, 2026-10-08 | Read in full; original IDs retained. This edition corrects shorthand that conflicted with current source documents. |
| `Harmonizer_Event_Lifecycle_Implementation_Plan.md`, version 3, updated 2026-10-06 | Read in full. Reports delivered Outage lifecycle behavior, 151 passing unit tests, 18 skipped Npgsql integration tests and embedded PostgreSQL smoke checks; not rerun here. Real provider/concurrency/load and Package Coordinator integration remain open. |
| `2026-10-06-outage-raw-fanout-v3-design(1).md` | Read in full. Design document supplies producer field mappings and stricter source-cutoff/receipt requirements; not proof of deployed behavior. |
| `0004_create_harmonizer_schema.sql`, version 1 | Read; not executed. Contains event control/readiness schema in the saved baseline. |
| 2026-10-11 source review | Saved Production/Outage source adapters and query builder; 0003/0004 CREATE TABLE definitions; aligned Fan-Out/Harmonizer archive source and READMEs. Read/inspected only; application builds/tests, SQL execution and live deployment not performed. |
| Recovered design discussions through 2026-10-08 | Supplemented operation-table, membership, phase-two scope and engineering-process rationale. Exact earlier decision dates remain unknown where not recorded. |

**2026-10-08:** Initial DL-001–DL-012 and Q-001–Q-005 compilation.  
**2026-10-09:** Added companion Discovery; preserved prior IDs and rationale; superseded the universal WTG-day and generic snapshot-pointer interpretations; added DL-013–DL-021 and Q-006–Q-012. Highlighted source-document disagreements instead of silently resolving them. No code, database, deployment or governance skill was modified.

The saved documents are a portable baseline for review and implementation, not a substitute for the actual repository. Add PR, test and runtime evidence to individual entries as work progresses.


**2026-10-11:** Added seven topic specifications and README, linked them from Platform Discovery, and recorded DL-022 supplied-source snapshot refinement and DL-023 documentation arrangement. Preserved prior rationale/IDs and open questions; added Q-013/Q-014. Inspected saved source adapters, 0003/0004 DDL and aligned Fan-Out/Harmonizer deliveries. No live repository, runtime, source database or deployed environment was inspected; no application build/tests or migration executed.


**2026-10-11 Adjustment addition:** Added eighth topic specification `adjustment.md`, recovered publisher/history/outbox sources, preserved earlier decisions and recorded DL-024/Q-015. Linked exact saved table relationships, API capture/lease/replay/delete/history behavior and unresolved dispatcher/correlation/cancellation integration. Updated Discovery/index/related specifications; no application implementation, tests, migration or deployment performed.


**2026-10-11 ADR addition:** User confirmed existing encouraged Kafka with topic-scoped AD-group access, Data Lake service abstraction and 2027 backing-store move to AWS, and company catalogue-only PostgreSQL. Added six ADR drafts/index, DL-025/DL-026 and Q-016–Q-019. Preserved SQS/platform direction pending comparative review. No implementation, broker/database provision, service migration, application test or deployment performed.
