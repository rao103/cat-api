# Harmonizer event lifecycle — implemented checkpoint

6 October 2026. This document replaces the earlier proposal with the delivered design and its remaining integration requirements. Implementation is restricted to Harmonizer Core/Worker and an additive copy of the supplied EventWorkV3 contract. ProductionItemDayWorkV2 and V3 remain supported.

## Decisions carried into implementation

- OutageClassificationId is canonical identity within a numeric windfarm ID. OutageId is an ordinary descriptive field.
- Open OutageClassificationEndUTC remains null. Interval overlap treats it as unbounded; output segments retain the original null end.
- LastUpdatedDate orders event changes. Newer tombstones delete; stale live data cannot resurrect them. Equal-version conflicting business content is an error.
- No active known event means no outage. There is no independent upstream completeness gate. Accepted pending work, failed reads and source conflicts still prevent false empty success.
- Business data, event history and lineage remain in files. Only ownership, current pointers, compact progress and existing receipts/outbox use PostgreSQL. Two event control tables are added.
- Known source fields are ordinary typed Parquet columns. Unknown descriptive fields are preserved in optional ExtraAttributesJson until a typed mapping is added; identity/interval/lifecycle rules do not depend on those descriptions.
- Event Lifecycle Coordinator runs inside the same Worker and shares its appsettings.json, telemetry providers and provider clients. Package Coordinator owns packages and calculation dispatch.
- Curtailment is a ten-minute WTG-day source, following the Production approach. Outage is implemented here; Alarm/Adjustment require their real contracts/fixtures before adding adapters.

## Folder organization and abstractions

The original folder structure is retained. Source-specific models, ports, adapters, repositories, serialization and operations live beneath Core/SourceTypes/Production and Core/SourceTypes/Outage. Shared queue/storage/telemetry/strategy ports remain in Core/Interfaces. Generic dispatch and heartbeat remain in Core/Operations. Worker contains Hosting, DependencyInjection, Observability and SourceTypes registration folders.

IHarmonizationHandler selects inbound source behavior. IEventLifecycleHandler supplies autonomous source polling. Generic hosted loops do not depend on concrete Production/Outage processors. Adding a source requires its adapter/use case and registration, rather than editing every shared worker service.

Operations depend on meaningful ports: IRawObjectStore, IHarmonizedArtifactStore, IOutageArtifacts, IOutageControlRepository and IOutageIdentityCatalog. Azure Blob, SQS, RDS/Npgsql and notification connections stay behind adapters/repositories and composition code. Runtime code never executes DDL.

Models are sealed reference records/classes. Appropriate numeric IDs, UTC timestamps, DateOnly and counters remain CLR value types. Pure mapping, interval, range and merge helpers have no artificial interfaces or logger dependencies. Async boundaries use cancellation and bounded work; immutable paths and database fences make publication reviewable.

## Verified incoming work

EventWorkV3 is copied byte-for-byte from the latest supplied Raw Fan-Out implementation. Supported Outage work kinds are CanonicalEvents, EventSnapshotReconciliation and empty incremental ArtifactObserved. The message supplies exact event/member keys, farm ID/name, WTG ID/name, contributing Raw paths, extraction/run/attempt identity, optional operation/execution/trace metadata and frozen snapshot scope/expected batches. Harmonizer calculates affected days from canonical old/new state.

The reader validates the actual contract's deterministic hashes, duplicate fields, identities, raw-path membership, payload/mode and UTC scope. It retains the original body unchanged for frozen dispatch-plan matching. Unsupported source/schema work is not acknowledged as empty.

The supplied normalized Parquet fixtures use EventId, WindfarmId, WtgId, OriginalStartUtc, nullable OriginalEndUtc, LastUpdatedDate, IsTombstone, SourceWatermarkUtc, EventType, Status, SourceRevision and descriptive fields. The fixture IDs are string names; numeric INT32 IDs are also supported. They resolve through the authoritative catalog. SourceWatermarkUtc is capture evidence in origins, not an equal-version business-content field. INT96 and output LastUpdatedTicks preserve source precision.

Identity SQL joins extraction_cursor to windfarm and wind_turbine. It uses cursor.windfarm_id as the integer farm ID, farm.windfarm_name as text, turbine.id AS wtgid as the integer turbine ID and turbine.wtg_name as text. The Outage catalog filters cursor.source_type='Outage'. Names are never cast to numeric IDs.

## Storage paths and WTG-day linkage

| Artifact | Path |
|---|---|
| Monthly canonical Outage | harmonized/events/outage/windfarm=WF01/startyear=2026/startmonth=07/revisions/revision=7/attempts/token=42/outage-events.parquet |
| Monthly merge state/provenance | snapshot.json and lineage.json beside that Parquet |
| Delete-first canonical state | harmonized/events/outage/windfarm=WF01/deleted-events/revisions/revision=1/attempts/token=43/deleted-events.parquet |
| Committed source catalog candidate | harmonized/events/outage/windfarm=WF01/_catalog/revisions/revision=9/attempts/token=55/manifest.json |
| WTG-day Outage | harmonized/wtg/windfarm=WF01/wtg=T001/date=2026-07-18/outage/revision=5/attempts/token=44/outage-segments.parquet |
| Daily merge state/provenance | snapshot.json and lineage.json beside that daily Parquet |
| Durable progress plan | harmonized/lifecycle/outage/windfarm=WF01/event={encoded-work-identity}/generation=4/attempts/token=55/progress={content-hash}/plan.json |
| Package manifest | Separate Package Coordinator's packages/revision=N/manifest.json |

Each nonempty daily row carries classification/EventId, EventRevision, CanonicalStartYear/Month, CanonicalMonthRevision, CanonicalCommittedAttempt and exact CanonicalEventPath. A query reads that immutable file and selects that classification/revision. It need not join an event business table or infer the latest month. A month move publishes both old and new month pointers atomically; old daily links remain readable.

Snapshot is internal normalized merge state; lineage records contributing canonical and Raw paths, operations/executions, extraction attempts and trigger evidence. Parquet is the typed analytical/display data. Empty cleanup still produces zero-row Parquet and lineage referring to the tombstone that caused removal. These artifacts do not establish what an end user actually saw in a UI.

Revision identifies a logical publication; attempt token identifies its fenced owner. Concurrent or expired owners can propose the same next revision, so revision alone cannot isolate their uploaded candidates. All files are immutable, and only committed database pointers are authoritative. The progress hash distinguishes successive checkpoints within one lease. Orphan collection/retention is future operational work.

## Canonical mutation and materialization

1. Register exact frozen batch identity/body and expected members in existing control tables.
2. Decode/validate declared Raw files. A snapshot barrier waits for successful canonical receipts from every expected child batch.
3. Claim a fenced farm/source lease, load the committed catalog and explicitly referenced monthly snapshots, then merge each classification by LastUpdatedDate.
4. For a mutation, plan the union of previous materialized footprint, outstanding cleanup and new applicability through the processable cutoff. Compact ranges avoid enumerating years of dates up front.
5. Upload changed months, plan candidates and the new catalog. One transaction publishes their pointers plus canonical receipts and pending ranges. A WTG/month move cannot publish only half the change.
6. Process at most DaysPerPass dates per pass. Each complete WTG-day includes all known overlapping events, preserving unrelated contributions. Day pointer, outbox intent and next progress checkpoint commit together behind the same fence.
7. Once canonical state and resumable plans are durable, delete the SQS delivery. This is accepted handoff, not final materialization acknowledgment. Background polling continues long jobs without repeated source redelivery.
8. Final batch completion and raw_artifact_ack commit together only after all frozen targets finish. A completion sweep recovers a crash between the last day and acknowledgment.

Shortening/deletion/movement cleans old dates, including explicit empty output. Delete-first records preserve their version barrier in deleted-events without using placeholder timestamps as a month. A newer legitimate live source version can replace a tombstone.

Default processable cutoff is yesterday UTC. Open and future-ended events continue when later dates become processable. Clock work starts another bounded generation, preserving event origins but using an untracked autonomous root operation. It does not change the event revision or keep a completed source operation open indefinitely. New source mutations retain unfinished old cleanup.

## Snapshot absence and capture ordering

Snapshot absence applies only to its declared WTG/UTC interval, never as a global tombstone. An omitted event could have moved outside that interval. Scoped exclusions target an exact event revision; newer business state restores applicability. A later snapshot can also restore presence at an unchanged business version.

Newer positive and absence observations protect overlapping scope from delayed older barriers. Capture ordering assumes refresh and incremental use the same monotonic fenced ExtractionToken namespace for the same (windfarm, Outage) extraction_cursor, as in the supplied producer. Tokens protect capture ordering only; LastUpdatedDate remains event version authority. An independent future refresh token namespace requires an explicit comparable capture sequence/watermark.

SourceWatermarkUtc supplies conservative source-version evidence for nonempty snapshots. Empty snapshots use their frozen scope, extraction token and receipt barrier. Empty incremental observations acknowledge observation without removing known events. Decoding/provider/validation failures remain failures.

## Database footprint and readiness

| Table | Purpose/change |
|---|---|
| control.wtg_day_input_state | Existing committed daily component pointers and status |
| control.harmonizer_batch | Existing frozen work/completion; adds event kind, farm/source, canonical receipt time, expected members/batches and error |
| control.harmonizer_batch_item | Existing receipts; adds typed event/scope/observation membership and materialization generation/time |
| control.harmonizer_outbox | Existing atomic coordination intents; not an SQS queue |
| control.event_partition_state | New current catalog/month pointers and farm/source lease/fence |
| control.event_materialization_state | New current event/work locator, plan pointer/generation, compact pending ranges, continuation/due/error |

No event_head, event_revision, event history, per-day lifecycle-job or independent completeness table is added. Actual canonical attributes and historical revisions remain in files.

The consolidated 0004 fresh-install baseline includes control.harmonizer_event_day_pending(farm,wtg,date,source) and control.harmonizer_input_readiness. Accepted unresolved canonical work, a registered waiting snapshot scope, and pending ranges block relevant readiness. An older Complete pointer remains readable history while its new rebuild is Pending.

Package Coordinator must call the pending predicate even when no current Outage component row exists. When it is false and no active known event/component exists, absence is the agreed empty Outage result. Do not require five physical Complete rows for that rule; unsupported or other sources retain their own readiness policy. The worker does not generate packages or direct CalculationWork.

Use the consolidated 0004 only as a fresh-install baseline after the Raw Fan-Out prerequisites and only where Harmonizer tables do not exist. Existing installations require reviewed forward migrations; do not apply the baseline over an installed schema. Never rerun the bundled 0003 reference schema in a real database. Deploy the consumer before routing EventWorkV3; older frozen Outage V2 work needs an explicit compatibility/redrive decision. Keep snapshots in ReceiptOnly mode until the producer supplies a verified source-version cutoff.

## HPA, ownership and fairness

Every pod may run both inbound and lifecycle loops. PostgreSQL permits one Outage publication owner per farm/source, while different farms run concurrently. The owner retains the lease through a bounded pass, heartbeats it and fences every canonical/day/progress commit by token, owner and database expiry. An expired pod can upload an uncommitted candidate but cannot replace committed pointers.

This delivered design intentionally uses serialized farm publication rather than the earlier optimistic generation/advisory-lock proposal. It simplifies overlapping-event correctness at the cost of one farm's concurrent Outage throughput. DaysPerPass limits each pass; ConcurrentFarms limits per-pod autonomous work. Source and lifecycle loops compete for the same durable lease, so there is no separate in-memory lock requirement.

Keep at least one worker replica running, including KEDA minReplicaCount=1. Queue depth alone misses ongoing-event/lifecycle backlog. Account for inbound plus lifecycle concurrency across replicas when sizing the Npgsql pool and memory. MaximumPartitions, raw byte/row/path bounds and sequential bounded passes constrain each operation; very large farm catalogs still require load validation.

## Telemetry and shared configuration

One appsettings.json contains Harmonizer.EventLifecycle and the existing Telemetry/Logging sections. Host-owned providers emit UTC JSON Timestamp values and OpenTelemetry logs, traces and metrics. OTLP is optional; endpoint/protocol/secret headers use OTEL_EXPORTER_OTLP_* environment variables.

ITelemetry is injected at necessary runtime boundaries: input reader, storage adapter, identity/control repositories, canonical reader, batch processor, lifecycle processor/handler, generic hosted loops and ownership heartbeat. Pure records/mappers/merge/interval/range rules require no logger. Log/trace scopes include operation/execution, source, numeric IDs/text names, batch/artifact, paths, date, planning generation and fence token where relevant. Metric labels use bounded source/trigger/outcome values. Credentials and full bodies are not logged. OperationId is movement correlation, not a W3C TraceId.

## Validation and remaining acceptance

The current monorepo check completed locked restores, a fresh Release Worker build and 155 passing unit tests. All 18 Npgsql integration tests were skipped because `HARMONIZER_TEST_POSTGRES` was unset. One inherited warning remains in the unchanged RawArtifactAvailableV1 contract. The prior standalone delivery reported an embedded PostgreSQL smoke run over repository SQL; it was not rerun in this checkout and does not replace real Npgsql or multi-connection race tests. See Validation.md for current evidence.

The project includes actual Outage Parquet fixtures and tests for typed/null/precision output, exact linkage, cleanup, bounded recovery, supersession, clock continuation, barriers, delayed scopes, source errors and empty observations. See Validation.md for evidence and limits.

Before deployment, execute integration tests against a disposable PostgreSQL instance, confirm the common capture token namespace, connect the real Package Coordinator readiness/notification contract, and validate SQS/Blob/RDS permissions and HPA/load behavior. Harmonizer publishing stays disabled in the sample configuration until the real proposed coordination consumer is aligned. Central operation-status writes, retention, DLQ tooling and other source adapters remain separate work.
