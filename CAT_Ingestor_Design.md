# CAT Ingestor Design

**Design date:** 24 September 2026  
**Status:** Agreed architecture, with implementation requirements identified below. This document does not claim that the repository already implements the changes.  
**Companion:** `Final_Distributed_Loss_Calculation_Architecture_Plan_v12.md`, ingestion revision dated 24 September 2026.

## 1. Decisions

The ingestor remains a small, source-specific EKS worker. One pod holds valid ownership of one `(source_type, windfarm_id)` at a time. Inside that pod, large source-update ranges are split into calendar-month windows and processed with bounded concurrency.

| Concern | Decision |
| --- | --- |
| Ingestion tables | Retain `extraction_cursor` and `extraction_run` |
| Cursor identity | Retain `(source_type, windfarm_id)`; no environment column |
| Ownership | One valid farm/source lease; internal tasks share the execution's authority |
| Parallelism | Multiple monthly windows inside the owning pod; no distributed month workers |
| Work tracking | In-memory window list and task outcomes; no `extraction_work` table |
| Coordination | Ordinary application service inside the ingestor; no new coordinator deployment |
| Production incremental Raw | Preserve the source rows returned by the bounded `LastUpdatedDate` queries |
| Raw placement | Parquet files directly beneath `extraction=<LeaseToken>/` |
| Filename | `window-{start}-to-{end}-part-{number}.parquet` |
| Checkpoint | One fenced whole-execution commit after every window's Raw and queue handoff succeeds |
| Crash recovery | Replay from the previous committed checkpoint under a new extraction token |
| Audit | `extraction_run` records attempts; it is not a work queue or the authority for claims/restarts |
| Downstream | Fan-out discovers Item-Days from rows; changed-row Production inputs require keyed merge |

The existing Raw Fan-out Coordinator, Calculation Package Coordinator, Adjustment outbox and downstream control tables remain. Avoiding a new ingestion coordinator does not remove these existing responsibilities.

## 2. Source-update time and production time

`LastUpdatedDate` answers when a source row changed. `ProductionTimestamp` answers which production period the row describes.

For example, the query `[2026-09-23T00:00:00Z, 2026-09-24T00:00:00Z)` can return production records for March 2025, January 2026 and September 2026. A daily execution queries roughly one day of changes, not necessarily one day of production. After an outage, the source-update range may cover many months.

The cursor must represent source-update progress. It must not become the minimum/maximum production date seen in a response. Those production dates are audit statistics; their extent does not prove continuous coverage. Empty results have null production-date bounds.

Monthly windows partition the query's `LastUpdatedDate` axis. A one-day update query remains one window even if its returned production records span a year. Fan-out and Harmonization provide downstream parallelism for those historical dates.

## 3. Registration, scheduling and execution modes

Administrators seed one cursor registration per source type and wind farm with an explicit starting checkpoint. Seeding must not reset an existing checkpoint or fencing-token sequence. Scheduled pods select enabled, due registrations from RDS; a normal CronJob does not supply a wind-farm ID.

A pod owns at most one farm/source at a time. The existing finite claim-process-repeat loop may subsequently claim another farm after releasing the first. Configure `MaxClaimsPerInvocation = 1` if a Job must handle exactly one farm. Neither interpretation permits two valid owners for the same cursor row.

| Mode | Query scope | Incremental cursor and schedule |
| --- | --- | --- |
| Scheduled incremental | Committed checkpoint, configured overlap and frozen source-update upper boundary | Advance only after whole-execution success; update incremental outcome/eligibility |
| Historical refresh | Explicit selected production dates from existing `REFRESH_*` request | Preserve incremental checkpoint and interval/backoff state |
| Explicit replay of an old update window, if exposed later | Validated caller-specified update bounds | Preserve normal incremental checkpoint; a replay is not cursor rollback |

RunNow and new all-farm/replay endpoints are not introduced by this document. Repeated requests for the same supported refresh range are allowed and produce distinct attempts and artifacts. Requests respect the same lease; an overlapping refresh reports Busy/unprocessed rather than claiming success. Do not add a durable pending-request queue as part of this change.

Retain the existing refresh variables: `REFRESH_WINDFARM_ID`, `REFRESH_DATA_DATES`, `REFRESH_RUN_ID`, `REFRESH_REQUESTED_BY`, `REFRESH_REASON`. Partial or invalid refresh inputs fail validation instead of falling back to scheduled fleet ingestion.

Scheduled eligibility remains derived from incremental outcome/completion and the configured ingestion interval or failure backoff. An active lease blocks selection; expired ownership is recoverable. No persisted `next_eligible_utc` is added. Schedule the worker often enough to meet the at-least-daily extraction requirement, including runtime and retry opportunities. A 24-hour interval plus an exactly daily trigger can skip a day if the previous execution finished after the next trigger's time.

## 4. Responsibilities of the two tables

### 4.1 `extraction_cursor`

Retain the current schema and map these responsibilities to existing columns:

- Source type, wind farm and enabled registration.
- Adapter-specific committed checkpoint; for Production, the existing timestamp/ID pair.
- Lease owner, monotonically increasing lease token and expiry.
- Active attempt correlation where already present.
- Incremental start/completion/outcome, failure count and sanitized error information used for eligibility.

It does not store Parquet counts, per-window progress or a list of tasks. Production min/max dates, if retained for diagnostics, never drive discovery, claims or checkpoints.

### 4.2 `extraction_run`

One audit row per attempt, distinct from an optional request correlation ID. Record source/farm, mode, Job/pod, attempt ID, extraction token, timing, outcome, requested scope, query boundaries, from/to checkpoint, rows/files/bytes published, observed production-date extent and sanitized diagnostics.

Artifact references can use existing audit fields or a reference to an immutable audit manifest if the list is large. Such a manifest is audit evidence, not a new scheduler or operational task ledger. Exact window boundaries are also retained in every Parquet file's metadata.

The runtime does not choose pending work from run history, resume individual windows by interpreting audit rows, or infer completeness from the number of run entries. Recording audit status alongside the fenced final commit is valid; that does not make the audit table the operational authority.

## 5. Claim, lease and lifecycle

1. Validate configuration, source routing, credentials references and execution mode.
2. Atomically claim the due cursor in a short RDS transaction. Assign owner, increment token, set expiry and create/correlate the audit attempt. The refresh branch claims only its selected registration.
3. Commit the transaction before source, object-storage or queue I/O.
4. Start one lease-renewal loop for the farm execution, independent of its window tasks.
5. Capture the original checkpoint and a finite source-update upper boundary, then build the in-memory window plan.
6. Execute windows with a configured concurrency limit. Observe all tasks and propagate cancellation.
7. Join all required tasks. Success requires completed enumeration and accepted publication for every emitted artifact.
8. Under the still-valid lease, atomically persist the verified checkpoint, successful incremental outcome, audit completion and lease release. Refresh finalization leaves incremental progress/eligibility unchanged.

Every renewal, failure finalization, success finalization and release is conditional on source/farm, owner, token and valid ownership. A zero-row fenced update means ownership was lost. The stale process must stop; it cannot release its successor's lease.

A lease cannot physically prevent a paused process from performing external writes after it resumes. Immutable attempt-specific paths and downstream version-aware merge protect those external operations. Run failure does not make already published Raw rows invalid or safe to delete.

## 6. Window planning and pagination

Capture `CommittedCheckpoint`, derive `ExtractionQueryStartInclusiveUtc` by applying the configured safety overlap, and obtain the frozen `ExtractionQueryEndExclusiveUtc` from the source adapter.

Split `[ExtractionQueryStartInclusiveUtc, ExtractionQueryEndExclusiveUtc)` at calendar-month boundaries in the source's verified UTC interpretation. Partial first and last months are valid. Do not approximate a month as 30 days.

| Window | Inclusive start | Exclusive end |
| --- | --- | --- |
| W1 | 2026-05-15T00:00:00Z | 2026-06-01T00:00:00Z |
| W2 | 2026-06-01T00:00:00Z | 2026-07-01T00:00:00Z |
| W3 | 2026-07-01T00:00:00Z | 2026-08-01T00:00:00Z |
| W4 | 2026-08-01T00:00:00Z | 2026-08-20T10:30:00Z |

Conceptual Production predicate; adapt syntax to the actual source database:

```sql
WHERE WindfarmId = @windfarmId
  AND LastUpdatedDate >= @windowStart
  AND LastUpdatedDate <  @windowEnd
-- On continuation pages only:
  AND (
       LastUpdatedDate > @pageTimestamp
       OR (LastUpdatedDate = @pageTimestamp AND Id > @pageId)
  )
ORDER BY LastUpdatedDate, Id
```

Each window starts with its own empty page cursor and uses a unique, stable row-ID tie-breaker. Never reuse a page cursor from another window. Never apply the previous run's `last_committed_id` to exclude the overlap on the first page.

Verify that the source updates `LastUpdatedDate` for all relevant mutations and that the ordering pair is adequate. A frozen time upper bound does not create a shared database snapshot across parallel connections. The source adapter must document its read-consistency and late-commit behavior. A bounded overlap only covers late visibility within its bound; unbounded late commits, hard deletes without change records, or backdated changes require a stronger source contract or reconciliation. Do not claim capture of every intermediate source version from a mutable-table poll.

## 7. In-process concurrency and memory

Use an explicitly configured `MaxParallelWindows`; **2 is an initial tuning value**, not a performance guarantee. A normal daily range may expose only one task. Every task has its own connection/reader, page state, part counter and writer buffers.

Use a bounded async loop, such as `Parallel.ForEachAsync` with explicit `ParallelOptions`, when supported by the repository's SDK. Do not share an EF `DbContext`, data reader, database connection or Parquet writer across concurrent tasks. Do not use `Task.Run` for ordinary asynchronous I/O or create unbounded tasks.

Bound page rows, part rows/bytes, writer buffers and upload concurrency. Approximate peak working memory as concurrent windows multiplied by per-window page/writer/upload buffers, plus host overhead. Stream or page source rows; do not materialize an entire month. Total upstream load also depends on how many other farm pods are running.

Calendar windows limit query scope, not file size. A large month produces several bounded Parquet parts. A window's parts have unique names even when windows finish out of order.

## 8. Raw payload, path and metadata

### 8.1 Incremental payload

Persist the source rows returned by the query without replacing them with a separate full-day read-back. Parquet encoding is allowed; business aggregation, gap filling and normalization belong downstream. Retain source identifiers, production timestamps, source update timestamps, revision/tombstone fields where provided, and the extracted source values needed for replay.

This preserves what the source returned for the execution. It does not claim a cross-connection source snapshot or byte-for-byte retention of a wire response.

### 8.2 Direct extraction-folder layout

```text
raw/production/windfarm=WF01/
  extraction=57/
    window-20260515T000000Z-to-20260601T000000Z-part-0001.parquet
    window-20260601T000000Z-to-20260701T000000Z-part-0001.parquet
    window-20260601T000000Z-to-20260701T000000Z-part-0002.parquet
    window-20260701T000000Z-to-20260801T000000Z-part-0001.parquet
```

No production-date or window subfolder is required. The filename range is the exact query window, **start inclusive and end exclusive**. `part-0001` resets per window. Treat four digits as minimum padding, not a maximum of 9,999 parts.

Use invariant UTC timestamps without colons. If query precision exceeds seconds, retain fractional digits in both filename boundaries, for example `20260924T103000.1234567Z`; do not round distinct boundaries to the same name. Preserve the original source type/precision information in metadata when conversion to UTC requires interpretation.

`LeaseToken` is scoped to a cursor identity. The full source/farm/token/window/part path is the artifact identity; a bare token is not globally unique. Preserve the token sequence and never delete/reseed a cursor to restart ingestion.

### 8.3 Parquet key-value metadata

| Field | Meaning |
| --- | --- |
| `schema_version`, `payload_kind` | Version and semantics, e.g. `ProductionChanges` |
| `source_type`, `windfarm_id`, `source_system` | Stream identity and adapter routing context |
| `run_id`, `request_id` when applicable, `extraction_token` | Attempt/request correlation |
| `query_axis`, `query_start_inclusive`, `query_end_exclusive` | `LastUpdatedDate` and exact executed bounds for incremental Production |
| `part_number`, `row_count`, `extracted_at_utc` | Physical artifact information |
| `min_production_timestamp`, `max_production_timestamp` | Observed extent only; not a completeness claim |
| `min_source_updated`, `max_source_updated` | Source update extent within this part |

The envelope or audit manifest can additionally carry bytes, checksum and trace context. File metadata cannot claim the final number of future parts while a window is still being streamed. Fan-out does not parse filenames as its source of business semantics.

## 9. Publication, immutability and retries

Complete and durably store a bounded part before publishing its notification. Do not notify on an incomplete multipart upload or unclosed Parquet file. For incremental changed rows, each part is independently consumable; it does not claim full-day completeness.

A successful queue send means that artifact's handoff was accepted. Check each entry of a batch response, including responses with HTTP 200. Retry only failed or ambiguous sends as appropriate; duplicate notifications are expected.

Retry layers are bounded:

- A failed upload can retry the same immutable bytes at the same identity using provider-appropriate conditional/existence and checksum checks.
- An ambiguous queue result can resend the identical envelope. Never reread the source merely to retry a queue send.
- A source-page retry before publishing its result can retry that page under the adapter's consistency contract.
- If recovery would restart a window whose earlier parts have already been published, fail the farm attempt after bounded retries and restart under a **new extraction token**. Do not requery and overwrite earlier published part names with different bytes.

Do not add random suffixes merely to hide conflicting writes. A conflicting existing object with different bytes is an error. A new attempt, including a deliberately repeated range, gets a new lease token and a distinct audit identity.

## 10. Whole-execution checkpoint and failure recovery

Window tasks report results in memory. They never update `extraction_cursor` individually. W3 finishing before W2 cannot move the farm checkpoint past W2.

For the existing Production timestamp/ID cursor, choose a non-regressing checkpoint from the verified completed scan, normally the maximum observed source pair after all windows succeed. The configured overlap is reapplied next time. If no newer row was observed, retain the committed pair and record successful no-data completion. Do not blindly write `ExtractionQueryEndExclusiveUtc` or fabricate an ID to represent an empty range. An adapter using a verified exclusive scanned-boundary checkpoint needs an explicit mapping/migration; do not mix the two representations.

Failure/cancellation preserves the pre-execution checkpoint. Record incremental failure/backoff and audit outcome under the valid lease, stop scheduling new windows, cancel or safely finish already started tasks, then release ownership. A restart rebuilds the plan from the old checkpoint and may choose a later frozen upper bound; it does not resume particular parts from audit history.

| Failure point | Required behavior |
| --- | --- |
| One of three windows fails | No execution checkpoint advancement; retry bounded operations, then fail/replay |
| Raw durable, notification fails | No advancement; retry notification without changing the file |
| Notification accepted, final DB commit fails | Next attempt may replay; consumers handle duplicates and versions |
| Commit response is ambiguous | Re-read the authoritative cursor/token; never force rollback or release a successor |
| Pod dies | Lease expires; another attempt uses a new token and original committed progress |
| Lease renewal fails or ownership is lost | Cancel work; no successful fenced finalization |
| Published files exist from a failed attempt | Keep them; downstream may already reference or be processing them |

There is no per-window resume after restart and no all-or-nothing visibility of all Raw artifacts. Some successful parts can reach Harmonization before the farm attempt finishes or fails. The all-window barrier protects checkpoint advancement, not atomic downstream publication.

Retention must protect artifacts needed by queue retries, DLQ recovery, downstream references and audit. A failed attempt is not sufficient reason to garbage-collect its published Raw files.

## 11. Raw notification and fan-out contract

Production change files may contain many production dates. Use a versioned envelope whose semantics support this; a required single `DataDate` in an older contract cannot be populated with the query date as a workaround.

Illustrative new envelope fields (wire version/name must be aligned with the existing contracts):

```json
{
  "schemaVersion": 2,
  "sourceType": "Production",
  "payloadKind": "ProductionChanges",
  "windfarmId": "WF01",
  "runId": "<attempt-id>",
  "extractionToken": 57,
  "queryAxis": "LastUpdatedDate",
  "queryStartInclusive": "2026-06-01T00:00:00Z",
  "queryEndExclusive": "2026-07-01T00:00:00Z",
  "partNumber": 1,
  "blobPath": "raw/production/windfarm=WF01/extraction=57/window-20260601T000000Z-to-20260701T000000Z-part-0001.parquet",
  "dataDate": null,
  "traceId": "<trace-id>"
}
```

Fan-out opens the referenced immutable part, derives distinct `(ItemId, UtcProductionDate)` identities from its rows, sorts deterministically, groups by actual date and splits each group into configurable item batches, initially up to 50. Each child carries the real date, selected items, payload kind and exact artifact reference. A deterministic child identity includes artifact identity, date, batch number and contract/batching-policy version.

The same Item-Day can appear in several parts, windows and executions. A child message represents that artifact's contribution, not an assertion that all changes for the Item-Day have arrived. Fan-out does not consult the cursor, wait for the whole extraction or infer dates from the filename. Parent redelivery may resend children; downstream handles this.

## 12. Harmonizer compatibility requirements

These are consequences of changing Production Raw from daily snapshots to changed-row parts. They are required before enabling the new producer contract.

1. Under the existing fenced Item-Day/input lease, read the latest committed Production state, merge the incoming contribution and publish a complete revised component. Never replace an entire day with one delta part.
2. Use the source's verified stable production-record key. If that key is a composite, explicitly map its fields; do not assume item/time is unique without source evidence.
3. Compare source version/update information **per record**. A later artifact, extraction token or Item-Day-level maximum timestamp cannot decide whether every row in another part is obsolete. Same key/version with different content requires a defined source tie-breaker or a visible conflict, not arrival-order selection.
4. Retain record-level version/provenance needed for future merges, including deletion versions so an older replay cannot resurrect a removed row. Persist newer ordering metadata even when business values have not changed. Identical row/version replays are no-ops.
5. Missing rows in a delta do not mean deletion or authoritative emptiness. Detect deletions through source tombstones/change capture or an explicitly authoritative refresh. If a record can move to another item/date, require old-scope reconciliation or a verified immutable partition key so its former contribution is removed.
6. A successfully merged part is not proof of complete daily coverage. Initial load and gap handling must establish expected coverage through the existing readiness policy; source row counts alone do not prove distinct interval coverage. Subsequent deltas can revise an already complete day. No new execution-wide downstream publication barrier is introduced.

### 12.1 Failure recovery for multiple contributions

A single `LastRawPath` cannot safely represent several independently failed delta contributions. For the initial `ProductionChanges` path, use the existing queue and failed-state row conservatively:

- If any item fails or is blocked by an earlier unresolved contribution, do **not acknowledge that ProductionChanges batch**. Healthy items may commit and become no-ops on redelivery.
- Under the existing Item-Day lease, preserve the first unresolved artifact's failure reference in `ItemDayInputState`. Another artifact must not overwrite it or clear `Failed`; defer that Item-Day until the failed artifact is repaired/replayed. This avoids accumulating a new per-artifact failure ledger.
- Only successful processing of the recorded failed contribution, or explicit validated repair, can clear that blocker. The reconciliation sweep may retry this exact contribution; original queue redelivery remains safe.
- After delivery retries are exhausted, the retained batch/DLQ message identifies the exact contribution. A poison data row can therefore cause a ProductionChanges batch to reach the DLQ. Keep readiness blocked and recover every affected contribution before DLQ/Raw retention expires.

This is an explicit small-team trade-off: some healthy-item replay and possible head-of-line blocking instead of a new durable failed-contribution table. The earlier generic rule “isolate an item, acknowledge the batch, recover only its latest Raw path” is insufficient for multipart Production deltas. Existing non-Production handling is not redesigned here.

## 13. Historical refresh and other sources

Historical refresh uses explicit **production-date** scope, not the incremental update window. It keeps the existing same-farm lease and never changes incremental cursor/schedule state. Large requests may use bounded monthly task groups inside the pod while preserving the exact requested dates, including non-contiguous selections.

Retain authoritative refresh semantics. A refresh payload must declare its query axis, exact snapshot scope and a completeness boundary. If a logical snapshot spans multiple parts, publish a complete immutable manifest listing all of them before allowing replacement/reconciliation. Empty scopes must be explicit; omission is not an empty snapshot. Filenames use their actual query bounds and metadata says `ProductionTimestamp`, not `LastUpdatedDate`. For a non-contiguous selection, retain the exact date set in the scope metadata rather than claiming that min/max is continuous.

Incremental `ProductionChanges` can flow part by part. They must never be passed to a snapshot-replacement path. An older refresh must also not roll back newer per-record changes; define comparable source versions/snapshot-as-of evidence before allowing overlapping downstream refresh and incremental results to supersede one another.

Outage and Curtailment retain source-specific changed-event semantics and event-footprint reconciliation. Alarm's increasing-ID cursor is retained only if it captures every required change; do not impose timestamp windows on an ID-only source. Adjustment keeps its existing transactional outbox. Use this execution pattern where the adapter supports it without silently changing those sources' checkpoint algorithms or wire contracts.

## 14. C# responsibility structure

The repository has not been inspected for this document. Preserve existing project names, SDK/package pins, test folders and deployment conventions. Prefer one existing executable project with cohesive folders over extra assemblies solely for this change.

Illustrative mapping within the Production ingestor:

```text
src/Raw/Ingestor/ProductionIngestor/
  Program.cs
  Hosting/IngestionWorker.cs
  Ingestion/IngestionExecutionService.cs
  Ingestion/WindowPlanner.cs
  Ingestion/WindowProcessor.cs
  Cursor/ExtractionCursorRepository.cs
  Cursor/LeaseHeartbeat.cs
  Audit/ExtractionRunRepository.cs
  Extraction/ProductionSourceReader.cs
  Raw/ParquetArtifactWriter.cs
  Messaging/RawArtifactPublisher.cs
  Configuration/IngestionOptions.cs
  Observability/
```

`Program.cs` owns composition. `IngestionExecutionService` owns the finite use case and final checkpoint decision. `WindowPlanner` is pure date logic. `WindowProcessor` streams source pages into bounded artifacts and publishes them. Repositories and source/storage/queue adapters isolate provider I/O. These are classes/folders inside the host, not new services or a general workflow engine.

Introduce interfaces at source, storage, queue and database boundaries where substitution and testing justify them. Do not add generic base services, a Domain project, CQRS or a second orchestration framework. Reuse an existing shared message-contract project if present; version the actual wire contract rather than sharing internal database entities.

Keep `CancellationToken` propagation, explicit lifetimes and resource disposal consistent. Each window creates its own scoped resources when necessary. Preserve exception context and use structured logs through the existing OpenTelemetry integration.

### Coding guidance — required by the user

Build with SOLID principles, clean code, human readability, C# best practices and appropriate design patterns. Prefer explicit, understandable execution flow over clever abstractions or compressed code. These requirements apply to all ingestor changes and associated tests/adapters.

#### Responsibilities and patterns

- **Single responsibility:** separate window planning, source reading, Raw writing, notification publication, cursor ownership, audit and execution outcome decisions. Keep `Program.cs` and hosted-service entry points thin.
- **Open/closed:** extend source behavior through focused adapters/strategies where multiple source implementations exist; avoid scattering source-type conditionals throughout the execution service.
- **Liskov substitution:** implementations must honor the same documented completion, cancellation, pagination and failure contracts. A source adapter must not return success when it stopped early.
- **Interface segregation:** expose small interfaces matching what callers need. Do not create a universal ingestor interface with unrelated methods.
- **Dependency inversion:** orchestration depends on source/storage/queue/cursor boundary contracts injected through constructors. Keep SDK details inside adapters.
- Use the **Strategy/Adapter** pattern for source/provider differences and existing **Repository** boundaries for cursor/audit persistence. Use a typed **Result** pattern for expected outcomes. These are tools for concrete responsibilities, not a requirement to add a framework, mediator or factory for every class.
- Preserve existing assemblies, dependency injection conventions and SDK/package versions unless a concrete required boundary justifies change. One interface per class and speculative base classes are not SOLID requirements.

#### Names must express the business meaning

Avoid vague names such as `HighWatermark`, `LowWatermark`, `ProcessData`, `DoWork`, `obj` or `data` when a precise domain name is available. Do not name a data value as though it were a method.

| Purpose | Preferred name |
| --- | --- |
| Query the latest source LastUpdatedDate for a farm | `GetLastUpdatedDateByWindfarmIdAsync` |
| Return a verified exclusive extraction query limit | `GetExtractionQueryEndByWindfarmIdAsync` |
| Latest timestamp actually observed in source | `LatestSourceLastUpdatedDateUtc` |
| Frozen exclusive query limit for this execution | `ExtractionQueryEndExclusiveUtc` |
| Inclusive query start after overlap | `ExtractionQueryStartInclusiveUtc` |
| Per-window bounds | `WindowStartInclusiveUtc`, `WindowEndExclusiveUtc` |
| Process one bounded window | `ProcessExtractionWindowAsync` |
| Finalize verified whole-execution success | `CompleteExtractionAsync` |

The method must match the query it actually performs. A method selecting `MAX(LastUpdatedDate)` returns an observed timestamp; it must not silently be treated as an exclusive upper bound, which would exclude rows at that maximum. A method obtaining source time/snapshot cutoff should use the query-end name. Preserve precision and boundary semantics. Ask the user if the repository/source contract does not establish which operation is intended; do not mechanically rename a variable into `Get...` or invent a timestamp increment.

#### Pass strongly typed objects between methods

Use dedicated request/context/result objects at ingestion service and adapter boundaries rather than long lists of unrelated primitive/value arguments or positional tuples. Use reference-type `sealed class` or `sealed record` DTOs; do not use `record struct`, boxing into `object`, `dynamic`, or untyped dictionaries to carry business data.

Illustrative objects:

- `ExtractionExecutionContext`: source/farm identity, attempt, token, query scope and mode.
- `ExtractionWindowRequest`: execution identity and exact window bounds.
- `SourcePageRequest` / `SourcePageResult`: scope, continuation and returned rows.
- `RawArtifactDescriptor`: identity, immutable path, payload kind and query metadata.
- `WindowProcessingResult` / `ExtractionExecutionResult`: typed outcome, source progress evidence, counts and failure detail.
- `GetSourceUpdateBoundaryRequest` / `SourceUpdateBoundaryResult`: farm/source identity and explicitly distinguished observed timestamp versus exclusive query boundary.

For example, a boundary method can take this shape:

```csharp
Task<SourceUpdateBoundaryResult> GetLastUpdatedDateByWindfarmIdAsync(
    GetSourceUpdateBoundaryRequest request,
    CancellationToken cancellationToken);

Task<WindowProcessingResult> ProcessExtractionWindowAsync(
    ExtractionWindowRequest request,
    CancellationToken cancellationToken);
```

Keep normal scalar types such as `DateTimeOffset`, IDs, counts and enums as properties inside these objects. `CancellationToken` remains a conventional separate parameter. This guidance does not mean replacing scalar properties with boxed objects, wrapping every arithmetic helper, or converting every existing public SDK signature. Keep the DTO cohesive, minimal and immutable where practical. Parallel windows must not mutate a shared context or result list without appropriate ownership/synchronization.

#### Expected outcomes, logging and exceptions

Do not throw exceptions for normal control flow such as no due work, no changed rows, Busy ownership, a known validation failure or a classified operation failure. Return explicit typed outcomes such as `Succeeded`, `NoData`, `Busy`, `Failed`, `Cancelled` and `LeaseLost`, with actionable error code/context where relevant. Model valid states so a failed result cannot look like a successful empty response.

Use the injected `ILogger<T>` with structured message templates through the existing OpenTelemetry pipeline. Include source type, farm, attempt, extraction token, query bounds, artifact identity and retry count where useful; redact secrets and sensitive row payloads.

- No work/no data: information or debug; do not log an error.
- Busy or cancellation: normal outcome with appropriate information-level context, unless a separate operational policy requires an alert.
- Retriable transient failure: warning/debug with bounded retry context; log the terminal failure as an error when retries are exhausted.
- Failed validation, source read, upload, notification or finalization: error at the layer responsible for handling/reporting it, with a typed failed outcome. Avoid duplicate error logs at every call layer.
- Unexpected exceptions: retain their diagnostic context and stack, log at the owning boundary and fail the attempt. SDKs may throw; catch where recovery/classification is meaningful. Do not replace every exception with a success-shaped default or catch-and-continue.

Use structured logging, for example `logger.LogError(exception, "Raw publication failed for wind farm {WindfarmId}, extraction {ExtractionToken}, artifact {ArtifactPath}", ...)`; pass the real exception when one exists instead of constructing one merely to log. Do not use string interpolation to flatten structured fields. If rethrowing an exception, use `throw;` to retain the stack.

**Logging an error is not sufficient error propagation.** A failed window returns/propagates failure to the execution service. The service prevents checkpoint advancement, records the appropriate audit/operational outcome and returns a meaningful Job exit status. Cancellation does not become success; no-data success does not become failure.

#### C# quality rules and questions

- Use async I/O end to end, the `Async` suffix, nullable annotations and honest result contracts. Avoid `.Result`, `.Wait()`, `async void`, fire-and-forget work and normal I/O wrapped in `Task.Run`.
- Use `using`/`await using` as supported by the actual SDK types. Keep per-window readers, contexts, connections and writers independent; use correct DI scopes and lifetimes.
- Validate typed options at startup; bound retries, memory, concurrency and shutdown. Keep source timezone/precision handling explicit and culture-invariant.
- Write small cohesive methods with guard clauses and descriptive local names. Comments explain reasons, invariants and trade-offs; they do not narrate obvious statements. Avoid deeply nested flow and unnecessary abbreviations.
- Keep tests readable and behavior-focused: boundary windows, failure propagation, stale ownership, duplicate contributions and safe finalization. Run the repository's build/analyzers/tests and report actual results.
- **Ask the user whenever a material requirement or implementation question remains unresolved.** First inspect the repository and this design; do not guess source keys/version guarantees, date semantics, schema migrations, deletion behavior, API contracts or business completeness rules. Present the concrete question, relevant evidence, options and a recommendation. Do not ask again about decisions already settled here; routine naming/style choices within these rules can proceed.

The current deliverable is documentation. No repository code has been changed or build/test execution claimed.

## 15. Configuration and telemetry

| Setting | Purpose |
| --- | --- |
| `MaxParallelWindows` | Positive bounded concurrency; initial tuning value 2 |
| Calendar-month window policy | Partial edge months; adapter may require a smaller maximum |
| Source page and Raw part limits | Bound memory, file scan cost and upload size |
| Safety overlap / source stabilization | Match verified late-visibility behavior; no unconditional guarantee |
| Source timeout and operation retries | Bounded backoff; account for SDK retry layers |
| Lease duration and renewal interval | Keep ownership alive during slow I/O; renewal starts well before expiry |
| Ingestion interval, trigger schedule, failure backoff | Meet daily freshness with actual worker availability |
| Invocation/attempt budget and shutdown grace | Bound total execution without silently truncating a successful range |

Log claim, attempt start, query bounds, window start/completion, rows/files/bytes, retries, publication failure, lease loss and final checkpoint/outcome. Put run/token/window/artifact identities in logs and trace attributes. Use bounded-cardinality metric labels; do not label metrics with paths, IDs or arbitrary timestamps. Avoid per-row success logs and redact credentials/source-sensitive payloads.

Track source lag, time since last successful ingestion, source/update versus production-date extent, running windows, memory, throughput, errors, queue/DLQ backlog and repeated replay. A successful pod exit or source connection is not proof of full production-data availability.

## 16. Validation contract

These are implementation acceptance scenarios, not tests claimed to have run against a repository.

| Scenario | Required result |
| --- | --- |
| Month edges, leap February, partial-day bounds | Complete non-overlapping `[start,end)` plan with preserved precision |
| One-day updates contain one year of production dates | One query window; fan-out emits actual Item-Days |
| Multiple source rows share update timestamp | Stable keyset pagination retains every required row |
| Three windows, concurrency limit two | At most two active source/window pipelines |
| W3 finishes before W2; W2 then fails | Original checkpoint unchanged |
| Raw upload succeeds, queue send fails or is ambiguous | Same immutable artifact retried; no premature checkpoint |
| Batch send has mixed entry outcomes | Only confirmed successes counted; every required send eventually verified |
| Pod termination after some notifications | New token, whole-range replay, no overwrite or duplicate business facts |
| Same range deliberately run twice today | Distinct audit attempts and extraction folders |
| Expired owner resumes | Cannot finalize/release successor; its messages cannot regress source versions |
| Two parts update different records of the same Item-Day | Both changes survive regardless of arrival order |
| Older record replay after a tombstone | Deleted record is not resurrected |
| Failed delta A, later delta B | B cannot erase A's blocker/reference; A remains recoverable |
| Empty incremental result | No fabricated empty-day payload/checkpoint; attempt completion recorded |
| Sparse delta on an uninitialized Item-Day | Not falsely marked fully covered |
| Authoritative refresh split into parts, including empty scope | Complete scope proof before replacement; no deletion inferred from a partial part |
| Historical refresh or replay | Normal incremental checkpoint and schedule preserved |
| Source mutation during paging / late commit | Adapter's documented overlap/consistency behavior demonstrated |

Use focused unit tests for the planner, completion rules and mode routing; PostgreSQL integration tests for atomic claims, fencing and finalization; and storage/queue/consumer integration tests for publication and multi-part merging. Run the repository's actual restore/build/test commands after code changes. This document makes no throughput or successful-build claim.

## 17. Delivery sequence

1. Inspect current runner, cursor/run repositories, source semantics, migrations, Raw writer, fan-out contracts and Harmonizer readiness/merge code. Apply Section 14 coding guidance and ask the user about unresolved material questions before inventing behavior.
2. Preserve the cursor key, registration and refresh compatibility. Remove any dependency on new distributed ingestion work tracking from this scope.
3. Implement pure monthly window planning and bounded in-process execution, with independent resources and lease renewal.
4. Implement immutable Raw parts, exact filename/metadata boundaries, notification retries and whole-execution checkpoint finalization.
5. Introduce a versioned multi-date Production change envelope. Deploy compatible fan-out and Harmonizer behavior before enabling producers; retain old snapshot interpretation for already queued messages.
6. Validate duplicate/out-of-order contributions, delta failure retention and daily coverage. Do not label old V1 snapshot messages as deltas merely because both use Parquet.
7. Run focused tests and representative source/load checks; tune concurrency and file/page limits. Preserve the existing Kustomize layout and API-compatible CronJob template.

## 18. Accepted limits

- A crash replays successful windows because no durable per-window work ledger exists.
- Monthly windows bound update time, while file/page limits bound volume; neither guarantees evenly sized tasks.
- Published parts can reach downstream before whole-execution success; ingestion success is not Harmonization or Calculation completion.
- Capture is constrained by the source's update/version/deletion guarantees. Immutable Raw preserves observed responses, not changes the query never observed.
- Delta-aware consumer changes are required; changing the producer path alone is insufficient.

## 19. References

Application decisions come from the discussion on 24 September 2026 and the current V12 plan. Platform mechanics are supported by:

- [Microsoft: Parallel.ForEachAsync](https://learn.microsoft.com/en-us/dotnet/api/system.threading.tasks.parallel.foreachasync) — bounded asynchronous iteration with explicit parallel options; use the repository's supported framework.
- [AWS: SendMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html) — inspect individual success/failure entries even when the batch request returns HTTP 200.
- [AWS: Standard queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html) — duplicate and out-of-order delivery require idempotent consumers.
- [PostgreSQL: SELECT locking clauses](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE) — short atomic claim selection; a persisted lease protects work after the transaction ends.
