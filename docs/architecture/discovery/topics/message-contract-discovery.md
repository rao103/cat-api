# Loss Calculation Platform — Message Contract Discovery

**Updated:** 2026-10-12 (Asia/Kuala_Lumpur)  
**Repository target:** `docs/architecture/discovery/loss-calculation/message-contract.md`  
**Purpose:** Give every Ingestor developer one integration reference before building or changing a producer, so source-specific implementations do not require repeated Fan-Out/Harmonizer contract refactors.  
**Status:** Source-backed discovery and implementation guardrails. Current repository/deployment compatibility must be verified; proposals and unresolved contracts are identified below.  
**Architecture boundary:** Supplements V13 and [Platform Discovery](../platform-discovery.md). It does not replace or amend V13.  
**Companions:** [Raw ingestion](raw-ingestion.md), [Raw Fan-Out](raw-fan-out.md), [Harmonizer](harmonizer.md), [Event management](harmonizer-event-management.md), [Adjustment](adjustment.md), [Decision Log](../../decision-log.md).

## 1. Start here: the contract a developer must establish

Before implementing an Ingestor, agree its **source data contract**, **Raw Parquet schema**, **Raw notification contract**, and **downstream work route**. A producer is compatible only when its real serialized JSON and Parquet can be consumed together by Fan-Out and Harmonizer. Reusing a DTO or compiling the Ingestor alone is insufficient.

The practical rule is: **reuse the shared message envelope and transport; implement the source-specific query, projection and semantics.** Do not copy another Ingestor's source assumptions because its fields look similar. Curtailment demonstrated why: it contains ten-minute status rows, including changes from curtailed to non-curtailed, and is not an Outage event stream.

For new work, complete the short contract card in section 12, select the applicable profile below, and validate one real fixture through both consumers before building the rest of the component. Keep this within the existing Micro-RPI; no separate large specification exercise is required.

## 2. Source profile matrix

| Source | Source shape and identity | Incremental capture | Refresh capture | Downstream route | Evidence/status |
|---|---|---|---|---|---|
| Production | Ten-minute series; numeric WTG plus interval/slot | Changed source rows; update-time query axis | Selected business-time intervals and frozen WTG scope | WTG-day work | Saved Production parser/serializer and shared contracts; exact active producer route must be checked |
| Curtailment | Ten-minute series; numeric WTG plus UTC slot | All changed rows, including active and inactive status | Whole UTC days and frozen WTG names | `CurtailmentWtgDayWorkV3` | Latest `Curtailment.zip` version 2 inspected; inline refresh completion implemented in supplied package |
| Outage | Event identified by `OutageClassificationId`; each event belongs to one WTG | Live/update feed and explicit deleted feed | Events overlapping a specified interval | `EventWorkV3`; old/new footprint reconciliation in Harmonizer | Saved parser and aligned topic specifications; current shared refresh is receipt-only |
| Alarm | Event direction; actual stable event/member key must be confirmed | Existing API uses Discovery/Polling routes, not a proven change watermark | Existing Historical route; completeness/absence authority unproven | Event/member direction | Saved `RawAlarmBatchAvailableV1`; no verified source watermark. Common V2 producer support remains unresolved |
| Adjustment | One aggregate ID with zero-to-many WTG children | Complete aggregate revision from transactional outbox; not ordinary source polling | Recovery/replay semantics need explicit agreement | Event/member direction with membership cleanup | Topic-level direction; exact wire/revision/dispatcher integration not inspected for this document |

**Do not treat all five sources as the same extraction algorithm.** Outage's explicit tombstone feed does not establish a deletion mechanism for Curtailment or Alarm. Adjustment child membership is not evidence that one Outage classification can contain multiple WTGs.

## 3. Boundaries and ownership

| Boundary | Producer responsibility | Consumer responsibility | Completion means |
|---|---|---|---|
| Source → Ingestor | Freeze query scope; page without gaps; project source identity/version/status | Ingestor validates source rows and writes immutable Raw | Source capture succeeded for the declared unit |
| Ingestor → Raw Fan-Out | Finalize files; publish exact references and truthful metadata; persist operation/run evidence | Validate envelope and payload, resolve scope/identities, freeze deterministic batches | Raw is available; not harmonization or serving completion |
| Raw Fan-Out → Harmonizer | Persist the complete batch plan and exact bodies before sending | Apply series merge or canonical event rules; persist receipt/materialization evidence | The declared batch reached its documented durable boundary |
| Harmonizer → subsequent stages | Publish committed revision/package-readiness evidence through the existing notification path | Apply the accepted coordinator/calculation/publication contract | A source revision is not calculation or SQL publication completion |

Ingestors do not compare incoming source rows with Harmonized data to decide whether a source change should be captured. In the current Curtailment implementation, a transition to non-curtailed must be captured even if downstream currently holds an active curtailed row. Harmonizer owns merge/version conflict handling and any later derived event interpretation. Source-side optimization is acceptable only when it preserves the agreed capture semantics.

This document specifies the Ingestor handoff and the downstream compatibility requirements. It does not invent a new Harmonizer → Calculation wire schema; package readiness remains an integration question in Platform Discovery.

## 4. Shared code and version ownership

The latest supplied package keeps transport-independent DTOs, conventions, identity helpers and validation in `src/Shared/Messaging.Contracts`. `Messaging.Sqs` owns the reusable sender. Each Ingestor's adapter delegates to it. Preserve existing Core/Worker organization; these responsibilities do not require new deployable services.

| Version field | What it versions | Inspected example |
|---|---|---|
| `EnvelopeVersion` | Raw SQS JSON envelope | `RawArtifactAvailableV2`: 2 |
| `PayloadSchemaVersion` | Referenced Raw Parquet row schema | Curtailment: 2 |
| `RefreshCompletion.SchemaVersion` | Inline refresh completeness structure | `RawRefreshCompletionV1`: 1 |
| `ManifestSchemaVersion` | External Raw manifest structure when used | `RawSnapshotManifestV1`: 1 |
| Work `SchemaVersion` | Fan-Out → Harmonizer JSON | Curtailment WTG-day: 3 |

These versions are independent. A class suffix is not sufficient to identify a wire version; inspect serialized fields and validation.

Existing legacy classes can remain for compatibility. Their presence does not authorize new publishers to use them. In particular, `RawCurtailmentBatchAvailableV1` still exists in the latest archive with event-oriented comments, but the current Curtailment publisher takes `RawArtifactAvailableV2`. Use the active producer/reader, not a leftover class name, as evidence.

The latest archive renames Production C# types to `ProductionWtgDayWorkV2/V3` while retaining their JSON fields. Earlier serializer snapshots use `ProductionItemDayWorkV2/V3`. Coordinate C# references when integrating; do not rename persisted JSON fields merely to standardize terminology. Use **WTG** in new designs.

## 5. RawArtifactAvailableV2 producer profile

This is the common envelope in the latest supplied Curtailment integration. Its validator currently supports Production, Outage and Curtailment. It does **not** establish Alarm or Adjustment support.

### 5.1 Common metadata

| Field | JSON type / rule for this producer profile | Meaning |
|---|---|---|
| `EnvelopeVersion` | Integer, exactly `2` | Raw envelope version |
| `MessageKind` | String, exactly `RawArtifactAvailable` | Notification discriminator |
| `SourceType` | Exact registered string | Current validator accepts `Production`, `Outage`, `Curtailment` |
| `PayloadSchemaVersion` | Integer; active profile `2` | Select the correct row reader; legacy Outage schema 1 is a reader option |
| `ExtractionMode` | `Incremental` or `Refresh` | Select capture semantics |
| `PayloadKind` | Source/mode-specific string; see section 5.2 | Select live/change/snapshot/deletion semantics |
| `WindfarmId` | Positive JSON integer | Internal catalog identity. Numeric string acceptance is compatibility reading, not the new producer format |
| `WindfarmName` | Nonblank string | Source/catalog name, separate from ID |
| `RunId` | Nonblank string | Extraction run identity; preserve existing representation |
| `ExtractionToken` | Nonnegative Int64 JSON number | Extraction/lease token, scoped by source and farm; not a source row version |
| `AttemptId` | String equal to invariant `ExtractionToken.ToString()` | This envelope profile's attempt identity; not a universal rule for older contracts |
| `QueryAxis` | Exact profile string | Meaning of query bounds, independent of physical SQL column spelling |
| `QueryStartInclusiveUtc` | ISO timestamp serialized from UTC DateTime | Inclusive lower query bound |
| `QueryEndExclusiveUtc` | UTC timestamp, greater than start | Exclusive upper query bound |
| `RowCount` | Nonnegative Int64; positive for incremental part | Declared referenced row total, before consumer deduplication |
| `OperationId` | Opaque nonblank string, at most 256 chars, no control chars | Root operation correlation; require a persisted ID for new extraction runs |
| `TraceId` | Optional string | Diagnostic correlation; not a durable business identity |

`OperationId` is nullable in the DTO for compatibility, and `Validate()` permits null. That does not mean new runs may omit it: the inspected package resolves/persists it at run creation and has `OperationCorrelation.Require`. Verify propagation separately. Do not substitute RunId, AttemptId, ExtractionToken or SQS MessageId for it. Existing API-supplied IDs must be resolved through the control-plane operation contract; consumers must not generate a replacement root ID.

No environment field is required for routing or database identity: environments have separate databases/queues/configuration. Do not add one as a prerequisite for compatibility.

### 5.2 Exact source/mode combinations in the common validator

| Source | Mode | `PayloadKind` | `QueryAxis` | Scope kind for refresh |
|---|---|---|---|---|
| Production | Incremental | `ProductionChanges` | `LastUpdatedUtc` | None |
| Production | Refresh | `ProductionSnapshot` | `ProductionTimestampUtc` | `SelectedIntervals` |
| Curtailment | Incremental | `CurtailmentChanges` | `LastUpdatedUtc` | None |
| Curtailment | Refresh | `CurtailmentSnapshot` | `CurtailmentTimestampUtc` | `SelectedIntervals` |
| Outage | Incremental | `OutageEvents` or `OutageDeletedEvents` | `LastUpdatedUtc` | None |
| Outage | Refresh | `OutageSnapshot` | `EventOverlapUtc` | `EventsOverlappingInterval` |

**Compatibility warning:** the separately saved older Production parser requires `LastUpdatedDate` for incremental changes and `ProductionTimestamp` for `ExtractionReady`. Those differ from the common validator strings above. They are version/profile differences to reconcile in the active branch, not interchangeable aliases that a developer may silently emit. See section 10.

### 5.3 Incremental part rules

One message refers to **one durable Parquet part** with `BlobPath`. Set `QueryWindowNumber` and `PartNumber` to positive, one-based integers. Preserve the actual planned window ordinal across the run; do not hard-code every window to 1. Part ordinal is within that window.

Do not include `SnapshotScope`, `ManifestPath`, `ManifestSchemaVersion` or `RefreshCompletion`. There is no envelope `DataDate`: an update-time window can contain changes to many historical business dates. Fan-Out derives WTG/date groups from the payload rows.

An incremental window with no rows produces no positive-row part notification in this profile; record its outcome in run/cursor evidence. An explicit empty refresh is a separate case and must have completion evidence.

### 5.4 Refresh rules and transport selection

Refresh declares a complete requested capture **after all its required reads and file writes succeed**. It does not publish each part as an independent authoritative snapshot. In the current common envelope, top-level `QueryWindowNumber` and `PartNumber` are null; part/window identities belong to the completeness structure.

Exactly one supported reference form applies:

| Form | Envelope fields | Inspected applicability |
|---|---|---|
| Incremental part | `BlobPath` only | Production/Outage/Curtailment common profile |
| External refresh manifest | `ManifestPath`, `ManifestSchemaVersion=1`, `SnapshotScope`; no BlobPath or inline completion | Current common validator for Production/Outage; legacy Curtailment pointer reading remains supported |
| Inline refresh completion | `RefreshCompletion`, `SnapshotScope`; no BlobPath, ManifestPath or ManifestSchemaVersion | Current publisher/validator allows **Curtailment only** |
| Older Production completion | `MessageKind=ExtractionReady`, separate `ProductionExtractionReadyV1` fields | Saved older Production reader; not the same DTO as RawArtifactAvailableV2 |

These are intentionally explicit compatibility profiles. Do not force existing Production/Outage to inline completion just because Curtailment uses it. Do not introduce an automatic inline-to-manifest fallback unless both consumers support it and its identity/recovery semantics are specified.

### 5.5 Completeness structure

`RawRefreshCompletionV1` reuses `RawSnapshotManifestV1` validation in memory; this **does not write a Raw manifest file**. Both structures describe:

| Field | Required semantics |
|---|---|
| `SchemaVersion` | Exactly 1 |
| `SnapshotAsOfUtc` | Capture/observation time; **not** a source cutoff or deletion authority |
| `MinRetrieveDateUtc` | Captured retention/retrieval floor; not a row revision |
| `RequestedIntervals` | Nonempty list of exact nonoverlapping UTC half-open intervals |
| `ScopeWtgNames` | Frozen distinct source/catalog WTG names; names remain names even when numeric-looking |
| `CompletedWindows` | One completed window per requested interval, exact matching bounds, one-based ordinal, RowCount/ArtifactCount |
| `Parts` | Exact BlobPath, QueryWindowNumber, PartNumber and positive RowCount for each file |

The envelope `RowCount` equals the sum of part counts and window counts. Every part belongs to a known completed window. Paths and `(QueryWindowNumber, PartNumber)` pairs are unique. Part ordinals are contiguous within each window. An empty window has zero parts/counts and is still declared completed. A failed source read or missing file is never an empty result.

For Curtailment, requested intervals are whole UTC days and parts live under the extraction's refresh prefix. `SnapshotScope` describes the outer bounding interval; **RequestedIntervals** supplies the exact scope when days are disjoint. Consumers must not fill a gap between selected days merely because it lies within the bounding interval.

Scope membership is frozen at capture time. Resolve names uniquely within the farm; do not silently replace the roster with current catalog membership on retry. An empty WTG roster and an empty result for a nonempty roster are different facts.

Check the serialized UTF-8 message against the configured application byte budget before sending. The current inline implementation fails an oversized completion; submit bounded date requests. Do not leave a refresh marked completed when its completion message was rejected.

## 6. Exact Curtailment examples

The examples below follow the inspected active DTO/validator. IDs and paths are illustrative. Unused nullable fields are omitted for readability; the default serializer may include them as null. These are not declarations that the corresponding control-plane operation exists.

### 6.1 Incremental part from the second planned query window

```json
{
  "EnvelopeVersion": 2,
  "MessageKind": "RawArtifactAvailable",
  "PayloadSchemaVersion": 2,
  "ExtractionMode": "Incremental",
  "PayloadKind": "CurtailmentChanges",
  "SourceType": "Curtailment",
  "WindfarmId": 12,
  "WindfarmName": "WOW02",
  "RunId": "1842",
  "ExtractionToken": 58,
  "AttemptId": "58",
  "QueryAxis": "LastUpdatedUtc",
  "QueryStartInclusiveUtc": "2026-10-11T00:00:00Z",
  "QueryEndExclusiveUtc": "2026-10-12T00:00:00Z",
  "QueryWindowNumber": 2,
  "PartNumber": 1,
  "BlobPath": "raw/curtailment/windfarm=WOW02/mode=incremental/extraction=58/window-0002-part-0001.parquet",
  "RowCount": 25,
  "OperationId": "op-example-001"
}
```

This window selects **source update time**, not the business date of the 25 slots. The illustrative incremental filename is not a mandated writer naming convention.

### 6.2 Explicit empty refresh for one selected day

```json
{
  "EnvelopeVersion": 2,
  "MessageKind": "RawArtifactAvailable",
  "PayloadSchemaVersion": 2,
  "ExtractionMode": "Refresh",
  "PayloadKind": "CurtailmentSnapshot",
  "SourceType": "Curtailment",
  "WindfarmId": 12,
  "WindfarmName": "WOW02",
  "RunId": "1843",
  "ExtractionToken": 59,
  "AttemptId": "59",
  "QueryAxis": "CurtailmentTimestampUtc",
  "QueryStartInclusiveUtc": "2026-10-10T00:00:00Z",
  "QueryEndExclusiveUtc": "2026-10-11T00:00:00Z",
  "RowCount": 0,
  "SnapshotScope": {
    "Kind": "SelectedIntervals",
    "StartInclusiveUtc": "2026-10-10T00:00:00Z",
    "EndExclusiveUtc": "2026-10-11T00:00:00Z"
  },
  "RefreshCompletion": {
    "SchemaVersion": 1,
    "SnapshotAsOfUtc": "2026-10-12T07:00:00Z",
    "MinRetrieveDateUtc": "2026-01-01T00:00:00Z",
    "RequestedIntervals": [
      { "StartInclusiveUtc": "2026-10-10T00:00:00Z", "EndExclusiveUtc": "2026-10-11T00:00:00Z" }
    ],
    "ScopeWtgNames": ["WOW02-WTG001"],
    "CompletedWindows": [
      {
        "QueryWindowNumber": 1,
        "QueryStartInclusiveUtc": "2026-10-10T00:00:00Z",
        "QueryEndExclusiveUtc": "2026-10-11T00:00:00Z",
        "RowCount": 0,
        "ArtifactCount": 0,
        "Completed": true
      }
    ],
    "Parts": []
  },
  "OperationId": "op-example-002"
}
```

This communicates a successful empty observation for a known WTG/day. It **does not delete existing Curtailment rows**. For a populated refresh, include every exact part, set each window's counts and set the envelope total; do not publish positive rows with an empty Parts list.

## 7. Raw data correctness is part of message compatibility

| Concern | Required behavior |
|---|---|
| Internal IDs versus source names | New active schema uses numeric WindfarmId/WtgId and separate names. Older schema fields can contain names even when called `WtgId`; schema determines interpretation |
| Query time versus business time | Update-time extraction bounds do not define affected business days. Read row interval/event fields for planning |
| Pagination | Define a stable source-specific composite key; equal timestamps must not lose rows between pages |
| Frozen upper bound | Use a genuinely exclusive query end. A frozen inclusive maximum must be converted using source timestamp precision; current Curtailment uses one .NET tick with overflow validation |
| Source version | Use genuine source LastUpdatedUtc/revision evidence. ExtractionToken, AttemptId, SnapshotAsOfUtc and SQS delivery order are not row versions |
| Status transitions | Capture both positive and negative status changes; filtering only current active rows can leave stale downstream state |
| Event deletion | Only the explicit supported tombstone feed/profile establishes deletion; absence alone is insufficient |
| Immutable paths | Paths identify finalized immutable artifacts; never replace referenced bytes during replay |
| Paths | Current common validator requires container-relative paths without leading slash, credentials/URL, query/fragment, traversal, empty segments or backslashes; maximum 2048 chars |
| Part metadata | Declared counts refer to actual read rows, not the deduplicated event count |

### Curtailment row schema and merge behavior

The inspected `CurtailmentParquetSchemaV2` declares these exact columns:

```text
WindfarmId, WindfarmName, WtgId, WtgName, SchemaVersion, ExtractionToken,
StartDateAndTimeUTC, EndDateAndTimeUTC, LostProductionCurtailmentKwh,
SiteCurtailmentLevelKw, WtgCurtailmentLevelKw, CurtailmentTypeLvl1,
CurtailmentTypeLvl2, LastUpdatedUtc, CurtailmentActive
```

Do not normalize spelling/capitalization in the writer without a payload migration. Confirm physical types/nullability from the actual writer/reader; the column-name declaration alone is not a full Parquet schema.

Rows must occupy aligned, exactly ten-minute UTC slots. Preserve both curtailed and non-curtailed rows and the source CurtailmentTypeLvl2 projection. Harmonizer merges by WTG/date/slot: newer LastUpdatedUtc wins; identical repeats are no-ops; older data cannot overwrite newer; different values at the same source version fail explicitly. Full-day completeness requires 144 slots. No delete feed, omission-based removal or Curtailment event aggregation is implemented in the inspected package.

### Outage-specific requirements

Canonical EventId comes from OutageClassificationId, never descriptive OutageId. Null end means an open event. Keep live and deleted payload kinds distinguishable and require tombstone flags to agree with the declared feed. Raw ingestion does not know all old affected dates after an event moves, shortens or is deleted; Harmonizer compares old/new canonical footprints.

Current shared Outage refresh is **ReceiptOnly**. Absence removal requires independently verified source cutoff/completeness semantics; observation time and extraction token cannot provide that authority. Preserve the source overlap predicate, including its actual endpoint comparison, rather than silently replacing it with a generic mathematical interval rule.

## 8. Fan-Out work and Harmonizer compatibility

Daily series and events use distinct work shapes. The Ingestor publishes Raw metadata; Fan-Out owns ArtifactId/BatchId generation and work serialization. Do not make Ingestors produce WTG-day work or event footprints.

`CurtailmentWtgDayWorkV3` has `SchemaVersion=3` and carries source/farm identity, DataDate, mode/payload schema, RequestId, AttemptId, ExtractionToken, exact RawPaths, `Wtgs` and `ScopeWtgs` (numeric WtgId plus WtgName), readiness/empty flags, removal-authority fields, original message body, optional trace and operation IDs. The **consumer work** has DataDate even though the Raw envelope does not.

Production work retains its established wire field names (`ItemTurbines`/`ScopeTurbines` in the saved V3 serializer); Curtailment uses `Wtgs`/`ScopeWtgs`. These are source-specific DTOs. Do not assert that they already use one interchangeable wire format. Reusing daily-series processing semantics does not require renaming every serialized field now.

For events, EventKey identifies the source/farm/event and MemberKey includes WTG membership. Freeze contributing Raw paths. Snapshot barriers/receipt work wait for required durable batch receipts; no consumer may depend on arrival order.

The saved Fan-Out design persists exact outgoing bodies before send. Retry/redrive reuses those identities and bodies, not a newly planned roster. Preserve persisted V2/V3 bodies and compatibility readers through their required replay lifetime.

## 9. Publication, checkpoint and failure semantics

1. Establish the root OperationId and create run evidence under the current lease/fence.
2. Freeze query bounds and source-specific paging state.
3. Read/validate source data and finalize immutable Parquet parts.
4. Incremental: publish each completed part under its actual window/part identity. Commit that window's checkpoint only after all required handoffs succeed.
5. Refresh: after every requested window succeeds, publish one supported completion declaration. Current Curtailment refresh leaves the incremental cursor unchanged.
6. Finish run evidence under the current lease. Never let a stale owner advance the cursor or mark success.

Previously committed incremental windows may remain committed if a later window fails; do not describe the entire run as one atomic checkpoint update. Current supplied Curtailment follows per-window publication/commit.

Database and SQS writes are separate. A crash after send but before checkpoint commit can produce duplicates. Stable capture identities, immutable references and consumer receipts support recovery; this document does not claim exactly-once transport or a distributed transaction.

| Failure or replay case | Expected outcome |
|---|---|
| Part write fails | No notification advertises that part; affected window does not advance |
| Second part send fails | Window checkpoint does not advance past the unsent part; recovery tolerates already-sent duplicates |
| Refresh window fails | No completion declaration says the whole refresh succeeded |
| Inline completion exceeds byte budget | Fail completion/run; bounded retry/request strategy, no false success |
| Duplicate/out-of-order message | Receipt/version semantics prevent duplicate effects or stale overwrite |
| Unknown schema, invalid scope or count mismatch | Fail validation; no partial Fan-Out plan publication |
| Lease expired | Reject commit; preserve recovery evidence |
| Dead-letter replay | Preserve original correlation and frozen bodies; replay request is not proof of recovery |

## 10. Compatibility register: do not silently reconcile these

| Finding | Source evidence | Required handling |
|---|---|---|
| Production query-axis names differ | Older Production parser uses LastUpdatedDate/ProductionTimestamp; common validator uses LastUpdatedUtc/ProductionTimestampUtc | Identify active route and fixtures; producer/reader migration together before changing strings |
| Production refresh has multiple supported shapes across snapshots | Older ExtractionReady reader, latest common external-manifest profile | Preserve active reader; verify producer choice instead of assuming all snapshots are the same release |
| Curtailment inline completion differs from older manifest direction | Latest archive README, runner and shared validation | New inspected Curtailment runs use inline completion; retain required pointer reader compatibility |
| Old Curtailment V1 type still describes events | Legacy class in latest archive | Do not use that comment/type as the current source model |
| Alarm lacks a source watermark | Saved RawAlarmBatchAvailableV1 | Do not fabricate LastUpdatedUtc or advertise authoritative replacement; define lifecycle/version evidence first |
| Adjustment common envelope not implemented in current validator | Supported SourceType checks and unresolved Adjustment integration | Define aggregate revision/member semantics and supported ingress before extending the validator |
| OperationId is nullable at DTO level | Shared OperationCorrelation and runner requirements | New run propagation must be checked explicitly; legacy absence handling remains a distinct policy |

The archive may contain legacy compatibility DTOs alongside active producers. This register describes evidence, not an instruction to refactor all existing sources now.

**Proposed future alignment:** use common metadata and validation for new producers, preserve explicit daily-series/event profiles, and expose only refresh forms consumers actually support. Extend Alarm/Adjustment deliberately when their source contracts are known. A single forced refresh representation for every existing Ingestor is not decided here.

## 11. Contract verification and rollout gate

These are acceptance criteria for message-affecting implementation work; they have not been rerun as part of creating this document.

| Verification | Minimum fixture/outcome |
|---|---|
| Real serialization | Producer-generated JSON, exact casing/types/discriminators accepted by actual Fan-Out parser |
| Real Parquet | Writer output consumed by Fan-Out identity reader and actual Harmonizer source reader |
| Multiple query windows | Window 2 emits QueryWindowNumber=2; each part maps to its true window |
| Cross-date incremental changes | One update window contains two business dates; Fan-Out plans both correct WTG-days |
| Equal-timestamp pages | Tie-breaker includes all rows once logically; overlap/repeat remains duplicate-safe |
| Curtailment status changes | Active→inactive and inactive→active both captured and merged |
| Empty refresh | Explicit completed scope with zero parts; no unintended removal |
| Populated/disjoint refresh | All exact parts/counts/roster accepted; gaps in requested dates remain gaps |
| Partial publication | Failed required part/completion cannot advance the affected checkpoint or mark full refresh success |
| Identity/source version | Numeric-looking names remain names; newer/older/repeat/equal-version conflict cases work |
| Event lifecycle | Outage shorten/move/delete cleans old footprint; delayed snapshots do not erase newer events |
| Correlation | Same OperationId survives Raw → work → telemetry/lineage; no fallback to run/attempt ID |
| Invalid contract | Unknown schema/axis, incompatible reference forms, duplicate paths, incomplete windows and mismatched counts rejected |
| Size and recovery | UTF-8 body limit enforced; retry uses immutable artifacts and frozen work; legacy reader replay still works |

Run only relevant source cases and existing affected consumer regressions. Live database lease/commit behavior and infrastructure validation require separate evidence; fake repositories are not proof of PostgreSQL transactions.

Deploy required consumer support before enabling the producer. Preserve old readers until the agreed queue/DLQ/persisted-replay horizon is satisfied; queue retention alone may be shorter than durable replay storage. Breaking discriminator, field-type, authority or interpretation changes require an explicit version/migration decision.

## 12. Developer contract card and handoff checklist

Complete this in the component Micro-RPI or PR and link this document. Source-specific SQL details belong in the source contract/adapters, not duplicated in a message DTO.

```markdown
Source / component:
Inspected source revision and consumer revisions:
Shape: daily series / event / aggregate-membership
Stable business key and source version evidence:
Incremental query axis, bounds and paging tie-breaker:
Refresh predicate, exact scope and frozen roster:
Status/deletion/absence rules:
Raw Parquet schema version + writer/reader mapping:
Raw envelope profile + exact payload kind/query-axis strings:
Refresh form: existing supported completion / manifest / inline
RunId / AttemptId / ExtractionToken / OperationId mapping:
Fan-Out output DTO/version and Harmonizer reader:
Empty result, maximum size and retry/checkpoint behavior:
Producer JSON + real Parquet fixture location:
Validation evidence and unresolved gaps:
Consumer-first rollout / compatibility plan:
```

- [ ] Source shape selected from actual source data, not copied from another component.
- [ ] Existing shared DTO/conventions/sender reused where supported.
- [ ] Actual consumer accepts both serialized message and real Parquet.
- [ ] Query-window/part metadata, completeness and correlation checked.
- [ ] Positive and negative status changes/deletion semantics are covered.
- [ ] Required failure/replay and legacy compatibility cases pass.
- [ ] Unresolved behavior is visible before producer enablement.

For another developer or AI session: supply the component source contract, this document, active shared contracts and consumer reader files together. Ask implementation to preserve the chosen profile and demonstrate the fixture, rather than letting it redesign downstream contracts independently.

## 13. Discovery backlog and evidence

| ID | Open question / next action | Related platform question |
|---|---|---|
| MC-001 | Pin actual Production producer/reader profile, query-axis strings and refresh shape in the active repository | Q-014 |
| MC-002 | Integrate/verify latest Curtailment package against the active shared library and consumers; preserve its inline completion semantics | Q-009 |
| MC-003 | Establish Alarm stable identity, ordering/version evidence and Discovery/Polling/Historical completion semantics | Q-009 |
| MC-004 | Define Adjustment outbox→ingress wire shape, complete aggregate revisions, membership cleanup and root operation propagation | Q-015 |
| MC-005 | Prove Outage refresh authority only if omission deletion is needed; keep ReceiptOnly meanwhile | Q-006 |
| MC-006 | Pin actual downstream package-readiness notification and ACK boundaries without equating Raw completion with serving publication | Q-007/Q-008/Q-013 |

**Evidence inspected for this document:** current `platform-discovery.md` (version 4), `raw-ingestion.md` (version 1), `raw-fan-out.md`, saved `ProductionEnvelopeParser.cs`, `OutageEnvelopeParser.cs`, `ProductionWorkSerializer.cs`, `RawAlarmBatchAvailableV1(1).cs`, and latest `Curtailment.zip` (version 2, modified 12 October 2026). The archive's README, active shared DTOs/conventions/schema declarations, Curtailment publisher and runner evidence were inspected. Exact field rules above are grounded in those files, not inferred from filenames.

**Verification limit:** The archive README reports prior .NET builds and 48 passing tests; those are package-provided results, not tests executed during this documentation task. No current Git branch, live source database, PostgreSQL, SQS, Data Lake or EKS deployment was inspected. This document cannot guarantee no future refactor, but it moves contract decisions and compatibility checks ahead of implementation and makes the known differences explicit.
