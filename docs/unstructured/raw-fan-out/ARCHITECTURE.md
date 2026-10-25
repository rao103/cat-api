# Artifact-scoped fan-out completion

## Architecture contract

The existing ingestor artifact message is the extraction-completion signal for exactly the files it references. No second producer message, run manifest or producer completion flag is required. All shards must be fully written and immutable before the existing message is sent. A monthly incremental change batch is a completed batch, not necessarily a complete monthly snapshot.

Project boundaries remain unchanged: `RawFanOut.Worker` hosts/configures queue consumers; `RawFanOut.Core` owns planning, provider adapters and PostgreSQL persistence; shared Contracts contains the outgoing work contract. Tests remain under `tests/Raw`. A single deployment handles all sources with multiple replicas. Each replica can process any artifact; no source is permanently assigned to a pod.

The flow is: parse existing message -> validate/read every shard -> atomically persist the complete child plan -> publish the persisted child messages -> record fan-out completion -> delete the Raw receipt. Harmonization acknowledgement is a separate downstream transaction. No SQL transaction is held open during SQS or Blob operations.

## Two completion states

| Field in control.raw_artifact_dispatch | Writer | Meaning |
|---|---|---|
| expected_batch_count / plan | Fan-out | Exact complete work set committed before the first publication |
| fan_out_completed_at_utc | Fan-out | Every child message was accepted by SQS |
| harmonization_completed_at_utc | Acknowledgement/dispatch SQL | Fan-out is complete AND every expected child has committed its required harmonization/materialization work |

The child plan is immutable. Concurrent replicas and retries use the first committed plan even if batch-size configuration changes. Duplicate delivery may republish already accepted work. Downstream idempotency is required. A database failure or partial publication leaves the parent undeleted; a retry repeats safe work. If all downstream acknowledgements arrive before fan-out records its marker, marking dispatch re-evaluates the barrier. The reverse order works too.

There is no completion queue message that could overtake data. Harmonizer reads the artifact and child state by ArtifactId; its batch acknowledgement re-evaluates completion atomically. A separate orchestrator can query the completed state later. No state-change notification dispatcher is included.

This small artifact/batch ledger is an explicit extension to v11's original ledger-free implementation, to implement the requested durable fan-out completion flag. It does not recreate external Raw-file lifecycle orchestration or whole-ingestor-run completeness.

## Incoming and outgoing data

Incoming JSON remains PascalCase. Production alone requires DataDate. BlobPath and ShardPaths are mutually exclusive; all listed shards belong to one artifact. RunId, ExtractionToken and source/window metadata remain in the original envelope. No producer needs to calculate ArtifactId: the coordinator does it using the shared deterministic helper. It includes RunId and normalized sorted paths. Producer retries should preserve the original metadata and immutable files; altered envelopes under an existing identity are rejected.

Production reads WindfarmId/ItemId/ProductionTimestampUtc and emits Item-Day identity batches. Alarm reads WindfarmId/ItemId/int64 AlarmId. Outage and Curtailment read WindfarmId/ItemId/string EventId. Event tombstones remain in the identity set. Events are dispatched by canonical event identity: the Harmonizer and lifecycle coordinator derive affected old/new Item-Days after canonical merge, rather than assigning the extraction window as the event date.

Adjustment reads v11's parent WindfarmId/string AdjustmentId, without requiring ItemId on the parent. The full parent/current membership remains in Raw for downstream diffing. Its actual Parquet encoding was not supplied; align the exporter with this explicit assumption. Membership-only files with a different schema cannot be passed as ordinary ShardPaths in this reader; they need a separate reference/reader extension. Cancellation and zero current members must retain the parent identity so removed Item-Day data can be cleared. Adjustment uses no DataDate in the supplied envelope reader.

| WorkKind | Work required before acknowledgement |
|---|---|
| ProductionItemDays | Fenced publication of the selected production components and input state |
| ProductionSnapshotReconciliation | Reconcile expected membership against the whole daily artifact; missing production follows the configured missing/partial-data policy |
| CanonicalEvents | Canonical merge plus all resulting old/new Item-Day materialization and cleanup |
| EventSnapshotReconciliation | Reconcile according to the original batch/window/snapshot semantics; never interpret an empty incremental as delete-all |

Reconciliation work can arrive before ordinary child work. It must be independently idempotent, derive its decisions from the complete artifact/canonical state and respect source/component fencing. It must not infer missing data merely from sibling batches that have not yet committed. If the implementation needs sibling dependency ordering, implement that in the future Harmonizer without acknowledging early.

## Empty artifacts and scope

A successfully empty extraction publishes the same existing message referencing a valid zero-row Parquet artifact. Fan-out emits reconciliation work even with zero identities; the plan is never empty. Successful reconciliation must be acknowledged before artifact harmonization is complete. Silence on the queue is not evidence of a successful empty extraction.

An empty incremental batch preserves unchanged canonical events. A complete authoritative snapshot can support absence reconciliation within its declared scope. Previously known open events, tombstones, old membership and previous materialization state still participate. No-event readiness for a particular Item-Day requires the Harmonizer to establish applicable source coverage and canonical emptiness, then publish `Complete`, record_count=0, component_path=NULL where appropriate. Missing production is not automatically valid zero production.

The coordinator does not invent business-date coverage for event envelopes that do not carry it. It does not claim completion of a whole ingestor run from one of that run's artifacts. The absence of sufficient scope/coverage metadata must remain unresolved in the downstream readiness policy, not be replaced with the ingestion timestamp.

## Harmonizer acknowledgement contract

The Harmonizer, Event Lifecycle Coordinator and calculation orchestrator remain future components. ArtifactId and BatchId are already on HarmonizationWorkV1. For each child, call `control.raw_artifact_ack(artifact_id,batch_id,actor)` (actor is a stable configured service identity, e.g. `harmonizer`) in the SAME transaction as the last durable state commit for all required work. Upload immutable output before that short pointer/state transaction. Acquire the parent artifact row lock before component locks consistently to avoid conflicting lock orders.

A canonical-event merge alone is not sufficient acknowledgement if asynchronous lifecycle work remains. The future lifecycle implementation must retain that dependency and acknowledge only after all required old/new Item-Days are materialized through its finite processable horizon. Duplicate acknowledgements are harmless; unknown batches fail; a transaction rollback rolls back both state and acknowledgement.

This marker does not replace item_day_input_state or package-revision consistency. Calculation readiness still uses required Item-Day inputs, known invalidations and current component revisions. A later changed artifact advances affected component/package revisions and triggers recalculation; an unchanged artifact should not manufacture a revision. Never use an old completed artifact to ignore newer pending work. Open events still need the clock-driven lifecycle continuation described in v11.

## Recovery and rollout

PostgreSQL is required before publication. Parent replay uses frozen plan IDs/bodies but still validates/reads the referenced Raw files, so retain them for the recovery period. Path immutability is a producer obligation; an ETag guard protects each individual download but does not prove an object was never overwritten between retries.

Raw DLQ handling archives the full envelope once per queue message in `raw_fan_out_dead_letter`, the single failure store. Replay republishes from the archive and records `replay_requested_at_utc`; it does not resolve the incident. An incident is closed only by an explicit operator closure with a reason once recovery is verified, never merely because republishing succeeded or calculation is pending. Durable batch bodies allow targeted downstream recovery; no automatic republication job is included. Retention (see SCHEMA.md) removes completed artifact ledgers six calendar months after harmonization completes and resolved incidents after their own retention period; unresolved incidents and incomplete artifacts are never deleted. Retain ledger rows through all queue/DLQ/replay windows. Alert on planned-but-undispatched artifacts, unacknowledged children and unresolved incidents.

Deploy the schema to a fresh database with `src/db/migrations/0001`-`0003` (see SCHEMA.md). Existing ingestor artifact messages continue unchanged.

## Contract notes: OriginalMessageBody and scope naming

- `OriginalMessageBody` is a required member of the published `HarmonizationWorkV1/V2` and `ProductionItemDayWorkV2/V3` contracts and is written by `WorkBatchBuilder` and `ProductionWorkSerializer`. It stays until a versioned contract change removes it; the external Harmonizer, which cannot be inspected from this repository, may read it. Frozen batch bodies are never rewritten. A unit test pins the current behaviour.
- The incoming `ScopeWtgIds` field carries turbine NAMES and is still read as names (`ProductionExtractionReadyV1`, `RawEnvelope`). Names are resolved within the windfarm to numeric `WtgId` through the catalog; a name that does not resolve fails the artifact. Outgoing V2 `ScopeWtgIds` and V3 `ScopeTurbines` are numeric. A rename to `ScopeWtgNames` needs a coordinated publisher/parser change with compatibility tests and is deliberately not part of this refactor.
- `ItemTurbines` is the turbines of one outgoing batch; refresh `ScopeTurbines` is every queried turbine (including zero-row turbines) and remains available to reconciliation. Empty results still emit reconciliation work.
