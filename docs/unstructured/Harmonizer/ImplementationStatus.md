# Implementation status — 6 October 2026

| Area | Status |
|---|---|
| Core + Worker only; existing source folder convention | Implemented |
| ProductionItemDayWorkV2 and V3; prior Production processing | Retained and covered by existing tests |
| Source strategy dispatcher and lifecycle strategy host | Implemented; Production and Outage registered |
| EventWorkV3 Outage reader, member identity, exact paths and catalog validation | Implemented from latest supplied Fan-Out contract |
| Actual normalized live/deleted Outage Parquet schema | Implemented; both supplied fixtures tested |
| Typed descriptive Outage output columns, null preservation and optional unknown attributes | Implemented |
| Canonical monthly revisions, tombstones and atomic month moves | Implemented |
| Event identity independent of WTG/month; LastUpdatedDate ordering | Implemented |
| Null-ended events, complete daily segments and exact canonical links | Implemented |
| Shortened/deleted/moved event cleanup and unrelated contributor preservation | Implemented |
| Durable compact ranges, bounded passes and restart continuation | Implemented |
| Frozen source completion, future closed events and autonomous open continuation | Implemented |
| Empty incremental observation vs scoped full snapshot absence | Implemented |
| Expected canonical batch receipt barrier; delayed snapshot protection | Implemented |
| Receipt-only snapshots and verified-cutoff authoritative absence fences | Implemented; upstream source cutoff capability remains a rollout prerequisite |
| Two added control tables, typed receipts and existing transactional outbox | Implemented |
| Farm/source lease and fencing across source/lifecycle pods | Implemented; real Npgsql concurrency tests require an external disposable server |
| Pending-day SQL function/view for Package Coordinator | Implemented; consumer integration required |
| UTC timestamped logs, operation scopes, OTel tracing/metrics and optional OTLP export | Shared host configuration retained; new runtime boundaries integrated |
| Separate Raw/Harmonized object store ports and clients | Retained |
| Packages / direct CalculationWork / serving publication | Owned by other components; not implemented here |

## Integration requirements outside this delivery

- The consolidated 0004 is a fresh-install baseline only. Existing deployments require reviewed forward migrations; do not apply the baseline over an existing Harmonizer schema. Do not rerun the Fan-Out reference schema in a real database.
- Route the latest EventWorkV3 Outage output only after deploying this consumer. Previously frozen legacy Outage V2 batches require a deliberate compatibility/redrive decision; this implementation does not reinterpret them as V3.
- Verify that Outage refresh and incremental captures share the same monotonic extraction cursor token namespace. LastUpdatedDate still orders each event mutation.
- Keep snapshot absence in `ReceiptOnly` mode until the producer supplies a verified source-version cutoff. `SnapshotObservedAtUtc`, `SourceWatermarkUtc`, extraction-token order and maximum selected-row timestamps are not source cutoffs.
- Package Coordinator must consult pending state, including when there is no current Outage row. Proposed outbox notification contracts must be aligned with its real consumer before enabling publishing.
- Alarm and Adjustment need their actual producer fixtures/mappings. Curtailment is a 10-minute WTG-day source, following the Production approach; its actual mapping is also pending. Unsupported work remains retryable and is not acknowledged as empty.
- Production destructive refresh remains unsupported without the agreed SourceVersionEvidence/source-as-of semantics. Existing non-removal refresh behavior is retained.
- Central operation status/progress writes, time-zone/DST output, retention/orphan collection, dedicated DLQ archive/redrive and external provider/load/deployment acceptance are separate work. Correlation is implemented; no central status API is invented.

There is no upstream completeness gate for a known empty Outage day. Technical failures and accepted unfinished work still block relevant readiness. This delivery is code for merging and validation, not a claim of live provider deployment.
