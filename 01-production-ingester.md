# Production ingester — implementation plan

**Scope.** Extract actual production, possible production, active power and any other agreed 10-minute measurements as one Production input. Do not create a separate ingester for each measurement tag. Implements architecture §§4.3–4.9, 5, 6.4, 6.9, 8.6, 14.7, 15.1 and 16.3.

## Source contract and work unit

- Configure the source endpoint, windfarm mapping, per-source credentials/identity, UTC timestamp semantics, row key `Id`, change timestamp `LastUpdatedDate`, measurement units, mandatory tags, and maximum page/window size. Verify source indexes support windfarm + changed-time discovery and windfarm + UTC production-day read-back. Do not assume these capabilities without a source test.
- Partition the **claim** by `(WindfarmId, Production)`. Partition Raw **data** by `WindfarmId + UTC ProductionDate`. `LastUpdatedDate` drives discovery; `ProductionTimestamp` drives the affected date. One run can discover many historical days.
- Use `ExtractionCursor` with committed `(LastUpdatedDate, LastCommittedId)`, `LeaseOwner`, monotonic `LeaseToken` and expiry. Schedule normally at the configured freshness cadence; backfill uses the same worker with a separate concurrency cap. Lease heartbeat/renewal and fenced final commit are required for long runs.

## Execution sequence

1. Acquire the cursor lease atomically; reject a concurrent owner for the same windfarm/source. Read committed position and freeze a finite source high watermark **below any unsafe in-flight transaction horizon** as established with the source owner. Compute `effectiveLow = committed.LastUpdatedDate − SafetyLagInterval` and split `[effectiveLow, high)` into bounded LastUpdated windows; use half-open UTC windows.
2. Within each window query the windfarm once with `LastUpdatedDate >= low AND LastUpdatedDate < end`, ordered by `(LastUpdatedDate, Id)`. Keyset-page using both columns within the current run. A previous `LastCommittedId` does **not** filter the lag-overlap pages. Collect distinct UTC `ProductionDate` values from changed rows; spill a large date set to bounded temporary storage rather than loading an unbounded result.
3. Re-read each affected complete Windfarm-Day snapshot from source using `ProductionTimestamp >= dayStartUtc AND < nextDayStartUtc`. Deduplicate source rows by agreed natural key (typically ItemId + UTC timestamp + measurement/tag, subject to source schema); retain source version/watermark and extract time for lineage. A legitimately empty authoritative day is still a snapshot, never silently skipped. If full-day read-back cannot be supported, implement a separately reviewed delta/keyed-merge variant.
4. Parallelize **different days**, with a bounded per-source worker semaphore. Stream/paginate within a day and write size-controlled Parquet row groups; bound in-memory rows, source connections, Blob uploads and in-flight SQS sends. If one day exceeds the configured artifact limit, split deterministic shards and make fan-out consume all shards as one logical day. Avoid 1 query per turbine per day. Start with conservative concurrency and tune against p95 source latency, throttling and the desired catch-up time.
5. Stage a deterministic attempt at `/raw/production/windfarm=<w>/date=<UTC-day>/extraction=<leaseToken>/production.parquet` (or deterministic shard paths). Validate counts/schema/checksum, finish durable upload, then send a `raw-fanout` message containing `sourceType`, `windfarmId`, `dataDate`, `extractionToken`, `blobPath` and trace/run ID. When a source window yields multiple days, require every day's Blob write and message acceptance before committing that window's progress.
6. Fenced-update the cursor (`WHERE WindfarmId, SourceType, LeaseToken` and expected previous committed position). For multi-window catch-up, persist the last **fully handed-off** boundary only; never jump to the frozen high watermark when a window fails. Release the lease. A no-change window can advance its verified source horizon after successful query; log zero changed rows separately from zero production rows.
7. Fan-out enumerates active ItemIds in the Raw day (plus expected effective-dated membership for missing-item handling) and sends configurable Item-Day batches, e.g. benchmark 10/25/50/100/200. Harmonizer validates the full UTC day, builds complete Item-Day production components and the Windfarm-Day WTG aggregate, then commits `ItemDayInputState`. A missing turbine or interval is recorded as Partial/Missing/Invalid in `DataAvailability`, never automatically filled with zero output. `Complete` means the source extraction/normalization completed; readiness policy for a partially populated day must be explicit and visible.

## Source-to-Raw schema and lineage

At minimum retain `WindfarmId`, `ItemId`, `ProductionTimestampUtc`, `ActualProduction`, `PossibleProduction`, agreed measurements with units, `LastUpdatedDate`, `Id`, source-system row revision (if available), extraction time, schema version and extraction token. Keep source values distinct from normalized Harmonized values; reject or quarantine malformed timestamps, duplicate contradictory keys and invalid units with a reason tied to the Item-Day.

## Failure, historical refresh and acceptance

- Blob write failure or send failure: leave cursor unadvanced; replay under the same token while lease is valid or a new token after takeover. Send success followed by crash duplicates downstream work safely. A token-fenced cursor update affecting zero rows forbids further writes/commits from that owner.
- One malformed turbine-day: mark its `ItemDayInputState` `Failed`, retain previous revision and block readiness; the item sweep retries. Batch-wide source/Blob/queue failures retry the batch; a raw-fanout DLQ entry becomes `FanOutFailure`.
- Historical `RefreshAndRecalculate` reads **each requested UTC Windfarm-Day** from the authoritative source, even if its LastUpdated timestamp has not changed; uses new extraction token, preserves old Raw revisions and sends fresh fan-out. Pure calculation re-execution does not hit source.
- Prove with fixtures: equal LastUpdated timestamps across pages; a row committed after a prior poll but stamped inside safety lag; a correction to a date months before LastUpdated; 1,500 WTGs × 144 intervals; an empty/partial day; a stalled worker losing its lease; one SQS send rejected after earlier days succeeded. Reconcile per-Item-Day count and values to the upstream day snapshot, plus the Windfarm-Day total. Track source-contact time, cursor lag, changed-date count, read-back row count, `ExpectedCount ≈ 144` versus actual, DLQ and end-to-end publication lag.

## Delivery slices

1. Verify source contracts and load-test bounded discovery/day read-back; create cursor/lease and extract-without-publish proof.
2. Add immutable Raw writer, queue send-before-commit, retries and failover cases.
3. Integrate fan-out/Harmonizer, explicit completeness state, quality metrics and historical refresh; dual-run and compare with existing results before cutover.
