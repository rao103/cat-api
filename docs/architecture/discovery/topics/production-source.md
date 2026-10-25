# Production source query and data contract

> Updated: 2026-10-11 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## 1. Scope and evidence

Production Ingestor reads `[cat_gold].[WtgPowerProductionContractual]` for a windfarm. Production is a 10-minute daily-series input. Raw Fan-Out plans WTG-day work; Harmonizer merges it into complete daily candidates. This file describes the saved source adapter, not a replacement SQL implementation.

Source evidence: `SqlProductionSourceAdapter(1).cs`, especially `GetWindfarmDaySnapshotAsync`, the change-discovery query, and the membership-filtered updated-window row query. Related: DL-001, DL-003, DL-013 and [Raw ingestion](raw-ingestion.md).

## 2. Query contracts

| Query route in saved adapter | Filtering | Paging / ordering |
|---|---|---|
| Latest updated time | Site; membership-filtered overload also exists | `MAX(LastUpdatedUtc)` |
| Change discovery | Site; `LastUpdatedUtc > @low AND LastUpdatedUtc < @end` | `(LastUpdatedUtc, WtgRDSPPID, StartDateAndTimeUTC)` |
| Windfarm-day snapshot | Site; start interval in `[dayStartUtc, dayEndUtc)` | `(StartDateAndTimeUTC, WtgRDSPPID)` |
| Updated-window full rows | Site + bound active-WTG parameters; start `> minRetrieveDate`; updated time `>= windowMin AND <= windowMax` | `(StartDateAndTimeUTC, WtgRDSPPID)` |

The updated-window route uses updated time as a filter, not its paging key. Business-row identity is WTG plus interval start. Keep that distinction when persisting continuation state.

### Representative day snapshot

```sql
SELECT TOP (@batchSize)
       pp.WtgRDSPPID, pp.StartDateAndTimeUTC, pp.EndDateAndTimeUTC,
       pp.ContractualPoPEEstimatedPowerProductionkWh, pp.ContractualPoPE,
       pp.ContractualPoPEColumn, pp.LostProductionCapacitykWh,
       pp.SiteAverageWindSpeed, pp.LastUpdatedUtc
FROM cat_gold.WtgPowerProductionContractual AS pp
WHERE pp.SiteRDSPPID = @siteRdsppId
  AND pp.StartDateAndTimeUTC >= @dayStartUtc
  AND pp.StartDateAndTimeUTC < @dayEndUtc
  AND (@hasContinuation = 0
       OR pp.StartDateAndTimeUTC > @continuationTimestamp
       OR (pp.StartDateAndTimeUTC = @continuationTimestamp
           AND pp.WtgRDSPPID > @continuationWtgName))
ORDER BY pp.StartDateAndTimeUTC, pp.WtgRDSPPID;
```

This excerpt mirrors the saved route. Exact executable SQL remains in the adapter. Bind site, turbine, date and batch values; do not concatenate source identity values into SQL.

## 3. Field contract

| Source field | Meaning / target handling |
|---|---|
| `SiteRDSPPID` | Source windfarm name; resolve numeric `WindfarmId` separately |
| `WtgRDSPPID` | Source WTG name; resolve numeric `WtgId` separately |
| `StartDateAndTimeUTC` | UTC interval start; merge key with WTG |
| `EndDateAndTimeUTC` | UTC interval end |
| `ContractualPoPEEstimatedPowerProductionkWh` | Production quantity; confirm business mapping through the row mapper |
| `ContractualPoPE` | Additional production quantity; confirm Actual/Possible semantics |
| `ContractualPoPEColumn` | Contractual source/selection metadata |
| `LostProductionCapacitykWh` | Loss-capacity value |
| `SiteAverageWindSpeed` | Source wind speed; target units/name must be verified |
| `LastUpdatedUtc` | Source row version timestamp |

The agreed vocabulary includes `ActualProductionKWh`, `PossibleProductionKWh`, `StartTimeInterval`, `EndTimeInterval`, `WtgRdsppId` and separate numeric IDs. Existing models contain legacy `ItemId` and `ProductionTimestampUtc`. A target-name list does not prove those columns already exist in the active Parquet writer. Verify writer/reader mapping before renaming fields.

## 4. Incremental and refresh flow

Incremental: claim the cursor → freeze query bounds → read changed identities/rows using the selected route → write immutable Raw parts → emit ready evidence → commit the checkpoint under the current lease. Follow the runner's actual route; do not combine several adapter overloads into a guessed checkpoint algorithm.

Refresh: read the requested business-time scope, split Raw into bounded windows/parts, and issue a complete logical ready message. The aligned shared route uses a `RawSnapshotManifestV1` pointer with identity, frozen WTG scope, completed intervals, parts and row counts. Fan-Out/Harmonizer validate it before treating an empty date as an explicit empty contribution.

Illustrative path: `raw/production/windfarm=WOW02/mode=refresh/extraction=58/window-20260201T000000Z-to-20260211T000000Z-part-0001.parquet`.

## 5. Downstream merge and readiness

Harmonizer merges by WTG/interval start, ordered by source `LastUpdatedDate`. Extraction tokens fence processing ownership and do not replace source row ordering. Newer corrections survive older deliveries.

A UTC day requires 144 distinct aligned 10-minute slots for Production Complete. Sparse input is Partial; do not synthesize missing zero rows. A durably accepted Partial contribution may be acknowledged without becoming calculation-ready.

The saved Harmonizer handles supplied refreshes with `AllowRemovalReconciliation=false`: present rows merge, absent rows remain. Replacement/removal refresh is unsupported until source-as-of authority is aligned. Receipt/completeness evidence is not automatic deletion authority.

## 6. Failure and recovery requirements

Retry the exact immutable Raw paths/frozen message; fence checkpoint and publication commits; do not acknowledge failed source reads as empty snapshots. Duplicate rows must merge deterministically. Same-version differing content must have an explicit conflict policy.

## 7. Open questions and verification

Q-002/Q-014: the adapter has different inclusive/exclusive timestamp predicates. The runner and current cursor DDL must show which route is active and how equal-timestamp boundaries avoid skips. `MAX(LastUpdatedUtc)` does not itself prove a consistent source cutoff; document late-commit/backdated-update behavior and any safety lag/overlap.

Verify Actual/Possible field mapping, active source uniqueness, membership changes, and writer schema. Meaningful scenarios: more than one page with equal timestamps, exact boundary timestamps, late correction, full/partial/empty day, delayed refresh following a newer incremental row, and replay after checkpoint commit failure.
