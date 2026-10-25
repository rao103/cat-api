# Outage source queries and data contract

> Updated: 2026-10-11 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## 1. Scope and identity

Source evidence: `OutageSqlQueryBuilder.cs` and `SqlOutageSourceAdapter.cs` inspected in this edition. Accepted baseline: DL-007. The classification key is `OutageClassificationID`, normalized as `EventId`. `OutageID` is descriptive metadata, not the merge key. One classification belongs to one WTG; Adjustment is different.

| Stream | Source table | Purpose |
|---|---|---|
| Live | `cat_gold.OutageClassification` | New/changed and current classifications |
| Deleted | `cat_gold.OutageClassificationPeriodDeleted` | Tombstones carrying classification identity and version |

## 2. Incremental queries

Live and deleted streams share half-open updated-time bounds `[low, end)` and resume strictly after `(LastUpdatedUTC, OutageClassificationID)`. They are ordered by that tuple, not WTG or a fabricated numeric row ID. WTG membership filtering does not change the windfarm-level cursor identity.

```sql
-- Live route: selected fields are listed in section 4.
SELECT TOP (@batchSize) ...
FROM cat_gold.OutageClassification
WHERE SiteRDSPPID = @siteRdsppId
  AND WtgRDSPPID IN (@t0, @t1 /* additional bound parameters */)
  AND LastUpdatedUTC >= @low AND LastUpdatedUTC < @end
  AND (OutageClassificationStartUTC > @minRetrieveDate
       OR (OutageClassificationStartUTC < @minRetrieveDate
           AND OutageClassificationEndUTC > @minRetrieveDate)
       OR (OutageClassificationStartUTC < @minRetrieveDate
           AND OutageClassificationEndUTC IS NULL))
  AND (LastUpdatedUTC > @afterUtc
       OR (LastUpdatedUTC = @afterUtc
           AND OutageClassificationID > @afterOutageClassificationId))
ORDER BY LastUpdatedUTC, OutageClassificationID;
```

This is an abbreviated contract excerpt, not directly executable SQL. Continuation is omitted on the first page. The saved retention predicate excludes start exactly equal to the floor; preserve that observed legacy behavior until a deliberate change is agreed.

Deleted query selects `SiteRDSPPID`, `WtgRDSPPID`, `OutageID`, `OutageClassificationID`, `LastUpdatedUtc` from the deletion table. It uses the same site/membership/window/continuation shape without the live event retention predicate. Zero selected turbines yields `1 = 0`.

Process live artifacts before deleted artifacts. SQS can reorder or duplicate deliveries, so Harmonizer must still compare source versions.

## 3. Refresh query and authority

The saved current-state query does not filter by LastUpdatedUTC. It selects current events by site, membership, the legacy retention predicate and:

```sql
OutageClassificationStartUTC < @scopeEndUtc
AND (OutageClassificationEndUTC IS NULL
     OR OutageClassificationEndUTC >= @scopeStartUtc)
```

It orders by `(WtgRDSPPID, OutageClassificationID)`. It includes events starting before the requested scope and open events. Note the observed `>=` end comparison: the aligned Fan-Out uses strict half-open overlap and excludes a row ending exactly at snapshot start. That source over-selection is distinct from actual daily contribution. Q-014 records the boundary for review.

The latest aligned shared refresh route is **ReceiptOnly**. A validated complete manifest proves capture/receipt scope and parts, but does not authorize removing omitted events. The authoritative route requires explicit verified `SourceVersionCutoffUtc`. Host observation time and extraction token are not substitutes. See DL-022 and [event management](harmonizer-event-management.md).

The adapter explicitly warns that streaming a scoped SELECT is not proof of a repeatable source snapshot. Latest updated time uses MAX across both feeds and inherits an unverified assumption about source timestamp/commit ordering. Treat deletion-feed retention and safe-cutoff evidence as source-contract requirements.

## 4. Normalized fields

| Source | Normalized meaning |
|---|---|
| `OutageClassificationID` | `EventId`, canonical classification identity |
| `OutageID` | Descriptive `OutageId`; string conversion supports source scalar identity types |
| `SiteRDSPPID`, `WtgRDSPPID` | Source names; map via catalog to numeric work IDs |
| `OutageClassificationStartUTC` | Original event start |
| `OutageClassificationEndUTC` | Nullable original end; null remains open |
| Comment / AlarmCode / AlarmText / Category / SubCategory | Preserved classification attributes |
| `LastUpdatedUTC` / `LastUpdatedUtc` | `LastUpdatedDate` and source version/watermark |

Live normalized status is Open or Closed. Deleted rows are `IsTombstone=true`, status Deleted; they do not carry valid event start/end fields. Do not turn the tombstone model's default start into a real business interval.

Legacy normalized model names `WindfarmId` and `ItemId` can contain source-name strings. Active payload schema 2 uses numeric IDs and separate names; legacy schema 1 remains readable through catalog resolution. Numeric-looking names must not be cast into internal IDs.

## 5. Raw and handoff

Keep live and deleted artifacts distinct: `raw/outage/windfarm=WOW01/extraction=42/events/...parquet` and `deleted-events/...parquet`. Raw is immutable. Ready evidence references exact contributing parts. The shared refresh pointer resolves `RawSnapshotManifestV1`; validate identity, OperationId, scope, completion and part counts.

Fan-Out emits stable event/member batches plus any scope receipt barrier. It does not infer all old affected WTG-days from the new source rows. Harmonizer owns that lifecycle knowledge.

## 6. Recovery and checks

A newer tombstone removes live state; a stale live row cannot resurrect it; an actually newer live version can supersede the tombstone. Equal-version conflicting content is an error. Never infer deletion from a failed/partial read.

Required scenarios: open outage; duplicate live/delete; delete-first arrival; stale live after delete; newer live after delete; shortened/moved interval; exact scope-boundary overlap; empty receipt-only refresh; delayed authoritative snapshot with newer live data; membership mismatch; expired deletion-feed history. Source consistency and actual end-to-end execution remain unverified.
