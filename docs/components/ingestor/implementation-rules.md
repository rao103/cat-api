# Ingestor Component Rules

These rules extend the shared platform standards.

They apply to Production, Outage, Alarm, Curtailment, and similar ingestion workers unless a source-specific specification overrides them.

## Primary responsibility

An ingestor moves authoritative source data into immutable Raw artifacts and publishes downstream availability.

It does not perform Harmonizer or Calculator responsibilities.

## Standard lifecycle

Typical pull-based flow:

```text
select eligible extraction_cursor
→ acquire lease / fencing token
→ create extraction_run
→ determine fixed source query boundary
→ read source data in bounded windows/batches
→ write immutable Raw artifacts
→ publish required Raw-available notifications
→ commit cursor last
→ mark run outcome
```

## Cursor rules

For pull-based sources:

- `extraction_cursor` is the operational checkpoint and lease authority.
- `extraction_run` is audit/history, not a work coordinator.
- Cursor advancement occurs only after all required work for the logical execution succeeds.
- Cursor commits must be fenced.
- Failed execution must not advance the committed checkpoint.

## Incremental vs refresh

These are different semantics.

### Incremental

- driven by source change ordering/checkpoint fields;
- advances incremental checkpoint only on full success.

### Refresh

- driven by explicit historical/business scope;
- creates a new run/extraction identity;
- must not rewind or advance normal incremental checkpoints unless explicitly designed to do so.

Do not treat refresh as “incremental with a different date range.”

## Raw artifacts

- Raw artifacts are immutable.
- Retry/re-execution uses a distinct extraction/run identity when required by the existing architecture.
- Raw path naming must follow the approved platform path convention.
- Do not infer business-day partitioning from source update windows.

## Production-specific principle

Production changes may be selected by update time while representing older production timestamps.

Fan-Out/Harmonizer determine affected WTG-days from actual row business timestamps, not from the extraction window label.

## Event-source principle

Event sources such as Outage/Alarm/Curtailment use stable event identity and source version/order metadata.

Do not force them into a simplistic “one source row equals one WTG-day” mental model.

## Outage main + delete feed

Where a source has a separate delete feed:

- both feeds belong to one logical source;
- each feed may have its own checkpoint pair;
- do not invent a separate business source type solely for deletion;
- commit all checkpoint movement only after all required feeds succeed.

## Outage refresh

Authoritative refresh:

- queries by event/business-time overlap;
- may contain zero events and still be authoritative;
- writes complete immutable snapshot artifacts/manifest;
- publishes only after the complete scope is durable;
- leaves incremental checkpoints unchanged.

## Concurrency

One lease owner owns the logical source + windfarm execution scope.

Internal bounded parallelism is acceptable where already designed.

Do not create a coordinator service or per-window work ledger merely for symmetry unless explicitly approved.

## Required observability

Track relevant:

- source type,
- windfarm,
- run/extraction token,
- operation/execution,
- mode,
- lease/fencing identity,
- artifact count,
- rows read,
- failure outcome.

## Non-goals

An ingestor must not:

- calculate loss,
- build Harmonized revisions,
- publish serving rows,
- use Raw path identity as business source ordering,
- rely on distributed transactions across source DB + blob + SQS + PostgreSQL.
