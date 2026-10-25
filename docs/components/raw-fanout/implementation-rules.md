# Raw Fan-Out Component Rules

These rules extend the shared platform standards.

## Primary responsibility

Raw Fan-Out translates durable Raw availability into bounded downstream work.

It discovers the actual identities affected by Raw data and emits Harmonizer work.

It does not perform Harmonizer merge/reconciliation logic.

## Core invariant

Raw Fan-Out is a dispatcher/planner, not the final business-state authority.

## Production and similar ten-minute data

For Production and sources whose Raw rows directly identify affected day membership:

```text
Raw extraction
→ discover WTG-day
→ emit bounded WTG-day harmonization work
```

`DataDate` comes from actual business timestamps in Raw rows, not from extraction update windows.

## Event sources

For Outage, Alarm, Adjustment, and similar lifecycle/event data:

- Fan-Out must preserve stable event/member identity and source scope.
- It must not pretend it can derive the complete old + new footprint from only the latest event row.
- Cleanup of prior footprint/state belongs to the downstream component that owns canonical event state/reconciliation.

Fan-Out may emit bounded date-scoped/event-scoped work as defined by the accepted event design, but must not invent historical state it does not possess.

## Idempotency and state

Fan-Out must survive duplicate parent and child deliveries.

Use persisted state for planning/dispatch.

Existing control concepts include:

- `raw_artifact_dispatch`
- `raw_artifact_batch`
- failure/dead-letter state

A duplicate RawAvailable/ExtractionReady message must not create duplicate logical downstream batches.

## Batch identity

Batch identity must be deterministic within the selected parent plan.

Preserve repository-defined artifact/batch keys and ordinals.

Do not redefine batch identity casually because downstream idempotency depends on it.

## Planning before dispatch

Persist the planned work before or together with dispatch-state transitions according to the existing repository transaction model.

Do not infer “published” solely from process memory.

## Partial failure

If planning or dispatch fails:

- preserve enough durable state to resume/retry;
- do not silently drop remaining batches;
- do not mark parent completion until completion invariants are satisfied.

## DLQ behavior

Permanently invalid or exhausted work must be diagnosable.

Persist:

- original payload where allowed,
- error classification,
- attempt count,
- correlation identifiers,
- audit actor/timestamps.

Do not log or persist secrets unnecessarily.

## Audit

Where audit columns are part of the schema:

- creation fields remain immutable,
- actor values are nonblank and service-specific,
- updates stamp modification fields consistently.

## Concurrency

Multiple worker replicas may process concurrently.

Correctness must come from database uniqueness/state transitions, not assumptions of single-process execution.

## Non-goals

Raw Fan-Out must not:

- write Harmonized component revisions,
- calculate loss,
- own event canonical lifecycle state,
- infer old event footprint without durable prior state,
- turn refresh manifests into partial authoritative data.
