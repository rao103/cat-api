# Database and Persistence Rules

## SQL safety

- Always parameterize SQL.
- Do not build SQL by concatenating untrusted or runtime data.
- Keep commands explicit and reviewable.

## Transactions

Transaction boundaries must correspond to real consistency boundaries.

Do not open a broad transaction across:

- external APIs,
- SQS,
- blob storage,
- long-running CPU work.

Do not attempt distributed transactions across unrelated platforms unless the architecture explicitly requires and supports them.

## Schema changes

Use the repository-approved migration mechanism.

A migration must be:

- forward-reviewable,
- safe for deployed code compatibility where required,
- testable,
- documented when backfill or phased rollout is required.

## Timestamps

Use UTC for persisted technical timestamps.

Prefer clear suffixes such as:

```text
CreatedUtc
UpdatedUtc
StartedUtc
CompletedUtc
```

or the repository's established equivalent.

Business-time fields may use domain-specific names such as event start/end.

## Concurrency

When correctness depends on ownership or ordering, use explicit mechanisms such as:

- lease tokens,
- fencing tokens,
- optimistic concurrency,
- unique constraints,
- version columns.

Do not rely only on “the worker probably runs once.”

## Idempotency

Persistence APIs used in retryable workers must define duplicate behavior.

A duplicate operation should be one of:

- no-op,
- return existing result,
- deterministic update,
- explicit conflict.

It must not create hidden duplicate business state.

## Database exceptions

Do not swallow:

- uniqueness violations,
- concurrency failures,
- foreign key failures,
- timeout errors.

Classify and handle them according to whether they are expected, transient, or invariant violations.

## Repositories

Use repositories/capability stores where they express meaningful persistence boundaries.

Do not create one repository interface per table automatically.

## Migrations and audit

Audit columns and actor values should follow a consistent platform strategy.

If audit fields are required, creation fields should normally be immutable after insert.
