# Testing Standards

## Principle

Test behavior and contracts, not implementation trivia.

## Unit tests

Use unit tests for:

- business rules,
- transformation rules,
- branching behavior,
- edge cases,
- deterministic ordering,
- validation,
- source-version comparisons,
- idempotency decisions.

Avoid tests that merely verify that one private implementation method called another.

## Integration tests

Use integration tests for boundaries whose real behavior matters:

- PostgreSQL,
- SQL Server where relevant,
- SQS-compatible behavior,
- Blob/object storage,
- Parquet serialization,
- migrations,
- transaction/concurrency semantics.

Do not mock everything merely to keep tests fast.

## Distributed failure tests

For workers, test at least the relevant subset of:

- duplicate message,
- retry after persistence success but before ack,
- publish timeout,
- stale/out-of-order message,
- partial external failure,
- cancellation,
- poison message,
- concurrent processing.

## Contract tests

Versioned message/file contracts should have serialization round-trip and compatibility coverage.

## Database tests

Verify:

- constraints,
- migrations,
- idempotent writes,
- transaction behavior,
- fencing/concurrency behavior.

## Naming

Test names should describe observable behavior.

Prefer:

```text
DuplicateMessage_DoesNotCreateDuplicateBatch
OlderEvent_DoesNotOverwriteNewerDeletion
FailedPublish_DoesNotAdvanceCursor
```

over:

```text
TestMethod1
Handle_ReturnsTrue
```

## Coverage

Coverage is a signal, not the goal.

Do not write low-value tests only to increase percentage.

Critical distributed invariants deserve tests even if line coverage is already high.

## Completion evidence

An AI-generated implementation should report:

- projects built,
- tests run,
- failures,
- tests added,
- anything not verified.
