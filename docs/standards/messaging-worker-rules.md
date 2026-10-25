# Messaging and Worker Rules

## Delivery model

Assume at-least-once delivery unless the queue contract explicitly guarantees otherwise.

Therefore:

- duplicate delivery is normal,
- redelivery after a crash is normal,
- messages may arrive out of order,
- network timeout does not prove publish failure.

## Processing order

Default lifecycle:

```text
Receive
→ Validate
→ Establish correlation
→ Load durable state/input
→ Execute business operation
→ Persist durable result/state
→ Publish required downstream intent/message
→ Acknowledge/delete source message
```

Exact order may vary when an outbox or durable intent is used.

## Acknowledgement

Do not acknowledge/delete the source message until required durable work is complete.

If downstream publication must be atomic with DB state, prefer durable outbox/intent patterns rather than premature acknowledgement.

## Idempotency

Each handler must define its idempotency key or authoritative state comparison.

Examples:

- artifact identity,
- message contract identity,
- business event ID + source version,
- work item key + version,
- persisted dispatch row.

## Ordering

Do not treat arrival order as business truth.

Use source versioning / timestamps / tie-breakers / explicit revision information where ordering matters.

## Retries

Retry only transient failures.

Examples:

- temporary database connectivity,
- throttling,
- network interruption,
- service unavailable.

Do not endlessly retry permanent failures such as:

- unsupported contract version,
- invalid required field,
- unrecoverable schema mismatch.

## Poison messages

Use DLQ/dead-letter handling for permanently unprocessable messages.

Persist enough diagnostic context to investigate without exposing secrets.

## Visibility and long-running work

If processing can exceed queue visibility:

- renew visibility or lease,
- stop renewal after completion/failure,
- design renewal failure behavior.

## Versioned contracts

Wire contracts must carry a version when evolution is expected.

Do not silently change the meaning of an existing field.

## Shutdown

Workers must stop receiving new work on shutdown and allow active work to observe cancellation according to platform policy.
