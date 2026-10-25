# Logging and Telemetry Rules

## Structured logging

Use message templates with named properties.

Preferred:

```csharp
logger.LogInformation(
    "Processing work {WorkId} for WTG {WtgId}",
    workId,
    wtgId);
```

Avoid interpolated log strings when structured properties are expected.

## Correlation context

Include identifiers relevant to the operation, such as:

- `operation_id`
- `execution_id`
- `run_id`
- `work_version`
- `attempt`
- `extraction_token`
- `message_id`
- `windfarm_id`
- `wtg_id`
- `source_type`

Not every log line needs every field. Enrich scopes/activities where practical.

## OpenTelemetry

Workers and APIs should use the platform OpenTelemetry conventions for:

- traces,
- metrics,
- logs.

Trace major boundaries:

```text
receive
parse/validate
external read
transform
persist
publish
ack
```

## Metrics

Metric labels must remain bounded.

Good labels:

- source type,
- stage,
- outcome,
- queue role.

Avoid high-cardinality metric labels such as:

- message ID,
- operation ID,
- WTG ID,
- artifact path.

Those belong in logs/traces.

## Logging failures

Log an exception at the layer that has enough context to act on it.

Do not log the same exception at every call frame.

## Secrets and sensitive data

Never log:

- passwords,
- access keys,
- connection strings,
- bearer tokens,
- secret values,
- full sensitive payloads.

## Operationally useful events

Typical meaningful events:

- message received,
- validation rejected,
- artifact read completed,
- persistence committed,
- downstream publish succeeded,
- message acknowledged,
- retry scheduled,
- DLQ/dead-letter persisted,
- stale/out-of-order work ignored.

## Telemetry must not change correctness

Telemetry failure must not silently turn successful business work into partial data loss unless the telemetry path is explicitly part of the business contract.
