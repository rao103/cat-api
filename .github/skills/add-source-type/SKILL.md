---
name: add-source-type
description: Extend an existing multi-source data pipeline with a new source type while reusing stable platform lifecycle patterns and preserving source-specific semantics.
---

# Add Source Type

Use only when the surrounding multi-source architecture is already established.

If the new source's behavior is not understood, run `architecture-discovery` first.

## Load

Always:

- `docs/AI_RULES.md`

Normally also load:

- relevant Ingestor / Raw Fan-Out / Harmonizer component rules,
- `docs/standards/dotnet-architecture-rules.md`,
- `docs/standards/messaging-worker-rules.md`,
- `docs/standards/database-and-persistence-rules.md` if control state changes,
- `docs/standards/testing-standards.md`.

## First classify the source

Do not assume all sources have Production semantics.

Determine whether the source is primarily:

- changed-row/time-series,
- event/lifecycle,
- snapshot,
- event-driven application data,
- another explicitly documented model.

Identify:

- stable business identity,
- update/version ordering,
- deletion semantics,
- business-time footprint,
- refresh semantics,
- cursor need,
- authoritative source of truth.

## Reuse lifecycle, not false semantics

Reuse stable platform patterns such as:

- lease/fencing,
- extraction run audit,
- immutable Raw,
- queue envelope,
- telemetry,
- configuration,
- worker hosting.

Do not copy a source-specific algorithm merely because another source already implements it.

## Cross-component impact

Trace the new source through all affected stages:

```text
Ingestor / producer
→ Raw artifact contract
→ Raw Fan-Out
→ Harmonizer
→ downstream calculation/package readiness
→ serving/publication if applicable
```

Do not declare the source supported because only the ingestor compiles.

## Contracts

Prefer explicit source handlers/strategies at real variation points.

Version wire/file contracts when semantics change.

## Acceptance

Use `references/source-type-checklist.md`.
