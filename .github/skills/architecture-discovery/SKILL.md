---
name: architecture-discovery
description: Investigate an existing or new component before implementation when behavior, boundaries, data flow, failure semantics, or repository patterns are not understood well enough to create a safe implementation plan.
---

# Architecture Discovery

Discovery answers **what is true** before deciding **what to build**.

Do not implement production code during a discovery-only task.

## Load

Always:

- `docs/AI_RULES.md`

Then:

- the relevant component rules if the component already exists,
- `docs/standards/dotnet-architecture-rules.md` when boundaries matter,
- other standards only when directly relevant.

Do not load every standard by default.

## Inspect repository evidence

Inspect enough of the real repository to trace one complete relevant flow:

```text
entry point
→ orchestration
→ external/persistence boundaries
→ durable state
→ downstream output
→ tests
```

Also inspect:

- project references,
- configuration,
- contracts/messages,
- database schema/migrations where relevant,
- failure/retry paths.

## Separate evidence classes

The output must distinguish:

### Verified

Confirmed from source/config/schema/test evidence.

### Inferred

Reasonable interpretation supported by evidence but not directly proven.

### Unknown

Information still required for a safe design decision.

Do not convert unknowns into invented architecture.

## Identify

Capture:

- component responsibility,
- inputs and outputs,
- source of truth,
- identity and ordering,
- state ownership,
- transaction/durable-success boundary,
- retry/idempotency behavior,
- concurrency/lease/fencing behavior,
- observability,
- compatibility constraints,
- existing patterns worth reusing,
- genuine variation points.

## Stop condition

Discovery is sufficient for a Micro-RPI when the remaining unknowns do not materially change:

- public contracts,
- state model,
- architecture boundary,
- correctness under failure,
- implementation sequence.

If a blocking unknown remains, ask only the smallest necessary question or request the missing source artifact.

## Deliverable

Use `references/discovery-output-template.md`.
