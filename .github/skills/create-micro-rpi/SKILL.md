---
name: create-micro-rpi
description: Create a small implementation-ready Research/Plan artifact for one bounded vertical slice after enough architecture discovery exists to implement safely.
---

# Create Micro-RPI

A Micro-RPI is a **bounded implementation contract**, not another architecture essay.

Use it when the problem is understood well enough to plan one coherent vertical slice.

## Load

Always:

- `docs/AI_RULES.md`

Then:

- relevant component rules,
- relevant accepted ADR/specification,
- discovery output or verified repository evidence,
- only shared standards touched by the planned change.

## Micro-RPI scope

A good Micro-RPI should normally be implementable and verifiable in one focused engineering pass.

Split the work when it mixes independent concerns such as:

- unrelated schema redesign,
- broad architectural migration,
- multiple deployables with independent acceptance criteria,
- unresolved component semantics.

## Required content

The plan must state:

1. objective;
2. current verified behavior;
3. exact scope;
4. explicit non-goals;
5. files/projects likely affected;
6. contracts/state changes;
7. implementation steps in dependency order;
8. failure/idempotency/concurrency behavior where relevant;
9. tests and acceptance evidence;
10. migration/backward-compatibility concerns;
11. unresolved assumptions.

## Implementation quality

The plan must be precise enough that the implementer does not need to redesign the feature while coding.

However, do not prescribe trivial line-by-line coding details that should remain local implementation choices.

## Vertical slicing

Prefer:

```text
contract/state
→ core behavior
→ persistence/external boundary
→ host/wiring
→ tests
```

over building many empty abstractions first.

## Output

Use `references/micro-rpi-template.md`.
