---
name: implement-micro-rpi
description: Implement an accepted Micro-RPI as a bounded vertical slice, preserving repository architecture and reporting real build/test evidence.
---

# Implement Micro-RPI

The accepted Micro-RPI is the implementation contract.

Do not redesign the architecture while coding unless repository evidence proves the plan is impossible or incorrect.

## Load

Required:

- `docs/AI_RULES.md`,
- the accepted Micro-RPI,
- relevant component rules,
- only the shared standards directly touched by the change.

Inspect current source before editing even when the Micro-RPI names expected files.

## Before coding

Confirm from the repository:

- target project names,
- existing types/contracts,
- DI/wiring pattern,
- migration mechanism if applicable,
- test structure,
- package/SDK versions.

If reality differs materially from the Micro-RPI, report the deviation and choose the smallest compatible adjustment.

## Implementation order

Prefer a working vertical slice.

Do not create unused scaffolding merely because the plan listed a future folder/type.

## Scope discipline

Do not:

- refactor unrelated code,
- rename unrelated concepts,
- upgrade packages without need,
- replace established architecture,
- weaken validation/tests,
- suppress failures to make CI green.

## Verification

Run applicable:

- restore/build,
- focused unit tests,
- integration tests,
- migration checks,
- serialization/contract tests,
- formatter/analyzers when repository policy requires them.

A build alone does not prove behavioral correctness.

## Completion report

Use `references/implementation-checklist.md`.

Do not say “complete” if required verification is blocked; state exactly what is implemented and what remains unverified.
