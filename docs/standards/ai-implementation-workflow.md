# AI Implementation Workflow

This is the operational workflow for AI-assisted engineering.

## Phase 1 — Inspect

Before changing code:

1. read `AI_RULES.md`;
2. read the relevant component rules;
3. inspect repository instructions;
4. inspect project files and dependency direction;
5. inspect one complete representative flow;
6. inspect relevant tests;
7. read the accepted feature plan / Micro-RPI / ADR.

Do not infer architecture from folder names alone.

## Phase 2 — Identify constraints

State internally:

- what must remain unchanged,
- what contract is authoritative,
- what failure/retry behavior exists,
- what is unknown,
- which shared standards are relevant.

## Phase 3 — Design minimally

Prefer the smallest design that:

- satisfies current requirements,
- follows repository patterns,
- preserves contracts,
- handles retry/failure correctly,
- can be tested.

Do not introduce optional future architecture.

## Phase 4 — Implement vertically

Prefer a working vertical slice over scaffolding the whole theoretical architecture.

For example:

```text
contract
→ handler
→ core operation
→ persistence
→ downstream publish
→ tests
```

Then expand.

## Phase 5 — Verify

Required as applicable:

- `dotnet build`
- unit tests
- integration tests
- migration validation
- contract serialization tests
- static analysis / formatter

## Phase 6 — Report

Report:

- files changed,
- behavior implemented,
- tests run,
- unresolved risks,
- deviations from plan,
- follow-up work outside current scope.

## Forbidden AI behaviors

Do not:

- refactor unrelated code,
- rename large areas for style,
- add abstractions merely because they are considered best practice,
- change architecture without authorization,
- claim tests passed without running them,
- claim a file or behavior exists without inspection,
- silently “fix” a contract by breaking compatibility.
