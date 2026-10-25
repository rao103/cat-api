---
name: csharp-architecture
description: Design, review, scaffold, or refactor C# project boundaries and dependency direction for APIs, workers, ingesters, and related components while preserving the established repository architecture.
---

# C# Architecture

Use this Skill for structural decisions. Do not use it merely for a small local code edit.

## Load first

Always read:

- `docs/AI_RULES.md`
- `docs/standards/dotnet-architecture-rules.md`
- `docs/standards/solid-design-principles.md`
- `docs/standards/design-pattern-guidelines.md`

Load other standards only when the task touches them.

For a known component, also read its `docs/components/.../implementation-rules.md`.

## Establish evidence

Inspect:

- solution and project files,
- actual project references,
- entry points and DI registration,
- configuration,
- representative use-case flow,
- relevant tests,
- current specification / ADR.

Separate:

```text
Observed fact
Proposed change
Assumption / unknown
```

Do not infer architecture from folder names or screenshots alone.

## Boundary decisions

Prefer the smallest useful project structure.

A new project must have a concrete reason such as:

- separate executable/deployment host,
- enforced dependency boundary,
- genuine independent reuse,
- shared wire contract used by multiple deployables.

Do not create Domain/Application/Infrastructure/Contracts projects simply because a generic architecture diagram contains them.

For CAT-style solutions, use the repository's established `Core + Worker` or `Core + API` convention when applicable.

## Architecture contract

For a structural change, produce:

1. scope and preserved behavior;
2. proposed project/folder tree;
3. responsibility of each project;
4. allowed dependency/reference direction;
5. one end-to-end execution flow;
6. important external boundaries;
7. patterns/abstractions introduced and why;
8. validation/build/test commands;
9. explicit non-goals.

## References

Read only when relevant:

- `references/api-core-reference.md`
- `references/worker-ingester.md`
- `references/structure-patterns.md`
- `references/rpi-routing.md`

## Implementation rule

If the user asked only for design or planning, do not change production code.

If implementation is authorized, prefer one working vertical slice before broad structural replication.
