# C# Platform Engineering Standards

## Purpose

This repository documentation defines a three-layer rule system for AI-assisted implementation of the platform.

The goal is to make generated code:

- consistent across components,
- aligned with the existing repository,
- maintainable by humans,
- safe under retries and distributed failure,
- observable in production,
- resistant to AI over-engineering,
- and cheap enough to load selectively without rereading the entire documentation set.

This documentation is **not** a replacement for feature specifications, ADRs, implementation plans, or source-code inspection.

## The three layers

### Layer 1 — Core Rules

Read for **every implementation task**:

- `AI_RULES.md`

This is the platform constitution. It is intentionally short and contains only non-negotiable rules.

### Layer 2 — Shared Engineering Standards

Read only the standards relevant to the current task:

- `standards/csharp-coding-standards.md`
- `standards/solid-design-principles.md`
- `standards/design-pattern-guidelines.md`
- `standards/dotnet-architecture-rules.md`
- `standards/async-and-concurrency-rules.md`
- `standards/logging-and-telemetry-rules.md`
- `standards/database-and-persistence-rules.md`
- `standards/messaging-worker-rules.md`
- `standards/testing-standards.md`
- `standards/ai-implementation-workflow.md`

A task that only changes validation logic should not load every database and messaging rule.  
A task that changes an SQS worker should load messaging, async, telemetry, testing, and any persistence rules actually touched.

### Layer 3 — Component Rules

Read the component-specific rules for the component being changed:

- `components/ingestor/implementation-rules.md`
- `components/raw-fanout/implementation-rules.md`
- `components/harmonizer/implementation-rules.md`

Component rules **extend** the platform standards. They do not duplicate or replace them.

## Rule precedence

When rules conflict, apply them in this order:

1. explicit current user requirement,
2. accepted implementation plan / ADR / feature specification,
3. repository-established behavior and compatibility constraints,
4. component rules,
5. shared engineering standards,
6. general best practice.

If a lower-level rule conflicts with an accepted higher-level decision, do not silently “improve” the architecture. Report the conflict.

## Recommended AI loading sequence

For any change:

```text
1. Read docs/AI_RULES.md
2. Inspect repository instructions and relevant existing code
3. Read component implementation-rules.md
4. Load only relevant shared standards
5. Read the current implementation plan / Micro-RPI
6. Implement the smallest compliant change
7. Build and run relevant tests
8. Report evidence and deviations
```

## Authoritative-source rule

Each rule should have one authoritative home.

Examples:

- C# naming -> `csharp-coding-standards.md`
- SOLID interpretation -> `solid-design-principles.md`
- SQS acknowledgement semantics -> `messaging-worker-rules.md`
- Raw Fan-Out batch identity -> Raw Fan-Out component rules

Other files should reference rather than copy the full rule.

## What belongs outside these files

The following should normally live in feature plans, ADRs, or design documents instead of generic standards:

- calculator formulas,
- source-specific SQL semantics,
- exact Parquet schema versions,
- temporary migration steps,
- one-off defect fixes,
- unresolved design experiments.

Stable and reusable rules belong here. Feature-specific knowledge does not.


## Workflow skills

The repository workflow layer lives under `.github/skills/`.

Use Skills for repeatable process and these docs for authoritative rules.

See:

- `.github/skills/README.md`
- `.github/copilot-instructions.md`

Do not duplicate standards inside Skills.
