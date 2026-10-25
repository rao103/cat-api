# Repository AI Engineering Instructions

These instructions are a router. The authoritative engineering rules live under `docs/`.

## Always

For any code, design, refactor, database, worker, API, or test change:

1. read `docs/AI_RULES.md`;
2. inspect the relevant existing repository code and tests;
3. read the applicable component rules under `docs/components/`;
4. load only the shared standards needed for the current change;
5. follow the accepted ADR / specification / Micro-RPI for the task.

Do not load every document automatically.

## Workflow skills

Use the appropriate skill for recurring workflows:

- `.github/skills/csharp-architecture/SKILL.md`
- `.github/skills/architecture-discovery/SKILL.md`
- `.github/skills/create-micro-rpi/SKILL.md`
- `.github/skills/implement-micro-rpi/SKILL.md`
- `.github/skills/database-migration/SKILL.md`
- `.github/skills/add-source-type/SKILL.md`

A Skill defines **how to work**.  
`docs/standards/` defines **what engineering rules must hold**.

## Repository preservation

- Preserve public behavior unless the accepted task changes it.
- Preserve unrelated edits.
- Do not introduce new projects, packages, abstractions, design patterns, or frameworks without a concrete reason.
- Do not silently upgrade dependencies.
- Do not rewrite the repository to match generic Clean Architecture.
- Build and test before claiming implementation is complete.
- Report actual verification evidence and anything not verified.

## Context handoff

When Research, Plan, and Implementation run in separate contexts, explicitly carry forward:

- the accepted specification / Micro-RPI path,
- relevant component rules,
- relevant standards,
- key repository evidence and decisions.

Do not assume another phase automatically retains previous context.
