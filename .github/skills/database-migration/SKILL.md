---
name: database-migration
description: Plan, implement, and verify schema or database-code changes using the repository's existing migration mechanism, with compatibility, backfill, rollback/roll-forward, and concurrency concerns made explicit.
---

# Database Migration

Use this Skill for material database changes such as:

- tables/columns,
- keys/constraints/indexes,
- functions/procedures,
- triggers,
- backfills,
- data-shape transitions.

## Load

Always:

- `docs/AI_RULES.md`
- `docs/standards/database-and-persistence-rules.md`
- `docs/standards/testing-standards.md`

Also load:

- relevant component rules,
- messaging rules if database state coordinates queue delivery,
- async rules if application database code changes.

## Discover the migration owner

Before writing SQL, inspect the repository's actual schema deployment mechanism.

Do not introduce a second migration framework.

Confirm:

- database engine,
- migration ordering/naming,
- deployment tool,
- compatibility expectations,
- current schema,
- application readers/writers.

## Design the transition

For a non-trivial change, explicitly decide:

```text
expand
→ backfill
→ switch readers/writers
→ contract
```

when backward-compatible staged deployment is required.

Do not assume an instantaneous coordinated deployment.

## Data safety

Consider:

- existing rows,
- NULL/default semantics,
- uniqueness,
- foreign keys,
- lock duration,
- large-table backfill,
- idempotent rerun behavior,
- audit columns,
- timezone/data-type conversion,
- rollback vs roll-forward.

## Application compatibility

Search for all readers/writers of the changed database contract.

A schema change is not complete when SQL applies but application code still expects the old shape.

## Verification

Use `references/migration-checklist.md`.
