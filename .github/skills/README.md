# Repository Workflow Skills

Skills describe repeatable engineering workflows. They intentionally avoid duplicating the Engineering Standards in `docs/`.

| Skill | Use when |
|---|---|
| `csharp-architecture` | Designing/reviewing project boundaries, dependency direction, or structural C# changes |
| `architecture-discovery` | The component/problem is not understood well enough to plan implementation safely |
| `create-micro-rpi` | Discovery is sufficient and a small, implementation-ready plan is needed |
| `implement-micro-rpi` | An accepted Micro-RPI exists and should now be implemented |
| `database-migration` | A task changes PostgreSQL/SQL Server schema, constraints, functions, procedures, triggers, or backfill behavior |
| `add-source-type` | Extending an existing multi-source ingestion/fan-out/harmonization pipeline with a new source type |

## Rule loading

Every skill starts from:

```text
docs/AI_RULES.md
```

Then it loads:

```text
relevant component rules
+
only relevant shared standards
+
task-specific specification / ADR / Micro-RPI
```

Skills must not become a second copy of C#, SOLID, testing, messaging, or database standards.
