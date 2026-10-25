# SOLID Design Principles

SOLID is a design constraint, not a target for maximizing the number of abstractions.

## S — Single Responsibility Principle

A type should have one primary reason to change.

Examples:

- an SQS host loop should not also contain transformation rules and SQL persistence logic;
- a parser should not own retry policy;
- a repository should not become the orchestration layer.

SRP does **not** mean:

- one method per class,
- splitting twenty coherent lines into eight services,
- creating forwarding wrappers with no independent responsibility.

Use cohesion as the test: do the members change for the same reason?

## O — Open/Closed Principle

Stable orchestration may be closed while true variation points are extensible.

Good candidates:

- source-type-specific processing,
- storage provider adapters,
- transformation policies that have multiple real implementations.

Do not create extension mechanisms for hypothetical future behavior.

Prefer:

```text
known variation -> explicit abstraction
```

over:

```text
possible someday -> speculative abstraction
```

## L — Liskov Substitution Principle

An implementation must honor the full contract of the abstraction.

Red flags:

- `NotSupportedException` for normal interface members,
- stricter preconditions in one implementation,
- unexpected side effects,
- different nullability or transactional semantics,
- silently weaker guarantees.

If implementations cannot honor one contract, the abstraction is probably wrong.

## I — Interface Segregation Principle

Interfaces should represent focused capabilities.

Prefer:

```text
IRawArtifactReader
IFanOutStateStore
IHarmonizationPublisher
```

over a broad interface that mixes unrelated capabilities.

Do not split interfaces so aggressively that every method receives its own interface without benefit.

## D — Dependency Inversion Principle

High-level policy should not depend directly on volatile infrastructure details.

Typical infrastructure boundaries:

- PostgreSQL/Npgsql
- SQL Server
- SQS
- Blob/S3
- HTTP APIs
- filesystem
- clock when deterministic time matters

Introduce abstractions when they:

- protect high-level policy from infrastructure,
- enable a meaningful alternate implementation,
- improve deterministic testing of policy,
- represent a stable contract between modules.

Do **not** create `IFoo` for every concrete class automatically.

## AI design test

Before adding an abstraction, answer:

1. What problem does this abstraction solve?
2. What changes independently?
3. Is this a real boundary or just another name for the same implementation?
4. Does the repository already have a pattern for this?
5. Is the added indirection cheaper than the coupling it removes?

If those questions have no concrete answer, prefer the simpler design.
