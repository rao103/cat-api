# Design Pattern Guidelines

Patterns are tools. A pattern must solve an identified design problem.

## Decision order

Use this order:

```text
requirement
→ identify invariant and variation point
→ apply SOLID constraints
→ choose the simplest suitable pattern
→ implement
```

Do not start with a favorite pattern and search for somewhere to apply it.

## Preferred patterns

### Strategy

Use when a stable workflow has multiple real behavior variants.

Good example:

- different source types with different slicing or canonicalization logic.

Avoid when:

- there is only one implementation with no real variation.

### Factory

Use when creation/selection logic is meaningful and centralized.

Keep factories thin.

A factory should not become a hidden service locator.

### Pipeline

Use for explicit ordered processing such as:

```text
Receive
→ Validate
→ Load
→ Transform
→ Persist
→ Publish
```

Each stage must have a meaningful boundary; do not turn every method call into a pipeline stage.

### Adapter

Use to isolate external SDKs and protocols from core policy:

- SQS SDK
- Npgsql
- Blob/S3 SDK
- HTTP clients
- Parquet library

Prefer capability-oriented adapters to generic wrappers.

### Outbox

Use when a durable database change and later message publication must be coordinated without a distributed transaction.

Required characteristics:

- durable write in the local DB transaction,
- independent dispatcher,
- retry-safe publication,
- observable failure/retry state.

### Idempotent Consumer

Use by default for at-least-once message handling.

A repeated delivery must not create an incorrect duplicate business result.

### Decorator

Useful for cross-cutting behavior when it adds clarity:

- tracing,
- metrics,
- validation,
- retry policies.

Do not wrap everything in multiple invisible decorator layers.

### Options Pattern

Use strongly typed configuration objects with validation.

### Result Pattern

Use selectively for expected domain/business outcomes where exceptions would be inappropriate.

Do not replace every exception with a generic result object.

## Patterns requiring justification

The following are not forbidden, but should not be introduced automatically:

- generic repository
- unit of work over EF/Npgsql purely by habit
- mediator / MediatR
- CQRS
- abstract factory
- builder
- domain event bus
- event sourcing
- specification pattern
- generic workflow engine

Use them only when the problem actually warrants the cost.

## Anti-over-engineering rules

Do not create chains such as:

```text
IFooService
→ FooService
→ IFooRepository
→ FooRepository
→ FooProvider
→ FooFactory
```

unless every layer has a distinct responsibility.

A pattern that adds more concepts than it removes is usually the wrong pattern.
