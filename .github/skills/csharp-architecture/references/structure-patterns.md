# C# Structure Patterns

These are selection aids, not mandatory templates.

## Small cohesive worker/API

Use one executable project plus tests when the scope is genuinely small.

## CAT-style worker/ingester

Prefer:

```text
<Component>.Core
<Component>.Worker
```

when the repository already uses that convention and Core represents reusable functionality while Worker is the host/composition root.

## CAT-style API

Prefer:

```text
<Component>.API
<Component>.API.Core
```

for similar existing CAT APIs.

## Shared contracts

Create a shared Contracts project only when two or more independently built/deployed producers/consumers genuinely share a wire contract.

Do not move internal models there.

## Separate Domain project

Introduce only when there is substantial domain behavior that should remain independent of hosting and infrastructure.

Do not create it for DTOs, enums, or simple validation alone.

## Selection test

Before adding a project, answer:

1. What responsibility does it own?
2. What dependency rule does the assembly boundary enforce?
3. Who consumes it?
4. Could a folder achieve the same result with less complexity?
5. Does it match the established repository convention?

If the answers are weak, keep the structure simpler.
