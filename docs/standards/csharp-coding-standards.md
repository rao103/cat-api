# C# Coding Standards

## Language baseline

Use the C# version already selected by the repository unless the task explicitly includes a language-version upgrade.

### Required

- Enable and respect nullable reference types.
- Do not use the null-forgiving operator (`!`) merely to silence warnings.
- Prefer clear, idiomatic modern C# where it improves readability.
- Keep types and members easy to understand without requiring clever language tricks.

## Naming

Use standard .NET conventions:

- types, methods, properties, events: `PascalCase`
- parameters and local variables: `camelCase`
- private instance fields: `_camelCase`
- interfaces: `IName`
- async methods returning `Task` / `ValueTask`: normally end in `Async`

Names should reflect business or technical responsibility.

Avoid vague names such as:

```text
Manager
Helper
Utils
Common
Processor
Service
```

unless the responsibility is genuinely clear from context.

## Files and types

- Prefer one primary public/internal type per file.
- File name should match the primary type name.
- Small private nested types are acceptable when they are implementation details.
- Avoid creating folders with no meaningful responsibility.

## `var`

Use `var` when the assigned expression makes the type obvious.

Good:

```csharp
var request = new FanOutRequest(...);
```

Prefer explicit types when the inferred type is not obvious or when clarity improves reviewability.

## Constants and configuration

Do not scatter magic values through business logic.

Use, depending on meaning:

- `const`
- static readonly values
- enums
- validated options/configuration
- domain value objects

Do not convert configurable operational behavior into compile-time constants.

## Methods

- A method should perform one coherent operation.
- Prefer guard clauses to deep nesting.
- Avoid long parameter lists; use a request/options object when parameters form a real concept.
- Do not create a wrapper DTO solely to satisfy an arbitrary parameter-count rule.
- Keep side effects explicit.

## Classes

- Keep responsibilities cohesive.
- Prefer composition over inheritance.
- Use inheritance only when substitutability is real and stable.
- `sealed` is reasonable for implementation classes that are not designed for inheritance, but should not be applied mechanically.

## Exceptions

- Exceptions represent exceptional failures, not normal control flow.
- Do not use empty `catch` blocks.
- Do not catch `Exception` merely to log and rethrow at every layer.
- Add context at the correct boundary.
- Preserve stack traces with `throw;`, not `throw ex;`.
- Do not include secrets or sensitive connection details in exception messages.

## DTOs and models

Do not automatically reuse one model for all layers.

Separate models where responsibilities differ materially, for example:

- external message DTO,
- source row model,
- domain/canonical model,
- database persistence model,
- API response model.

Do not create duplicate models when semantics are genuinely identical.

## Immutability

Prefer immutable records/value objects for stable messages and value-like data.

Use mutability when the lifecycle requires it; do not force record types everywhere.

## Collections

Expose the least mutable shape required by the consumer.

Do not allocate extra collections unnecessarily in high-volume paths.

## Comments

Comments should explain:

- why,
- invariants,
- surprising constraints,
- compatibility decisions,
- non-obvious distributed-system behavior.

Do not write comments that merely restate code.

## Formatting

Use repository tooling (`dotnet format`, `.editorconfig`, analyzers) as the formatting authority.

Do not introduce formatting-only churn into unrelated files.
