# AI Core Implementation Rules

These rules apply to every AI-assisted code change in this platform.

## 1. Preserve repository intent

- Inspect the existing implementation before designing a replacement.
- Prefer the repository's established architecture and conventions.
- Do not create a new local architecture because it appears theoretically cleaner.
- Do not rename, move, or refactor unrelated code.

## 2. Make the smallest correct change

- Implement only the scope required by the accepted task.
- Avoid speculative extensibility.
- Avoid abstractions without an identified variation point or boundary.
- Do not add framework layers, packages, patterns, or projects without a concrete need.

## 3. Follow SOLID without over-engineering

- Apply SOLID to responsibilities and dependency direction.
- SOLID does not mean “everything needs an interface.”
- SRP does not mean splitting small coherent logic into many classes.
- DIP is primarily for meaningful policy / infrastructure boundaries and real variation points.

## 4. Preserve contracts

- Do not silently break public APIs, message contracts, database contracts, or file schemas.
- Version external/wire contracts when semantics change.
- Preserve backward compatibility when required by the accepted implementation plan.

## 5. Distributed processing is retryable

- Assume at-least-once delivery for messaging unless the platform explicitly guarantees otherwise.
- Make handlers idempotent where duplicate execution is possible.
- Do not acknowledge work before durable completion.
- Do not treat message arrival order as source truth.

## 6. Use safe async and cancellation

- I/O must use asynchronous APIs when available.
- Do not use `.Result`, `.Wait()`, or sync-over-async.
- Propagate `CancellationToken` through cancellable boundaries.
- Do not wrap normal asynchronous I/O in `Task.Run()`.

## 7. Keep observability structured

- Use structured logging, tracing, and metrics.
- Correlate work using stable identifiers such as `operation_id`, `work_version`, `attempt`, `extraction_token`, and `message_id` when relevant.
- Do not log secrets, credentials, connection strings, tokens, or unnecessary sensitive payloads.
- Do not repeatedly log the same exception at every layer.

## 8. Configuration must be explicit

- Use strongly typed options for runtime configuration.
- Validate required options at startup.
- Do not hard-code environment-specific values or secrets.
- Do not silently fall back to localhost, dummy credentials, or unsafe defaults in production paths.

## 9. Persistence must be safe

- Use parameterized SQL.
- Make transaction boundaries explicit.
- Schema changes must use the repository's migration process.
- Use UTC for persisted timestamps unless a specific business-time rule requires otherwise.
- Preserve idempotency and concurrency semantics.

## 10. Verify before declaring completion

Before calling work complete:

- build affected projects,
- run relevant tests,
- add or update tests for changed behavior,
- report failures honestly,
- document any deliberate deviation from the accepted plan.

If source code, runtime behavior, or contracts are unknown, inspect them. Do not invent them.
