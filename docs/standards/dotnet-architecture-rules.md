# .NET Architecture Rules

## Preserve existing solution shape

Do not invent additional projects or architectural layers without a concrete need.

For this platform, a common and acceptable structure is:

```text
<Component>.Core
<Component>.Worker
```

or for APIs:

```text
<Component>.Core
<Component>.Api
```

Add separate Domain/Application/Infrastructure/Contracts projects only when real boundaries justify them.

## Dependency direction

Typical rule:

```text
Worker / API
    ↓
Core
```

Core must not depend on Worker/API hosting.

Shared wire contracts may live in an agreed shared contracts project if multiple producers and consumers require them.

## Composition root

Dependency registration belongs at the host boundary.

`Program.cs` should remain focused on:

- host creation,
- configuration binding,
- dependency registration,
- telemetry registration,
- startup validation,
- starting the application.

Business logic must not accumulate in `Program.cs`.

## Core responsibilities

Core may contain, when appropriate:

- operations/orchestration,
- domain or canonical models,
- interfaces/capability contracts,
- repositories,
- adapters,
- serialization,
- validation,
- configuration models,
- factories/resolvers,
- observability helpers.

Folder names follow real responsibilities; do not create empty taxonomy.

## Infrastructure boundaries

External systems should be isolated behind explicit capabilities where that meaningfully protects policy.

Examples:

- source database reader,
- control-state repository,
- artifact store,
- queue publisher/consumer,
- Parquet serializer.

Avoid “one generic repository for everything.”

## Dependency injection

- Use constructor injection.
- Do not use service locator patterns.
- Do not pass `IServiceProvider` into business logic to resolve arbitrary dependencies.
- Avoid static mutable service state.

## Package dependencies

- Prefer existing approved packages.
- Do not upgrade packages as part of an unrelated feature.
- New packages require a concrete reason.
- Keep SDK-specific dependencies near infrastructure boundaries.

## Architecture consistency rule

Repository consistency is more valuable than locally introducing a “better” pattern.

If Raw Fan-Out, Harmonizer, and Calculator solve the same hosting/configuration/telemetry problem, prefer one shared convention.
