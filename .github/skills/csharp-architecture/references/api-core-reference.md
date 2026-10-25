# API + Core Reference

This reference preserves the useful repository pattern from the older C# Copilot setup.

## Preferred shape for similar CAT APIs

```text
<Name>.API
<Name>.API.Core
<Name>.API.UnitTests
<Name>.API.IntegrationTests
```

### API project

Owns:

- HTTP endpoints/controllers,
- host lifecycle,
- configuration binding,
- logging/telemetry registration,
- Swagger/OpenAPI setup,
- health endpoints,
- dependency injection composition.

### Core project

Owns the main functionality, including as appropriate:

- use cases/services/handlers,
- validation,
- models/DTO mapping,
- persistence implementations,
- provider integrations,
- repositories/adapters.

Core is a pragmatic functionality library. It is **not automatically a dependency-free Domain assembly**.

### Dependency direction

```text
API -> Core
Core -X-> API
```

Add separate Application, Infrastructure, Domain, or Contracts projects only for a demonstrated boundary.

## Existing CAT conventions to preserve

The earlier CAT API reference used role-based folders such as:

```text
Core/
  Attributes/
  Configurations/
  Constants/
  DBContexts/
  Enums/
  Extensions/
  Factories/
  Handler/
  Helpers/
  Interceptors/
  Interfaces/
  Mappers/
  Middlewares/
  Migrations/
  Models/
  Repository/
  Resolvers/
  Services/
  Validators/
```

This is a reference, not a mandatory scaffold. Create folders only when actual code needs them.

## Controller rule

Keep HTTP orchestration thin.

A typical flow is:

```text
Controller
→ injected Core use-case service/handler
→ persistence/provider boundary
→ response mapping
```

Do not invent an extra mediator/handler/service layer merely to make the diagram longer.

## Verification before structural change

Inspect at least:

- API and Core `.csproj`,
- `Program.cs`,
- DI registration,
- one representative controller,
- its Core service/handler,
- relevant persistence boundary,
- tests.

Preserve existing routes, response contracts, validation behavior, and authentication/authorization semantics unless the accepted task changes them.
