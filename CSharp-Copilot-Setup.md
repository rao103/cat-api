# C# Copilot Architecture Setup

This setup supplies a reusable architecture skill, repository instructions, and RPI guidance for C# APIs, workers, and large-data ingesters. It is a starting engineering standard, not a compiled application template or a guarantee of production readiness.

## Updated ingestor reference - 16 September 2026

New CAT-style ingestors use Core plus a Worker host, with an API only when required. Core uses role-based folders: Operations, Interfaces, Repositories, Adapters, Factories, Authentication, Configuration, Validators, Resolvers, and Serialization. Options are data-only; dedicated validators integrate with startup validation; settings resolution is separate from connection creation. Source SQL, control-plane PostgreSQL, SQS, and blob storage are examples of four distinct capability boundaries, not four generic repositories.

The full reusable contract, folder tree, dependency rules, and verification checklist are in the embedded worker/ingester reference below and the installed [worker-ingester.md](.github/skills/csharp-architecture/references/worker-ingester.md). Keep these copies synchronized when changing the standard. Preserve another project's approved architecture and create only the folders and integrations it needs.

## Updated API reference — 15 September 2026

Similar APIs now default to your API + Core convention, based on the supplied CAT screenshots. Core holds main functionality and can contain persistence implementations. Extra Application, Infrastructure, Domain, or Contracts projects require a concrete reason. Existing role-based folders and tests under src are preserved. The controller example confirms delegation to an injected IAdjustmentService. Service internals, dependency references, and the complete HTTP behavior still require source inspection.

For an existing installation, merge the updated instructions and replace only the matching skill files after checking for local customizations. The new `references/api-core-reference.md` records observed structure separately from proposed responsibilities and unknown details.

## Install in your VS Code repository

Ask Copilot in Agent mode to read this document and create each file at the exact destination shown below. Each code block contains the complete file content. Merge repository instructions if that file already exists. Preserve existing skills and conflicting local requirements; report any conflicts. Do not change application code during installation.

The skill resides under `.github/skills/csharp-architecture/`. Keep the references and asset beside it; SKILL.md uses relative links. You do not need another plugin. Keep using your existing RPI plugin.

After reviewing the added files, commit them to your application repository so your team and future Copilot sessions use the same standards. Installation here in ChatGPT does not install files into your VS Code repository.

## First use: repair the current solution

Use this request with your RPI research/plan phase:

> Read `.github/copilot-instructions.md` and `.github/skills/csharp-architecture/SKILL.md`, including relevant references. Review the current C# solution using actual project references and one complete processing flow. Identify concrete design problems, then propose the smallest structure that fixes them. Include a responsibility/reference table and incremental migration plan. Preserve existing behavior. Plan only; do not modify application code yet.

After accepting the plan, use this request in your RPI implementation phase:

> Implement the accepted architecture plan using `.github/skills/csharp-architecture/SKILL.md`. Pass the architecture contract into each phase. Start with one working vertical slice, build and test it, then continue. Preserve public behavior, explain material deviations, and report verification evidence.

The exact slash commands depend on your RPI plugin; these requests deliberately avoid assuming a distribution. If skill auto-selection is uncertain, explicitly ask the agent to read the file paths.

## Included files

### .github/skills/csharp-architecture/SKILL.md

````markdown
---
name: csharp-architecture
description: Design, scaffold, review, and refactor C# solution and project structures for APIs, workers, and data ingesters. Use when planning project boundaries, dependency direction, or implementing features that cross those boundaries, including RPI workflows.
---

# C# Architecture

Produce a maintainable solution whose project boundaries match actual responsibilities. Preserve the user's chosen frameworks, deployment model, and authorized scope. Treat the recommendations here as defaults; explicit requirements and established repository decisions take precedence.

## Establish context

Inspect repository instructions, solution and project files, SDK/package configuration, entry points, dependency registration, tests, and one representative feature. Read relevant architecture decisions and the requested specification. Separate observed facts, proposed changes, and assumptions. Do not invent an existing architecture or claim a folder name proves a boundary.

For a new solution, establish workload type, deployment units, external dependencies, data volume, failure semantics, and supported SDK. Use repository-approved versions; verify unfamiliar or version-sensitive APIs in official documentation. Do not silently upgrade the framework or bypass configured package sources.

For routine edits, preserve the structure and keep the plan proportional. For new solutions or structural changes, produce an architecture contract before implementation: project tree, responsibility of each project, allowed project references, one end-to-end use-case flow, package/abstraction rationale, and validation commands. If the user requests a plan only, stop after the plan. If implementation is authorized, proceed within the stated assumptions; do not invent an extra approval gate.

## Choose boundaries

For this user's CAT-style APIs, use [api-core-reference.md](references/api-core-reference.md) as the preferred starting structure: API host plus Core functionality. Preserve that two-project convention for similar APIs unless concrete requirements justify a change. This preference does not automatically apply to workers, ingesters, or unrelated repositories. Core in this reference includes persistence and services; do not interpret it as a pure Domain assembly.

Read [structure-patterns.md](references/structure-patterns.md) for structure selection and examples. Start with the smallest useful structure. A folder groups code; a project establishes an assembly/dependency boundary; an executable project supplies a host. None automatically implies a separate deployment.

- Require an identifiable boundary, independent reuse, or separate host for each added project.
- Preserve established folder conventions, including the reference API/Core layout. Within those conventions, keep related functionality cohesive and avoid unrelated catch-all behavior. Do not reorganize existing Services or Repository folders merely to enforce feature-first naming.
- Keep entry points, endpoints, and hosted-service loops thin. Put use-case orchestration in explicit handlers/services and provider I/O in adapters.
- In a layered solution, keep application/domain code independent of infrastructure and hosting. The host may reference infrastructure for dependency registration. Use interfaces owned by the consuming application boundary.
- Introduce a Domain project only when meaningful domain rules justify it. Introduce Contracts only for a real shared wire-contract consumer; avoid publishing internal entities.
- Justify interfaces by substitution, external boundaries, or meaningful test seams. Avoid one interface per class, generic repositories around every SDK, universal base services, or speculative extension frameworks.
- Introduce CQRS, MediatR, mapping frameworks, event buses, or additional services only when requirements justify their cost. Preserve intentional existing usage.

## Implement C# deliberately

Use typed configuration with startup validation for required values. Keep secrets outside source code. Use constructor injection and explicit DI lifetimes; hosted services must create scopes when consuming scoped dependencies. Avoid service location inside business logic.

Propagate CancellationToken through I/O and long-running processing. Avoid blocking on async work, async void outside event handlers, and unobserved background tasks. Bound concurrency at the relevant resource; do not create one task per item for unbounded input. Do not wrap normal asynchronous I/O in Task.Run.

Keep nullable contracts honest. Avoid null-forgiving operators used to hide missing initialization. Prefer explicit mapping where it stays clear. Preserve exception context; do not swallow failures or convert every failure into success-shaped defaults. Validate untrusted input at entry boundaries and enforce business invariants where they belong.

Use structured logs with correlation identifiers; exclude credentials and unnecessary payload data. Choose metrics with bounded cardinality. Handle authorization at the appropriate API/application boundary; authentication alone does not grant access to every resource.

For workers, queues, bulk processing, or ingestion, read [worker-ingester.md](references/worker-ingester.md). Do not apply queue/database machinery to applications that do not need it.

## Work with RPI

Keep the user's RPI implementation and commands. Use [rpi-checklist.md](references/rpi-checklist.md) to strengthen research, plan, implementation, and review outputs. Refer explicitly to this skill's files in each phase if the RPI tool does not retain them. Do not assume another agent or phase loaded these instructions.

When adding this guidance to a Copilot repository, merge [copilot-instructions.md](assets/copilot-instructions.md) into existing repository instructions, adapting it to local decisions. Never overwrite existing instructions wholesale.

## Verify and report

Run the repository's applicable restore/build/test/analyzer commands. Report actual commands and results, including any blocked restore or unavailable SDK. Do not claim a compilable solution based on static inspection alone.

Test behavior that could regress: business rules, boundary serialization, cancellation, retries, duplicate delivery, and partial failure as applicable. Use integration tests for provider/database behavior mocks cannot establish. For a structural refactor, verify project references and externally visible behavior. Recommend automated dependency checks for stable boundaries; do not pretend prose instructions enforce them.

Fix new failures attributable to the change. Do not suppress warnings, weaken assertions, or remove tests to manufacture success. Avoid creating redundant tests for trivial edits. Report changed boundaries, runtime behavior, verification evidence, and remaining limitations. For reviews, prioritize concrete failures and cite the responsible code before suggesting cosmetic reorganizations.
````

### .github/skills/csharp-architecture/references/api-core-reference.md

````markdown
# API + Core reference

## Evidence and scope

Source: three user-provided VS Code screenshots and the statement that the main functionality is in Core, supplied 2026-09-15. The original screenshots show folders and filenames. A later, partially cropped AdjustmentsController screenshot supplies the controller evidence below; project references and service implementations remain unseen. Treat the observed names as a reference, not proof of coupling, framework choice, or code quality.

Use this as the preferred starting layout for similar APIs. Preserve exact existing names in the reference repository; substitute the new product prefix when creating a different API. Do not copy every folder into every new solution. Do not infer hidden child contents.

## Observed repository layout

```text
cat-backend-api/
  .config/dotnet-tools.json
  .github/
    workflows/
    Dependabot.yml
  .kustomize/
    base/
      configmap.yml
      db-migration-job.yml
      deployment.yml
      hpa.yml
      ingress.yml
      kustomization.yml
      secretproviderclass.yaml
      service.yml
    overlays/
      development/
      production/
      staging/
      testing/
  docker-helpers/
  k8s/
  refs/
  samples/
  src/
    O.WP.CAT.API/
      _System/
        Builders/
        Configurators/
          AppConfigurationConfigurator.cs
          LoggingConfigurator.cs
          ServicesConfigurator.cs
          SwaggerConfigurator.cs
        Controllers/HealthController.cs
        Enrichers/
        Filters/SwaggerAuthorizeOperationFilter.cs
        Results/Result.cs
      Controllers/
      Properties/
      ReportTemplates/
      appsettings.Development.json
      appsettings.json
      DependencyInjection.cs
      Program.cs
      O.WP.CAT.API.csproj
    O.WP.CAT.API.Core/
      _System/
      Attributes/
      Configurations/
      Constants/
      DBContexts/
      Enumerator/
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
      O.WP.CAT.API.Core.csproj
    O.WP.CAT.API.IntegrationTests/
    O.WP.CAT.API.UnitTests/
```

The screenshot also partially shows O.WP.CAT.Database; its project type and role are unknown. Do not create or remove it based on this reference. Solution filename, SDK, packages, and all project references remain unverified. Deployment configuration names above are observations, not instructions to enable deployments or migrations.

Exclude bin, obj, local IDE state, _Logs, and temp output from new scaffolds. A folder visible in Explorer does not prove it is committed; inspect ignore rules and tracked files before cleanup. Verify whether temp or report folders contain intentional assets before changing them.

## Proposed responsibility contract

| Project | Intended responsibility | Reference policy to verify |
|---|---|---|
| API | HTTP endpoints, host lifecycle, configuration, logging setup, Swagger, health endpoints, DI composition | API may reference Core |
| Core | Main use cases and processing, validation, models, services, and existing persistence/integration implementations | Core must not reference the API project |
| UnitTests | Isolated behavior and business-rule tests | Reference the project(s) under test |
| IntegrationTests | HTTP/DI/persistence/provider integration behavior | Reference API and/or Core as required |

This is a recommended dependency contract, not a claim about the unseen csproj files. Core is a pragmatic business-and-data library; infrastructure dependencies such as a database library may live there. Do not add an Infrastructure assembly just to match generic Clean Architecture advice.

Keep HTTP orchestration thin. A proposed flow is API controller -> Core service or handler -> Core repository/provider -> response mapping. Confirm one actual flow before enforcing the Handler/Services/Repository relationship. A controller need not traverse both a handler and a service, and a pure calculation need not call a repository.

Preserve _System as the existing convention; do not rename it to Shared or Common automatically. Keep business controllers distinct from the observed _System health controller. Inspect Core Middlewares before moving them: reusable HTTP middleware may reasonably live in a library, but business use cases should not require HTTP context unnecessarily. Do not assume Result.cs is the universal response wrapper or that every model is an EF entity.

## Scaffolding and RPI rules

1. Start similar new APIs with API and Core, adding tests for actual behavior. Create folders only when needed by the first use case.
2. For modifications, trace existing DI registration before adding another registration helper. Inspect Program.cs, DependencyInjection.cs, and ServicesConfigurator.cs together.
3. Keep role-based folders where established; introduce feature subfolders only when they improve navigation without breaking the convention.
4. Treat db-migration-job.yml, Core/Migrations, and the partly visible Database project as unresolved migration ownership. Inspect actual tooling before proposing schema changes or deployment commands.
5. Preserve Kustomize environment names and file conventions in this repository. Do not infer separate namespaces/databases or running infrastructure from manifests alone. Do not copy both k8s and .kustomize into a new project without establishing their distinct purpose.
6. In the architecture plan, distinguish observed layout, proposed boundaries, and unresolved details. Verify the actual project-reference graph, DI wiring, and tests during implementation.

## Observed controller pattern

The supplied endpoint screenshot shows `O.WP.CAT.API.Controllers.v1.AdjustmentsController : BaseController`. A versioned namespace alone does not establish URL/API versioning behavior; inspect routing and startup configuration.

| Element | Visible evidence |
|---|---|
| Constructor injection | ILogger<AdjustmentsController>, IAdjustmentService, IHttpContextAccessor; the accessor appears forwarded to the base constructor, whose remainder is cropped |
| GET | HttpGet route `adjustments/windturbine/{wtgRdsppId}`; calls `_adjustmentService.GetAdjustmentsForWindTurbineAsync(...)`; returns `new JsonResult(adjustments)` |
| GET response metadata | ProducesResponseType declares IEnumerable<AdjustmentGroupingDto> for HTTP 200 |
| POST | HttpPost route `adjustments/add`; binds AdjustmentViewModel from the request body; calls `_adjustmentService.AddAdjustments(...)` |
| POST validation | ValidateAdjustmentFutureDateRange attribute, ArgumentNullException.ThrowIfNull, and ModelState.IsValid check |
| POST response metadata | ProducesResponseType declares bool for HTTP 200; the full return and failure branches are cropped |
| Models | A visible import references Core.Models.ViewModel; actual DTO and interface declarations remain unseen |
| Logging | Controller logs request/progress messages around service calls |

The confirmed delegation is controller -> injected IAdjustmentService. The user describes main functionality as belonging to Core; verify the interface namespace and implementation before documenting exact source paths. No repository, handler, transaction, or mediator call is visible. Do not invent an extra handler/mediator layer in order to complete this flow.

### Guidance derived from the example

- Prefer thin controllers with injected use-case services for similar APIs. Preserve BaseController and existing routing/response conventions after inspecting their implementations. Do not create a generic controller superclass solely because this example has one.
- Preserve existing wire responses and status codes during structural work. JsonResult is the observed style, not a requirement for every future endpoint. Any proposed change to typed results or error contracts should be explicit and compatible.
- Keep request models and response DTOs distinct when their semantics differ. Do not serialize database entities merely because the example stores models in Core.
- Inspect BaseController, global filters, middleware, and the validation attribute before deciding that manual ModelState checks or null guards are redundant. ApiController behavior and exception-to-HTTP mapping are not visible here. Do not claim null input currently produces a specific status code.
- Inspect whether date-range validation is a business invariant that must also hold for non-HTTP callers. Do not rely solely on a controller attribute for such invariants.
- Preserve existing context handling until understood. For new use cases, prefer passing required caller identity explicitly or through a focused abstraction over coupling business logic to the whole HttpContext. The screenshot does not reveal the complete second AddAdjustments argument.
- Verify cancellation propagation in complete signatures; right-hand code is cropped, so absence cannot be concluded from the image. Add propagation through async I/O where needed and compatible.
- For new logs, use structured templates and meaningful identifiers without sensitive payloads. Do not copy interpolated constant strings or the unrelated sample comment as conventions.
- Treat ProducesResponseType as declared documentation, not proof that runtime results match it. Check actual service returns and all controller branches with relevant HTTP tests.

## Details needed for a more exact template

The controller example is now supplied. Next request IAdjustmentService and the matching AdjustmentService method, then the repository/model used by that method if applicable. Request BaseController and the custom validation attribute when resolving HTTP behavior; do not ask the user to resend the same screenshot. Also request both csproj files, Program.cs, DependencyInjection.cs, and ServicesConfigurator.cs to establish dependencies and wiring. Inspect the Database project and migration setup only when that scope is relevant. Do not request secrets or full application settings.
````

### .github/skills/csharp-architecture/references/rpi-checklist.md

````markdown
# RPI architecture checklist

Use the installed RPI plugin's actual commands; this document does not assume a specific RPI distribution. If phases have separate contexts, explicitly pass the skill path, specification, architecture contract, and previous phase's artifact.

## Research

- Read the current repository and identify deployment units, project references, established patterns, package/SDK constraints, and representative tests.
- Trace a relevant existing use case. Record real file paths and important failure paths.
- Identify requirement gaps that affect architecture; label assumptions and ask only for genuinely blocking information.
- Preserve facts separately from proposed improvements. Do not modify production code during a research-only request.

## Plan

For similar CAT-style APIs, load `api-core-reference.md` and start with API + Core. Distinguish screenshot observations from verified source behavior, and justify deviations from this reference.

Produce an architecture contract with:
1. Requirement and scope, including what observable behavior must remain compatible.
2. Proposed solution tree, with each project justified.
3. Table of responsibilities and allowed project references.
4. One end-to-end flow, including I/O boundaries and failure handling.
5. Proposed packages and abstractions with reasons.
6. Data volume, concurrency, idempotency, and state-transition decisions where relevant.
7. Small implementation increments, files affected, and acceptance evidence per increment.
8. Concrete repository commands for build and relevant tests; identify environment prerequisites.

If the user requested review before coding, pause here. Otherwise continue within the authorized task. Avoid repeated approvals for routine implementation details.

## Implement

Implement one working vertical slice first. Verify wiring and behavior before copying the pattern broadly. Stay within the architecture contract; explain material deviations and update it. Do not generate unused scaffolding to satisfy the proposed tree.

Use TDD when requested: write a meaningful failing behavior test, verify the failure reason, implement the smallest correct change, then refactor with the test passing. Compilation alone does not validate the business requirement; mocked happy paths do not establish crash recovery.

## Review

Compare actual project references and representative code against the contract. Check scope, public contract compatibility, DI lifetimes, resource bounds, failure semantics, and actual test results. Report concrete findings by severity, with file evidence and a recommended fix. State which claims remain unverified. Do not equate a passing build or clean folder tree with production readiness.
````

### .github/skills/csharp-architecture/references/structure-patterns.md

````markdown
# Structure patterns

These are illustrative defaults, not mandatory templates. Preserve established naming, solution format, and test directory conventions.

## User reference takes precedence

For similar CAT-style APIs, follow [api-core-reference.md](api-core-reference.md) before considering the generic alternatives below. Do not replace the two-project API/Core structure with Application/Infrastructure/Domain projects by default. The reference keeps tests under src as shown; do not move them solely for convention.

## Small worker or CRUD API

Start with one executable project plus tests when the application has a small cohesive scope. Use feature folders inside it. Isolate provider calls in adapters even when they share the same assembly. Explain that folder boundaries are conventions unless enforced separately.

```text
src/ProductionIngestor/
  Program.cs
  Hosting/IngestionWorker.cs
  Ingestion/IngestWindow.cs
  Ingestion/IngestionRequest.cs
  Discovery/
  Checkpointing/
  Adapters/Source/
  Adapters/Storage/
  Adapters/Messaging/
  Configuration/
  Observability/
tests/ProductionIngestor.Tests/
```

Create only folders that contain needed code. Add integration test projects when their dependencies or execution lifecycle warrant separation. Do not create Refresh, Contracts, Domain, or API endpoint folders without a requirement.

## Worker with stable adapter boundaries

Consider this when processing rules change independently of provider integrations, or compile-time isolation has concrete value:

| Project | Responsibility | Allowed production references |
|---|---|---|
| ProductionIngestor.Worker | Hosting, DI registration, worker lifecycle | Application, Infrastructure |
| ProductionIngestor.Application | Ingestion use cases, checkpoint decisions, boundary interfaces | None of the host or infrastructure projects |
| ProductionIngestor.Infrastructure | Source, storage, queue, checkpoint implementations | Application |

Application interfaces describe needed operations, not cloud SDK APIs. Infrastructure implements those interfaces. Worker owns the composition root. Application tests need no cloud SDK. Infrastructure integration tests verify actual adapter behavior in appropriate test environments.

A separate Domain assembly is unnecessary if Application already holds a modest amount of processing logic. Separate Contracts only when an API/message contract is genuinely consumed elsewhere and needs an intentional compatibility/versioning policy.

## Business API with substantial domain rules

Use Api -> Application and Infrastructure, Infrastructure -> Application and Domain, Application -> Domain. Domain must not reference the host, application orchestration, or provider SDKs. Api's infrastructure reference is for wiring, not provider calls in endpoints. Add Projects only when this separation earns its maintenance cost.

For simple CRUD, the small-application pattern may be sufficient. EF Core models are not automatically domain models. Share models only when semantics and evolution actually match.

## Repository configuration

Honor the repository's SDK pin, NuGet sources, package version strategy, analyzers, editor configuration, and CI. For new repositories, propose a supported SDK pin, nullable checking, and coherent analyzer settings. Reuse centralized package management if present; do not add it merely for one dependency. Do not change all existing warnings into build errors without assessing the baseline.

Use a checked-in working starter solution or dotnet template when available. This reference is architecture guidance, not a compiled starter template. Verify any generated solution with the available SDK before calling it runnable.

## Refactoring an existing poor structure

1. Identify concrete coupling, runtime, or testing problems with file/project evidence.
2. Map each existing responsibility to its proposed owner; identify public interfaces and behavior to preserve.
3. Move one cohesive use case, update references/namespaces/DI, and build.
4. Verify serialization, migration discovery, reflection-based registration, or deployment paths when affected.
5. Remove obsolete projects only after references and builds prove they are unused and removal is within scope.

Do not rewrite business logic while reorganizing files unless required to repair a demonstrated defect. Do not rename a project simply to make its name match an architectural fashion.
````

### .github/skills/csharp-architecture/references/worker-ingester.md

````markdown
# Workers and large-data ingesters

Apply only relevant items and make guarantees explicit. Do not create a distributed orchestration platform for a simple batch job.

## CAT ingestor structure

For new CAT-style ingestors, use the role-based Core layout agreed for ProductionIngestor on 16 September 2026. Preserve another project's explicitly approved architecture; do not restructure it automatically. ProductionIngestor is a concrete example, not a requirement to copy its providers, domain names, or every folder.

### Project responsibilities

| Project | Responsibility | Allowed production references |
|---|---|---|
| `<Name>Ingestor.Core` | Ingestion operations, models, capability interfaces, validation, resolution, persistence, and external integrations | Approved libraries and genuine shared message contracts; never Worker or API |
| `<Name>Ingestor.Worker` | Configuration binding, DI, process lifecycle, request parsing, cancellation, and exit codes | Core and required host/platform libraries |
| `<Name>Ingestor.Api` (optional) | HTTP requests, authorization, scheduling/submission of work, and API composition | Core and required host/platform libraries; never Worker |
| UnitTests / IntegrationTests | Isolated behavior tests / provider and host integration checks | Projects under test |

Core is a pragmatic functionality library, not a dependency-free Domain assembly. SQL and cloud SDK dependencies may live there. Do not add Infrastructure, Application, or Domain projects without a concrete boundary requirement. Do not add an API unless the ingestor needs HTTP entry points. Keep existing solution and test locations; the tree below describes roles, not mandatory repository paths.

### Core folder layout

```text
<Name>Ingestor.Core/
	Configuration/             Options properties, defaults, section names
	Validators/                Configuration and input validation
	Resolvers/                 Settings selection, fallback, and normalization
	Interfaces/                Capability contracts consumed by operations
	Factories/                 Database connection construction and lifecycle
	Authentication/            Credential and token acquisition
	Repositories/              Application-owned state persistence
	Adapters/
		Source/                  Reads from the external source system
		Messaging/               Publishes or receives external messages
		Storage/                 Object storage operations
	Operations/
		Discovery/               Discovers changes and plans bounded windows
		Extraction/              Coordinates source snapshot extraction
		Execution/               Orchestrates an ingestion run
		Refresh/                 Plans requested reprocessing
	Serialization/             Artifact encoding, such as Parquet
	Raw/                       Raw artifact paths and related conventions
	Models/
	Enums/
	Constants/
	Monitoring/
```

Create a folder only when it owns actual code. Keep namespaces aligned with the chosen folder convention and update all consumers when moving types. Do not turn Factories, Resolvers, or Operations into generic catch-all folders. This convention does not imply that every ordinary C# class must separate properties from behavior.

### External integration boundaries

| Integration | ProductionIngestor example | Location |
|---|---|---|
| Source SQL Server | `IProductionSourceAdapter` / `SqlProductionSourceAdapter` | Interface in Interfaces; implementation in Adapters/Source |
| Control-plane PostgreSQL | `IExtractionCursorRepository` / `PostgresExtractionCursorRepository` | Interface in Interfaces; implementation in Repositories |
| SQS | `IRawArtifactPublisher` / `SqsRawArtifactPublisher` | Interface in Interfaces; implementation in Adapters/Messaging |
| Azure Blob Storage | `IRawObjectStore` / `AzureBlobRawObjectStore` | Interface in Interfaces; implementation in Adapters/Storage |

Repositories persist application-owned state such as cursors and leases. Adapters integrate with external sources, messaging, and storage. A repository is technically also an adapter; the sibling folders are a navigation convention, not two mandatory layers. Do not wrap SQS or blob SDKs in generic repositories for naming symmetry.

Operations depend on capability interfaces at external I/O boundaries, not concrete SQL/AWS/Azure implementations. Pure planners and serializers can remain concrete. The flow is `Host -> Operation -> Capability interface -> Repository or adapter -> External system`, not a mandatory `Repository -> Adapter` chain. Hosts select implementations and own DI lifetimes. A SQL adapter or repository may use a connection factory; factories construct connections and manage provider lifecycle, not execute ingestion queries. Authentication providers acquire credentials; settings resolvers do not fetch tokens.

### Options, validators, and resolvers

- Keep options data-only: bindable properties, defaults, and section-name constants. Do not leave `Validate`, `ValidateForIngestion`, `Resolve`, or `BuildConnectionString` methods, including forwarding wrappers, on options classes.
- Use one top-level options class per file. Small supporting configuration types used only by their parent may share its file. Use one validator per options type that needs validation; do not collect all options or validators into a single file.
- Implement `IValidateOptions<T>` in Validators and return `ValidateOptionsResult.Fail` for expected invalid configuration. Register the validator with the host and use `AddOptions<T>().Bind(...).ValidateOnStart()`. Start the host before claiming work or opening provider connections.
- Keep a single implementation of each validation rule. Where non-DI callers need fail-fast checks, expose a deliberate validator entry point such as `ValidateAndThrow`; use it from the options-validator adapter and direct callers. Preserve direct-call exception contracts deliberately. Do not add public methods or interfaces without a consumer.
- Move settings selection and normalization to Resolvers. For example, `SqlSourceSettingsResolver` chooses keyed versus legacy source settings and resolves service-principal references. Reuse that logic in validation and connection construction instead of duplicating precedence rules. Build provider connection strings in Factories, not in options or validators.
- Cross-options validation may depend on `IOptions<T>` for sibling settings, but its dependency graph must be acyclic. Do not make source validation resolve SQL options while SQL validation resolves the same source options. Read a narrowly identified raw fallback or use an independent resolver where needed. Register every dependency before host startup; registration order alone does not break a cycle.
- Invalid options at startup normally raise `OptionsValidationException`; multiple startup failures may produce `AggregateException`. Assert the affected options type and meaningful failures in tests. Do not retain inline throwing validation delegates merely to preserve a test that expects the old startup exception type.
- Validate format, required values, ranges, and local prerequisites without authenticating to remote services. Keep real credential acquisition lazy and connection-scoped as required. Preserve configured fallback precedence, TLS verification, token renewal, and cancellation; never log credentials or raw secret-bearing connection strings.

### Reuse and verification checklist

1. Record the new ingestor's source, state store, message transport, object store, and durable-success point. Implement only the capability interfaces it actually needs.
2. Keep provider code out of Operations and method bodies out of options classes. Keep API and Worker composition separate from Core and check actual project references.
3. For a refactor, move one cohesive responsibility, update namespaces/callers/DI, and run focused tests before moving the next. Preserve configuration keys, wire contracts, schema ownership, and behavior unless a change is explicitly approved.
4. Test keyed/legacy precedence, invalid configuration, direct validation, cross-options startup failures, connection security, and lazy credentials. Build Core, Worker, optional API, and integration-test projects; run the ingestor unit suite. Compilation does not establish live SQL/SQS/blob behavior.
5. Run relevant provider integration tests only with their documented prerequisites. Report commands, exit codes, warnings, test counts, and unverified external behavior. Preserve unrelated edits and do not commit unless requested.

## Processing boundaries

Distinguish scheduled one-shot jobs from continuous queue consumers. A Kubernetes Job normally performs its bounded work and exits with a meaningful status; a continuous consumer runs until cancellation. Do not accidentally deploy an infinite polling loop as a one-shot job. Keep scheduling ownership explicit.

Split large ranges into deterministic bounded windows and source pages. Define interval semantics, UTC/source timezone interpretation, partition keys, and stable sort/tie-break keys. Keyset pagination needs a stable ordering and change-capture strategy; a timestamp alone may skip records with equal timestamps. Define handling of late-arriving or revised data rather than assuming a completed date can never change.

Stream or batch data with explicit limits on pages, records, buffers, and concurrent uploads. Bound total concurrency across windows, workers, and replicas. Tune against source limits and connection pools. A semaphore alone does not prevent allocating millions of waiting tasks.

## Durable success and recovery

Define what Completed means: durable data, durable manifest if used, and reliable downstream notification intent. Specify ownership and ordering for each state transition.

Do not assume database updates, object writes, and queue publishing form one transaction. If data is written and publishing fails, use a justified outbox or reconciliation/replay mechanism. Record enough durable identity to retry without losing or duplicating logical work. A transactional outbox atomically covers its database transaction; it does not automatically make object storage transactional.

Advance the cursor only after the required durable outcomes are secured. With parallel windows, advance a contiguous committed frontier, not merely the latest finished window. Preserve failed windows for retry.

For shared claims, use atomic conditional updates. Where leases are used, define owner, expiry, heartbeat, reclaim behavior, and stale-owner protection through fencing/version conditions. A lease by itself does not prevent a resumed old worker from writing. Avoid holding a database transaction open during network I/O.

Treat queue delivery as potentially duplicated. Use stable work identities and idempotent writes. Acknowledge/delete only after durable processing success. Match visibility/lock duration to processing, renew when necessary, and define bounded retries and dead-letter handling. Queue redelivery does not automatically repair an application's stale Running state.

Classify transient versus permanent errors. Assign retries to a defined layer, use bounded backoff with jitter, and account for SDK retries. Authentication/configuration failures should surface clearly rather than loop indefinitely. Cancellation is not a normal retryable error.

## Storage and portability

Use provider-supported publication semantics. Do not assume object storage supports atomic filesystem rename; on S3 a rename is generally implemented as copy/delete. Consider immutable versioned objects and an authoritative commit manifest/pointer with concurrency control. Readers must distinguish committed data from incomplete uploads.

Abstract storage operations that the use case needs, not every provider feature. Keep cloud SDK types in adapters. Explicitly test semantics that differ across providers: conditional writes, listing, identity, checksums, and retries. Record run/window/source revision and committed object identities for traceability.

Keep environment-specific configuration out of business records when deployment uses separate databases per environment. Follow the repository's established migration system; do not invent a second schema-deployment mechanism or assume SQL Server tooling applies to PostgreSQL.

## Lifecycle and visibility

Validate configuration before claiming work. Use workload identity/short-lived credentials when the platform supports them; respect existing approved secret retrieval. Bound shutdown: stop claiming work, cancel/finish in-flight work according to policy, and leave incomplete work recoverable.

Track throughput, source errors, work lag, queue age, retries, and completion coverage separately. An available source is not proof that the expected data is complete. Include run ID, work ID, source, window, and attempt in appropriate logs/traces. Avoid per-item identifiers as metric labels.

## Meaningful verification scenarios

Choose scenarios applicable to the actual guarantees: duplicate delivery; source page fails midway; cancellation while writing; upload succeeds but notification fails; crash after claim; stale owner resumes after lease expiry; window N+1 completes before N; multiple records share a cursor timestamp; revised historical data arrives. Verify bounded memory/concurrency with a representative load or instrumented stress harness when claiming large-data efficiency. Do not claim ADF-equivalent performance without measurements.
````

### .github/skills/csharp-architecture/assets/copilot-instructions.md

````markdown
# Repository engineering instructions

Merge these defaults with existing repository instructions and specific architecture decisions. User requirements and established repository constraints take precedence.

For C# solution design, structural refactoring, or changes crossing project boundaries, read `.github/skills/csharp-architecture/SKILL.md` and the relevant linked references before planning. In separate RPI phases, pass those paths explicitly; do not assume the previous phase's context is retained.

- For similar CAT-style APIs, use API + Core as the preferred starting layout and read `.github/skills/csharp-architecture/references/api-core-reference.md`. Keep main functionality and existing persistence implementations in Core; do not introduce extra layers or copy every visible folder by default. Verify API -> Core references and preserve Core independence from the API project.
- Inspect the existing solution, project references, representative feature, tests, and SDK/package settings before changing the design.
- Preserve the approved structure for routine feature work. For a new structure, first document each project's responsibility, allowed references, and one complete use-case flow. Keep the process proportional to the task.
- Require a concrete reason for additional projects, frameworks, shared contracts, or abstractions. Keep entry points thin and external I/O behind appropriate boundaries.
- Respect approved framework versions, package feeds, schema deployment, and secret handling. Do not silently upgrade dependencies or bypass configured sources.
- Propagate cancellation, bound large-data concurrency, and make durable-success and retry ownership explicit when applicable.
- Build and run relevant behavior tests. State commands, results, and any unavailable prerequisites accurately. Never weaken tests or suppress new failures to claim success.
- Preserve unrelated changes and public behavior. Do not overwrite instructions, rewrite the entire solution, or introduce unused layers without a task-specific reason.

These instructions guide the agent; enforce stable project boundaries and quality requirements with appropriate CI checks.
````

### .github/copilot-instructions.md

````markdown
# Repository engineering instructions

Merge these defaults with existing repository instructions and specific architecture decisions. User requirements and established repository constraints take precedence.

For C# solution design, structural refactoring, or changes crossing project boundaries, read `.github/skills/csharp-architecture/SKILL.md` and the relevant linked references before planning. In separate RPI phases, pass those paths explicitly; do not assume the previous phase's context is retained.

- For similar CAT-style APIs, use API + Core as the preferred starting layout and read `.github/skills/csharp-architecture/references/api-core-reference.md`. Keep main functionality and existing persistence implementations in Core; do not introduce extra layers or copy every visible folder by default. Verify API -> Core references and preserve Core independence from the API project.
- Inspect the existing solution, project references, representative feature, tests, and SDK/package settings before changing the design.
- Preserve the approved structure for routine feature work. For a new structure, first document each project's responsibility, allowed references, and one complete use-case flow. Keep the process proportional to the task.
- Require a concrete reason for additional projects, frameworks, shared contracts, or abstractions. Keep entry points thin and external I/O behind appropriate boundaries.
- Respect approved framework versions, package feeds, schema deployment, and secret handling. Do not silently upgrade dependencies or bypass configured sources.
- Propagate cancellation, bound large-data concurrency, and make durable-success and retry ownership explicit when applicable.
- Build and run relevant behavior tests. State commands, results, and any unavailable prerequisites accurately. Never weaken tests or suppress new failures to claim success.
- Preserve unrelated changes and public behavior. Do not overwrite instructions, rewrite the entire solution, or introduce unused layers without a task-specific reason.

These instructions guide the agent; enforce stable project boundaries and quality requirements with appropriate CI checks.
````

## Validation and limits

The API reference is based on screenshots, not source-code inspection. The ingestor reference captures the agreed ProductionIngestor structure and options separation, checked against its implementation. Verification results are specific to that implementation and do not establish readiness for another ingestor or live provider environment. This package contains guidance, not a compiled starter solution.
