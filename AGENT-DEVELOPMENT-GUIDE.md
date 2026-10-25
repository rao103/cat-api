# CAT DL Backend Service - Agent Development Guide

This repository contains the backend ingestion, harmonization, and loss-calculation services for the CAT platform. Use this file as a navigation guide before changing code. The root `README.md` and the design documents under `docs/superpowers/specs/` contain detailed behavior and operational contracts.

## Start Here

1. Read the root `README.md` for supported commands and runtime configuration.
2. Identify the data domain being changed: Production, Alarm, Outage, Curtailment, Harmonized, or Calculation.
3. Read the relevant Core project before the Worker or API project. Core owns the processing contract; hosts provide configuration and dependency injection.
4. Find the closest focused test project and run it before making broad changes.
5. Check `git status` before editing and preserve unrelated worktree changes.

The solution is `O.WP.CAT.LossCalculation.Platform.sln`. The repository targets .NET 10 through the solution/build configuration; do not add a separate framework version to an individual project without a specific reason.

## Repository Layout

```text
src/
  Api/                         ASP.NET Core APIs that start or control ingestion jobs
  Raw/Ingestors/               Source-specific raw-data ingestion pipelines
  Shared/Messaging.Contracts/  Contracts shared by publishers and consumers
  Harmonized/                  Raw-to-harmonized processing
  Calculation/                 Loss calculation processing

tests/Raw/                     Focused unit, integration, and end-to-end tests
deploy/production-ingestor/    Kustomize deployment for the Production Ingestor
tools/sqs-smoke-test/          Explicit smoke test for SQS publishing
docs/superpowers/specs/        Approved design and implementation documents
localdbconnection/             Local database/tunnel setup documentation and scripts
scripts/                       Repository maintenance scripts, including RDS CA refresh
```

## Project Boundaries

### Raw ingestors

Each raw ingestor follows this structure:

```text
src/Raw/Ingestors/<Name>/
  <Name>Ingestor.Core/    Domain models, interfaces, adapters, processing, persistence abstractions
  <Name>Ingestor.Worker/  One-shot executable; parses input, binds options, registers services, runs Core
  db/                     SQL migration scripts or source-specific database setup
```

The current ingestors are:

- `Production`: production measurements and raw Parquet publication.
- `Alarm`: alarm discovery and open-alarm polling.
- `Outage`: outage classification changes and deletion processing.
- `Curtailment`: curtailment source processing.

The Core project should remain host-independent. Keep source access, object storage, messaging, and database implementations behind interfaces. The Worker owns host configuration, options validation, request parsing, dependency injection, exit-code behavior, and process lifetime.

Production and Outage also have API projects under `src/Api/`. These APIs start Kubernetes Jobs; they do not copy their own configuration into the Worker. The Worker Job configuration is the source of truth for ingestion credentials and runtime settings.

### Shared contracts

`src/Shared/Messaging.Contracts/` contains message types exchanged between pipeline stages. Change a contract only with coordinated publisher, consumer, and test updates. Preserve serialized field names and compatibility requirements unless the relevant design document explicitly changes them.

### Harmonized and Calculation workers

```text
src/Harmonized/Harmonizer/
  Harmonizer.Core/
  Harmonizer.Worker/

src/Calculation/LossCalculation/
  LossCalculation.Core/
  LossCalculation.Worker/
```

These use the same Core/Worker separation. Start with the Core project and its tests, then inspect the Worker for hosting and configuration details.

## Typical Ingestion Flow

Most raw ingestion runs follow this order:

```text
Worker request/configuration
  -> validate options and parse windfarm/run input
  -> acquire a durable, fenced extraction lease
  -> discover source changes or read the requested historical range
  -> write raw and quarantine artifacts
  -> publish an artifact-reference message
  -> commit the extraction cursor only after publication succeeds
  -> complete or fail the audit run
  -> release the lease and return an explicit exit code
```

Important invariants:

- Publication must happen before the durable cursor/checkpoint advances.
- Lease ownership must be checked by the database operation that commits state.
- A rejected lease-fenced operation is not success; the runner should report a zombie/lost-ownership outcome as defined by that ingestor.
- Failed windows may replay already-published data, so downstream consumers must be idempotent.
- Cancellation must stop work without falsely committing progress.
- Historical or refresh modes must not advance an incremental cursor unless their design explicitly says so.

The exact source contract differs by ingestor. Do not copy Production semantics into Alarm, Outage, or Curtailment without reading that ingestor's README section, interfaces, and tests.

## Where to Change Code

| Task | Start in | Usually also inspect |
| --- | --- | --- |
| Source query or paging | `Core/Adapters/` or `Core/Operations/Discovery/` | source adapter tests, query-builder tests, options validators |
| Lease, cursor, or run audit | `Core/Repositories/` and `Core/Interfaces/` | runner tests, migration SQL, failure/retry behavior |
| Raw Parquet or quarantine output | `Core/Serialization/` and `Core/Raw/` | artifact writer tests, path-builder tests, downstream contract |
| Blob or SQS publishing | `Core/Adapters/` | publish-before-commit tests and message contract |
| Runtime configuration | `<Name>Ingestor.Worker/Configuration/` and `Program.cs` | `appsettings*.json`, options validators, deployment manifests |
| API-triggered refresh | `src/Api/<Name>Ingestor.Api/` | Worker request parser and Job template configuration |
| Shared event/message shape | `src/Shared/Messaging.Contracts/` | every publisher, consumer, and serialized fixture |
| Kubernetes behavior | `deploy/production-ingestor/` | base manifests, overlays, ConfigMaps, Secrets, service accounts |

Prefer a small change in the owning layer. Controllers and Worker entry points should remain thin; processing behavior belongs in Core services and operations.

## Configuration and Secrets

- Never commit credentials, tokens, connection strings containing secrets, or developer AWS profiles.
- Use the existing deployment secret/configuration mechanism for environment-specific values.
- Production source SQL authentication is configured through the Worker's keyed `ConnectionStrings` and `ServicePrincipals` settings. Required values such as tenant IDs and client secrets are injected externally.
- Production control-plane PostgreSQL settings live under the Worker's `ControlPlane` configuration. Local tunnel details are documented in `localdbconnection/`.
- Production SQS publishing uses `Sqs:Region` and `Sqs:RawFanOut:QueueUrl`. Do not revive the removed Worker fallback to `Source:SqsQueueUrl`.
- RDS TLS uses the official CA bundle and `SSL Mode=VerifyFull`. Local and EKS paths are different; do not introduce an SSM tunnel or developer profile into the EKS workload.
- The API does not transfer its settings to a Worker Job. Check the CronJob/Job template when debugging API-triggered ingestion.

## Deployment

The Production Ingestor deployment is under `deploy/production-ingestor/`:

- `base/` contains shared Kustomize resources, application configuration, the Worker CronJob, and the API Deployment.
- `overlays/` contains environment-specific patches. Inspect the actual overlay names before assuming a dev/test/preprod/prod matrix.
- The RDS CA PEM is repository trust material, not a secret. It is generated into a dedicated ConfigMap and mounted read-only into the Worker pod.
- Use `scripts/update-rds-ca.ps1` to refresh the CA bundle; validate the rendered Kustomize output after changing deployment wiring.

Do not apply migration SQL automatically from application startup unless the existing project explicitly does so. The SQL files under an ingestor's `db/migrations/` folder are the schema/source-of-truth scripts for that area and may require an operator-managed deployment step.

## Testing and Verification

Build the solution:

```powershell
dotnet build O.WP.CAT.LossCalculation.Platform.sln
```

Run the focused raw-ingestor suites:

```powershell
dotnet test tests/Raw/ProductionIngestor.UnitTests/ProductionIngestor.UnitTests.csproj
dotnet test tests/Raw/ProductionIngestor.IntegrationTests/ProductionIngestor.IntegrationTests.csproj
dotnet test tests/Raw/AlarmIngestor.UnitTests/AlarmIngestor.UnitTests.csproj
dotnet test tests/Raw/OutageIngestor.UnitTests/OutageIngestor.UnitTests.csproj
dotnet test tests/Raw/CurtailmentIngestor.UnitTests/CurtailmentIngestor.UnitTests.csproj
```

For a narrow change, run the affected project first, then build the solution if the change touches shared contracts, project references, deployment assumptions, or common runner behavior. Use `--no-restore` only when dependencies are already restored and the result is intentional.

Tests are mostly unit-level and use in-memory storage or fakes. They do not prove live PostgreSQL, SQL Server, Azure Blob, SQS, Kubernetes identity, or source-provider behavior. Report those live checks separately rather than treating a passing unit suite as deployment verification.

## Agent Safety Rules

- Inspect the current file contents before trusting older plans, memories, or delegated-agent claims.
- Preserve existing user or parallel-agent changes; do not reset or clean the worktree broadly.
- Avoid broad renames and regex edits in C# because string literals and escape sequences can be changed accidentally.
- Keep schema changes explicit and document whether they were applied anywhere. Do not run live migrations as part of a normal code change unless requested.
- When a test or build appears to run a stale command, use a fresh focused invocation and capture its result in a uniquely named log or TRX file.
- Update the root `README.md` when a new ingestor, source contract, configuration rule, or known limitation becomes part of the supported repository behavior.

## Detailed References

- `README.md`: supported commands, runtime configuration, Production/Alarm/Outage behavior, and operational limitations.
- `CSharp-Copilot-Setup.md`: reusable Core/Worker/API structure and implementation conventions.
- `ProductionIngestor-RDS-TLS-Implementation.md`: RDS TLS, CA bundle, Kustomize, and local debugging design.
- `docs/superpowers/specs/`: approved feature designs and source/provider decisions.
- `localdbconnection/README.md`: local PostgreSQL/SSM setup and connection prerequisites.
- `deploy/production-ingestor/`: deployment manifests and Kustomize overlays.
