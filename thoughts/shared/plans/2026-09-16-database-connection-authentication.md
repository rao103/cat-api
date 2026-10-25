# Database connection authentication implementation

Approved specification: [database connection authentication](../../../docs/superpowers/specs/2026-09-16-database-connection-authentication-design.md).
Follow the repository C# architecture skill and its worker-ingester reference.
Preserve Core independence, net8.0, existing provider packages, and unrelated work.
The referenced writing-plans skill is unavailable; this plan uses the repository's
existing plan location. No commit or cloud mutation is authorized.

## Phase 1: Provider contract and PostgreSQL authentication

- [x] Add a small Core connection-factory contract; preserve the typed SQL factory API.
- [x] Add validated PostgreSQL IAM/legacy configuration and a pooled Npgsql factory.
- [x] Acquire fresh AWS credentials when signing tokens; keep signing endpoint/port
      separate from the connection port and enforce TLS verification.
- [x] Test Azure compatibility, PostgreSQL settings, token callbacks, and failures.

Check: focused SourceAuthenticationTests and ControlPlaneAuthenticationTests.

## Phase 2: Worker and cursor integration

- [x] Route PostgreSQL cursor connections through the factory without changing SQL.
- [x] Bind and validate explicit control-plane settings, retaining legacy fallback
      only when the explicit section is absent.
- [x] Register disposable factories with the Worker; preserve source/SQS behavior.
- [x] Test startup validation, provider rejection, and connection lifecycle.

Check: ProductionIngestor unit suite and Worker build.

## Phase 3: Local and EKS setup

- [x] Add native Windows PowerShell tooling, non-secret CAT configuration, and
      instructions for SSO, role profiles, prerequisites, Windows hosts, CA, and SSM.
- [x] Add explicit start/stop behavior and reject occupied tunnel ports.
- [x] Document EKS IAM, database grants, networking, CA mount, and CronJob settings
      without applying infrastructure or inventing unknown identifiers.
- [x] Document Azure connection-level support and cursor-provider limitation.

Check: PowerShell parser and offline validation, JSON parsing, existing integration
suite, API build, editor diagnostics, and whitespace checks.

## Live verification gate

Requires completed RDS provisioning, IAM auth enabled, actual RDS resource ID,
approved database user and grants, local database role ARN when required, CAT
bastion, confirmed SSO URL, and EKS identity/network configuration. Do not expose
secrets or invoke ingestion as a smoke test. Use a read-only database query and
verify new connections after token expiry from Windows and EKS. Azure requires its
own authorized credentials/network access. Report unavailable live checks explicitly.

## Verification results (2026-09-16)

- `dotnet test tests/Raw/ProductionIngestor.UnitTests/ProductionIngestor.UnitTests.csproj`:
      122 passed, including AWS/SQL authentication and Worker configuration coverage.
- `dotnet test tests/Raw/ProductionIngestor.IntegrationTests/ProductionIngestor.IntegrationTests.csproj`:
      5 passed. These are local provider/serialization tests, not live cloud tests.
- `dotnet build src/Api/ProductionIngestor.Api/ProductionIngestor.Api.csproj`: passed.
      The unit test build also built the Worker successfully.
- PowerShell parser, offline prerequisite check, appsettings target assertion,
      missing-SSO validation before profile writes, and redundant-JSON absence: passed.
- Editor diagnostics and `git diff --check`: no errors reported.
- User correction incorporated: all connection configuration is in Worker
      appsettings.json, including ControlPlane:LocalConnection. No separate JSON remains.
- AWS CLI is present; Session Manager plugin, psql and the local RDS CA bundle are
      unavailable. SSO URL, bastion and approved database login/role still need actual
      values. No profiles, hosts entries, credentials, or cloud resources were changed.
- Live Windows/.NET/EKS/Azure authentication, network access, grants, CA acceptance,
      and reconnection after token expiry are unverified. No ingestion was executed.