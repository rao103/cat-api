# CAT Raw Fan-out Coordinator — artifact-completion revision

This package adds a .NET 10 Core/Worker service and tests. Core and Worker builds and the unit suite have been verified locally. The external PostgreSQL/Blob/SQS integration tests require Docker and have not been run here. No live RDS, SQS, or Blob resources were accessed or changed.

## Architecture

The service has two projects. `RawFanOut.Worker` is the host: it binds and validates startup configuration, selects continuous queue consumption or a one-shot execution mode, owns hosted-service shutdown, registers dependencies, selects host credentials, and configures telemetry exporters. `RawFanOut.Core` owns fan-out use cases and source rules, internal models and capability interfaces, serialization/parsing, provider adapters, PostgreSQL repositories and connection/authentication implementations, retention, and processing telemetry.

The only production dependency direction is `Worker -> Core`. Core does not reference Worker; both projects use the existing `Messaging.Contracts` project for shared wire contracts. The Worker composes Core implementations but does not contain processing or persistence rules.

Place active queue consumers and queue bindings in Worker `Hosting/`; composition methods in `DependencyInjection/`; host startup/concurrency settings and validation in `Configuration/`; and exporter setup in `Observability/`. There is no separate Worker authentication implementation to relocate, so no empty `Authentication/` folder is created. Keep credential selection in the host composition; Core's RDS IAM password provider remains under Core `Authentication/`.

In Core, keep use-case orchestration in `Operations/` grouped by fan-out, delivery, dead letters, and retention. Place application-owned contracts in individually named `Interfaces/` files and their request/result records in `Models/`. Keep Parquet, SQS, and Blob SDK code under the corresponding `Adapters/` subfolder; SQL stays in `Repositories/`; connection construction stays in `Factories/`; envelope/work JSON code stays in `Serialization/`; processing logs, traces, and metrics stay in `Observability/`. Create folders only when they contain owned code.

An end-to-end raw-message flow is: Worker configuration is validated and the host registers Core services; `QueueWorker` receives a message and `DeliveryObserver` establishes processing telemetry; `DeliveryHandler` bounds processing and renews visibility; `RawFanOutProcessor` parses the existing envelope; `FanOutPlanner` reads Parquet identities, loads the farm catalog, and applies the source strategy to build stable batches; the dispatch repository freezes or reloads the plan; `ReliableBatchPublisher` publishes it; then the processor marks dispatch complete before the delivery handler deletes the input receipt. Failures remain eligible for retry or dead-letter handling. Retention is a Core use case invoked as a bounded one-shot Worker mode, with scheduling owned by the deployment manifest.

## What is implemented

- Your four supplied source schemas plus the v11 Adjustment parent identity contract.
- Artifact-scoped fan-out completion, immutable child plans and separate downstream acknowledgement SQL. No additional ingestor message.
- PascalCase incoming messages, Production-only required DataDate, mutually exclusive BlobPath/ShardPaths, preserved run/trace/window metadata.
- Complete-shard validation, minimal Parquet identity reads, stable sorted batches and bounded work.
- Continuous SQS consumption, long polling, visibility renewal, cancellation, partial batch-send validation/retry, delete only after successful publication.
- `control.raw_fan_out_dead_letter` is the single recovery archive for DLQ messages (original body kept verbatim, even invalid JSON).
- Incident replay (`--replay <id>`) republishes and records `replay_requested_at_utc` only; an incident is resolved solely by an explicit operator closure (`--resolve <id> --reason <text> --operator <name>`).
- Unit tests, real-Parquet integration tests, opt-in PostgreSQL and Blob/SQS emulator integration tests.
- Kustomize development deployment template and an approved-image Dockerfile.

## Outage event work

Outage accepts the existing `RawOutageBatchAvailableV1` incremental feeds (`OutageEvents` and `OutageDeletedEvents`) and the V2 refresh envelope. Refresh accepts the producer's `ManifestPath` plus `WindowStartUtc`/`WindowEndUtc`; the manifest is validated against the current schema (`SchemaVersion=1`, `Parts`, `RecordCount`, and `SnapshotAsOfUtc`). Direct path snapshots are also supported when they declare scope and row count. Incremental messages require at least one Raw path. `OutageSnapshot` rows are checked against the producer overlap rule and declared row count, and event/turbine names are resolved through the existing farm catalog.

New Outage work is serialized as `EventWorkV3`. Canonical `EventKey` excludes WTG identity; each event/WTG member has a stable `MemberKey` and only the exact Raw paths containing that member. Snapshot plans include a frozen numeric WTG scope and a barrier listing all ordinary event batch IDs, including for an empty snapshot. The current Outage producer's `SnapshotAsOfUtc` is host observation time, not a source-version cutoff, so its snapshots are `ReceiptOnly` and cannot authorize absence deletion. `Authoritative` requires an explicit complete WTG scope and a verified `SourceVersionCutoffUtc` in addition to the normal completeness checks.

Keep the new Outage output disabled until the Harmonizer consumes EventWorkV3, records ordinary batch receipts independently, and implements the receipt barrier. Authoritative absence reconciliation additionally requires its durable scoped version fence and concurrency protection; that work is outside this Fan-Out merge. Existing persisted plans retain their stored V1/V2 bodies and replay behavior.

**Read `docs/ARCHITECTURE.md` before connecting a Harmonizer.** Events are dispatched as canonical-event work following v11 section 6.11; the future Event Lifecycle Coordinator derives corrected/open/tombstone Item-Days after canonical commit. Production and full-snapshot reconciliation messages handle missing identities and empty artifacts. These are new outgoing contract work kinds, not existing consumer behavior. Adjustment reads WindfarmId/AdjustmentId and preserves full parent/membership artifacts; align that explicit contract with your future exporter. The existing artifact message is the producer completion boundary. The future Harmonizer acknowledges its child batches; fan-out completion alone never means calculation-ready. See CHANGELOG.md when replacing the earlier ZIP.

## Merge into your existing repository

1. Copy `src/Raw/FanOut/`, `tests/Raw/RawFanOut.UnitTests/`, `tests/Raw/RawFanOut.IntegrationTests/`, and `deploy/raw-fanout/`.
2. Merge the supplied contract/helper `.cs` files into your real `src/Shared/Messaging.Contracts/`. Preserve the existing project and incoming publisher classes. Adjust the supplied `CAT.*` namespaces to your repository conventions if necessary, including test usings. The consumer-local RawEnvelope does not remove DataDate from your Production contract.
3. Update Core's ProjectReference if your shared project filename differs from `Messaging.Contracts.csproj`.
4. Use your existing root target framework, analyzers, SDK and approved package management. The supplied `Directory.Build.props`, `Directory.Packages.props`, shared-project shim and `RawFanOut.slnx` exist only to make this package independently buildable; **do not overwrite your repository's versions of those files**. The four requested pins are AWSSDK.SQS 4.0.100.13, Azure.Storage.Blobs 12.29.2, Npgsql 10.0.3 and Parquet.Net 6.1.0. AWSSDK.RDS is moved to 4.0.104.1 to avoid mixing AWS SDK major versions; confirm this additional pin exists in your approved feed, or retain your approved shared database credential factory. Other helper/test package pins are listed in Directory.Packages.props. They have not been restored against your private JFrog feed. No NuGet source override is included.
5. Add the four projects to `O.WP.CAT.LossCalculation.Platform.sln` with the commands below.
6. Deploy the schema to a fresh database by running `src/db/migrations/0001`, `0002`, `0003` in order (the FanOut tables are in `0003_create_raw_fanout_schema.sql`), then `src/db/PostDeployment/`. The Worker never runs DDL. See docs/raw-fan-out/SCHEMA.md for ownership, grants and boundaries.
7. Merge the relevant supported behavior, configuration and commands from this README into your root README.

```powershell
dotnet sln O.WP.CAT.LossCalculation.Platform.sln add src/Raw/FanOut/RawFanOut.Core/RawFanOut.Core.csproj
dotnet sln O.WP.CAT.LossCalculation.Platform.sln add src/Raw/FanOut/RawFanOut.Worker/RawFanOut.Worker.csproj
dotnet sln O.WP.CAT.LossCalculation.Platform.sln add tests/Raw/RawFanOut.UnitTests/RawFanOut.UnitTests.csproj
dotnet sln O.WP.CAT.LossCalculation.Platform.sln add tests/Raw/RawFanOut.IntegrationTests/RawFanOut.IntegrationTests.csproj
```

## Validate locally

From this standalone package root (or use your real solution after merging):

```powershell
dotnet restore RawFanOut.slnx
dotnet build RawFanOut.slnx -c Release
dotnet test tests/Raw/RawFanOut.UnitTests/RawFanOut.UnitTests.csproj -c Release --logger "trx;LogFileName=raw-fanout-unit.trx"
dotnet test tests/Raw/RawFanOut.IntegrationTests/RawFanOut.IntegrationTests.csproj -c Release --logger "trx;LogFileName=raw-fanout-integration.trx"
```

Real-Parquet tests run without external services. External tests are explicitly **skipped** unless enabled; skipped is not passed. To run the emulator/database tests on a workstation approved for Docker:

```powershell
docker compose -f compose.integration.yml up -d --wait
$env:RAWFANOUT_INTEGRATION = "1"
dotnet test tests/Raw/RawFanOut.IntegrationTests/RawFanOut.IntegrationTests.csproj -c Release --logger "trx;LogFileName=raw-fanout-external.trx"
Remove-Item Env:RAWFANOUT_INTEGRATION
docker compose -f compose.integration.yml down
```

The PostgreSQL tests only accept `localhost`/`127.0.0.1` with the admin database `rawfanout_test`; each test creates and drops its own throwaway `rawfanout_it_*` database and applies the three schema scripts (`MigrationDatabase`). If local port 55432 is unavailable or changes, set `RAWFANOUT_TEST_POSTGRES` to the isolated test connection string. External Blob/SQS tests use fixed local Azurite/LocalStack ports. Mirror the example pinned emulator images into JFrog if required. No external test proves live RDS TLS, cross-account IAM, Azure federation, EKS admission or production SQS redrive behavior.

After unit/integration tests pass, build your full solution because the outgoing shared contract is new. Exercise one sanitized ingestor-generated Parquet fixture per source; the supplied Parquet fixtures test the consumed identity columns, not all producer encodings and optional fields.

## Runtime configuration

Environment variables use the prefix `RawFanOut__`; the base JSON settings are under `RawFanOut`. `appsettings.json` targets the shared CAT development SQS queues, the Production raw Blob container, and the `cat_dev` PostgreSQL control plane. FanOut does not connect to the data warehouse.

`ControlPlane` uses typed settings and RDS IAM authentication. Production/EKS uses the real endpoint and port 5432 directly. `appsettings.Development.json` overrides the local TCP port to 55432, selects the local `superuser` AWS profile for database IAM-token signing, and points to the Windows CA bundle. The real RDS hostname remains `Host` and `SigningHost`; the real port remains `SigningPort`.

| Setting | Development value/purpose |
|---|---|
| `ControlPlane:Provider` / `Authentication` | `PostgreSql` / `AwsIam` |
| `ControlPlane:Host` / `SigningHost` | Actual RDS hostname, identical for TLS verification and token signing |
| `ControlPlane:Port` | `55432` for the local tunnel; `5432` in EKS |
| `ControlPlane:SigningPort` | Actual RDS port, `5432`, even when the local tunnel uses another port |
| `ControlPlane:Database` / `Username` | `cat_dev` / approved IAM-enabled database login |
| `ControlPlane:AwsProfile` | Local DB-token profile only; empty in EKS |
| `ControlPlane:RootCertificate` | Trusted RDS CA bundle path |
| `RawFanOut:RawQueueUrl` / `RawDlqUrl` | `cat-raw-dev` and `cat-raw-dev-dlq` |
| `RawFanOut:HarmonizationQueueUrl` | `cat-harmonize-dev` |
| `RawFanOut:RawContainerUri` | Approved raw Blob container URI, without SAS |

For Windows F5 debugging, follow [the repository's CAT database connection guide](../../../localdbconnection/README.md): install the trusted regional RDS CA, configure and sign in to the approved AWS profile, add the real RDS hostname to the Windows hosts file as documented, and start the existing SSM tunnel. Run `./localdbconnection/dbconnect.ps1 debug-env` after the tunnel is ready; it sets the local ControlPlane environment variables from the Production worker's shared CAT connection settings. Start a new VS Code window from that same PowerShell process so it inherits them. The tunnel listens locally on 55432 while Npgsql still connects with the real hostname, `SSL Mode=VerifyFull`, and the trusted CA. `ControlPlane:AwsProfile` is used only to generate the database IAM token; SQS uses the AWS SDK default credential chain and needs its own queue permissions. If the `cat-dev` SSO identity has those permissions, set `$env:AWS_PROFILE='cat-dev'` before launching the new VS Code window.

In VS Code, select `RawFanOut.Worker` and the `RawFanOut.Worker` launch profile, then press F5 after the tunnel and credentials are ready. The Worker immediately polls the configured **real development SQS queue** and reads from the configured **real development Blob container**. Do not start it unless consuming a `cat-raw-dev` message is intended. A running worker may delete a message after successful publication. The new V3 ExtractionReady output also requires downstream consumer support before deployment.

IAM token rotation is implemented with Npgsql's periodic password provider at ten-minute intervals. EKS uses the AWS workload identity/default credential chain with no developer profile or static credentials in appsettings. The factory does not create cross-account trust or silently assume a role.

Azure uses DefaultAzureCredential. Supply your existing approved EKS-to-Entra federation settings and blob read authorization. AWS SQS uses its SDK default credential chain. Neither AWS nor Azure credentials belong in appsettings.json.

## Deployment and permissions

The Kustomize overlay is a template with explicit REPLACE placeholders. Before deployment provide the existing `raw-fanout` service account, `raw-fanout-secrets`, CA ConfigMap and actual queue/container/image values. Do not launch it against a queue until its outgoing work kinds are supported by the Harmonizer. No Service/HTTP API is needed.

Required capabilities: receive/delete/change visibility on Raw and Raw DLQ; send on Harmonization; Raw Blob read; RDS connect and DML on the two failure tables and two artifact-tracking tables, execution of the artifact functions, plus sequence usage for FanOutFailure. Use your existing grants/roles; no blanket account permissions or DDL permissions are needed at runtime. Add KMS permissions if queues require them. The input queue's redrive policy must target the Raw DLQ with an appropriate maxReceiveCount; this application neither modifies that policy nor manually sends poison records to the DLQ.

The template reserves writable `/tmp` and read-only CA trust material. Concurrency multiplies scratch/row-group memory use; tune the configured limits and pod resources together. The template's process lifecycle is not a health/readiness guarantee; add your team's observability checks for queue age, fan-out errors, DLQ persistence and unresolved incidents. No synthetic HTTP health endpoint is supplied.

## Replay an incident

Inspect unresolved incidents through your existing operations tooling:

```sql
SELECT incident_id, message_id, first_observed_at_utc, artifact_paths
FROM control.raw_fan_out_dead_letter
WHERE resolved_at_utc IS NULL
ORDER BY first_observed_at_utc;
```

Run one controlled replay with the same runtime settings:

```powershell
dotnet run --project src/Raw/FanOut/RawFanOut.Worker -- --replay <incident-uuid>
```

The command resolves only after every outgoing batch has been confirmed. Failure leaves the incident open. This resolves **fan-out**, not downstream calculation completeness. A malformed message is preserved for investigation and is not automatically edited into a guessed valid message.

## Technical references

- [SQS SendMessageBatch: partial outcomes, entry and byte limits](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html)
- [Npgsql security and dynamic password providers](https://www.npgsql.org/doc/security.html)
- [RDS IAM authentication with the .NET SDK](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.NET.html)
- [Parquet.Net reference package](https://www.nuget.org/packages/Parquet.Net/6.1.0)

`docs/VALIDATION.md` records what was actually checked during authoring.
