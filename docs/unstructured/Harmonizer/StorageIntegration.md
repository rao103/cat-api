# Apply the separate-storage and output-layout update

Use the files from this updated project, not a mixture of the uploaded pre-correlation
Parquet store and the earlier OperationId implementation. The uploaded Parquet class omitted
operation/execution metadata; the merged version preserves it. Shared Messaging.Contracts,
Production V2/V3 wire behavior, merge/fencing/receipt/outbox logic and SQL migrations are unchanged
by this update. There are no package version or project-reference changes. The replacement
list assumes the earlier OperationId/ExecutionId checkpoint has already been merged. If it
has not, use the complete updated Core/Worker projects so their models, registration and
repository constructors remain consistent.

## Runtime files to replace

Paths below are relative to the Harmonizer project root in the ZIP. Map them to your real
repository's corresponding project paths if its root differs.

| Action | File |
|---|---|
| Replace | `src/Harmonized/Harmonizer.Core/Adapters/BlobObjectStore.cs` |
| Replace | `src/Harmonized/Harmonizer.Core/Configuration/HarmonizerOptions.cs` |
| Replace | `src/Harmonized/Harmonizer.Core/SourceTypes/Production/Adapters/ParquetProductionArtifactStore.cs` |
| Replace | `src/Harmonized/Harmonizer.Worker/DependencyInjection/HarmonizerRegistration.cs` |
| Merge development destinations / add output settings | `src/Harmonized/Harmonizer.Worker/appsettings.json` |
| Add, or replace your Copilot copy | `src/Harmonized/Harmonizer.Core/Interfaces/IRawObjectStore.cs` |
| Add, or replace your Copilot copy | `src/Harmonized/Harmonizer.Core/Interfaces/IHarmonizedArtifactStore.cs` |
| Add | `src/Harmonized/Harmonizer.Core/Serialization/HarmonizedArtifactPathBuilder.cs` |
| Delete, if still present | `src/Harmonized/Harmonizer.Core/Interfaces/IObjectStore.cs` |

The supplied `IRawObjectStore(1).cs` is normalized to `IRawObjectStore.cs`; keep only one
interface definition. IHarmonizedArtifactStore explicitly declares its read/write methods
and no longer inherits the Raw role. The common BlobObjectStore implements both ports, but
DI creates a different instance/client for each store. There is no unqualified global
BlobContainerClient registration that can accidentally bind both roles to one account.

## Tests to replace/add

| Action | File |
|---|---|
| Replace | `tests/Harmonized/Harmonizer.UnitTests/ParquetTests.cs` |
| Replace | `tests/Harmonized/Harmonizer.UnitTests/OperationCorrelationTests.cs` |
| Replace | `tests/Harmonized/Harmonizer.UnitTests/TelemetryTests.cs` |
| Replace | `tests/Harmonized/Harmonizer.IntegrationTests/ProductionPipelineTests.cs` |
| Add | `tests/Harmonized/Harmonizer.UnitTests/StorageSeparationTests.cs` |

Also update README.md and docs/Architecture.md, ImplementationStatus.md and Validation.md.
Current TRX results are in docs/validation. Neither new interfaces nor the pure path builder
requires its own logger; all affected operational classes retain injected ITelemetry<T>.

## Configuration and credentials

| Setting | Role |
|---|---|
| `Harmonizer__BlobContainerUrl` | Raw container URL; retained existing setting name |
| `Harmonizer__OutputBlobContainerUrl` | Required Harmonized container URL |
| `Harmonizer__BlobConnectionString` | Optional Raw-only connection string |
| `Harmonizer__OutputBlobConnectionString` | Optional Harmonized-only connection string |

The included appsettings preserves the provided development Raw and silver URLs, queue and
non-secret database configuration. Merge these settings into your environment-specific
configuration rather than replacing unrelated values. Do not commit connection-string secrets.
When a store's connection string is absent, that store uses DefaultAzureCredential. The
Harmonized connection string never falls back to the Raw connection string. Provision access
for each account/container using your established credential setup; this code does not create
accounts or permissions. Startup validates the required output URL before processing work.

## Output hierarchy

New candidates use the confirmed WTG-day grouping inside Harmonized storage:

```text
harmonized/wtg/windfarm=WOW02/wtg=WTG11/date=2026-09-01/
  production/revision=5/attempts/token=12/
    production-10minute.parquet
    snapshot.json
    lineage.json
```

Names stay readable for ordinary RDSPP identifiers; reserved characters are encoded as part
of a single partition segment. Numeric WindfarmId/WtgId remain in data, manifests and database
keys. The path builder checks that the supplied identities match the leased component.
The source-specific Parquet store chooses its filename; the shared helper owns the hierarchy.
Outage storage/lifecycle is implemented in the 6 October update; see Outage.md and ReplaceFiles.md. Other source mappings and the packages/manifest.json coordinator remain separate integration work.

## Existing output and rollout

There is no new database schema migration for storage separation or naming. The consolidated
0004 fresh-install baseline includes operation correlation and event lifecycle state. Existing
deployments require forward migrations; do not apply the baseline over an installed schema.

If existing committed outputs were written into Raw storage, pause the worker and copy those
Harmonized output blobs to the new Harmonized container using exactly the same relative paths.
Include Parquet, snapshot and lineage files reachable from committed/current and retained
historical manifests; verify copies before switching. Raw source artifacts stay in Raw.
An unchanged database pointer remains valid only when its objects exist in the configured
output container. This project does not copy production data automatically or fall back to
Raw when a committed output is missing.

Existing committed relative paths, including the earlier source-first layout, are read as
stored. Do not rename them or rewrite historical manifests. Later changed revisions use the
restored layout; healthy no-ops reuse the copied prior artifacts. PreviousManifestPath links
remain valid because old relative keys were preserved. The new filename therefore needs no
rewrite of existing database pointers.

## Telemetry and verification

Blob read/write and Parquet processing scopes include StorageRole=Raw/Harmonized. They inherit
OperationId/ExecutionId and the existing batch/farm/WTG scopes. UTC JSON timestamps, exception
logging, application traces and optional OTLP exports are unchanged. Storage-role values are
bounded; credentials and payload rows are not logged.

Locked restore, Release build, unit tests and publish are verified in Validation.md. PostgreSQL
integration tests are compiled but skipped in this environment; run them on a disposable server
before deployment. Live Raw/silver account access and deployment were not performed.
