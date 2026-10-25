# Harmonizer replacement guide — 6 October 2026

This guide compares the delivery with the previous saved Harmonizer project (archive version 5). All paths below are relative to its Harmonizer folder. If Copilot has made additional changes in your real repository, merge the listed files against those changes rather than overwriting blindly.

The delivery contains 34 new files and 23 replacement files, including documentation, fixtures and test reports. Runtime source/configuration accounts for 26 additions and 8 replacements. The superseded unapplied 0005/0006 SQL files are removed from this delivery. No existing shared message contract or package lock is changed; EventWorkV3 is an additive, byte-for-byte copy of the latest supplied Raw Fan-Out contract.

## Changes since the immediately preceding delivery

For this consolidation update, replace database/0004_create_harmonizer_schema.sql and remove unapplied 0005/0006 scripts. The integration fixture now loads the single baseline, and README/design/validation documents have been updated. Runtime C# behavior and appsettings.json are unchanged from that delivery. The full file list below still compares against archive version 5, before Outage implementation.

## Merge sequence

1. Add the files listed under Add and replace/merge those under Replace. Keep the original SourceTypes organization. Production V2/V3 processing remains present; the generic worker dispatches to Production and Outage strategies.
2. Replace the old 0004 with the consolidated database/0004_create_harmonizer_schema.sql. Remove superseded unapplied 0005_harmonizer_operation_correlation.sql and 0006_harmonizer_event_lifecycle.sql from your migration input. Apply the new 0004 once after existing Raw Fan-Out prerequisites, before starting Harmonizer. It creates all six tables plus correlation, event receipts, indexes, audit triggers and readiness helpers in one transaction. Use it only where Harmonizer tables have not yet been created. Deployed databases require forward migrations. Do not rerun the bundled 0003 Raw Fan-Out reference schema in your real database.
3. Merge appsettings.json values with your deployment environment: inbound cat-harmonizer-dev, distinct Raw/Harmonized stores, existing PostgreSQL/credentials, EventLifecycle limits and shared Telemetry/Logging. Keep at least one replica running for idle-queue continuation. The default processable date is yesterday UTC.
4. If your real shared contracts already contain exactly this EventWorkV3, use that definition and avoid a duplicate class. Deploy the Harmonizer consumer before routing current EventWorkV3 Outage work. Frozen older Outage V2 batches need an explicit compatibility/redrive choice.
5. In Package Coordinator, call control.harmonizer_event_day_pending(farm,wtg,date,source) even when no Outage row exists; the readiness view handles existing rows. Accepted pending work must block calculation. False plus no known event is empty Outage. Harmonizer does not write packages or direct CalculationWork. Publishing remains disabled until the proposed notification contract matches your real coordinator.
6. Run locked restore/build/unit tests and the Npgsql suite against a disposable PostgreSQL instance before deployment. Never run the schema-creating test fixture against your real database. Confirm Outage incremental and refresh share a monotonic extraction cursor token namespace, then validate permissions, load and HPA in your environment.

This update also consolidates the previously unapplied schema chain into one 0004 baseline. The main runtime design change is farm/source serialized Outage publication with resumable bounded day work. Two control tables store current pointers/ownership/progress; canonical business history remains in immutable Parquet/snapshot/lineage files. SQS deletion means durable canonical/plan acceptance; final raw_artifact_ack waits for materialization. Existing Production receipt behavior is retained.

Telemetry uses the existing ITelemetry abstraction and host-owned UTC JSON/OTel configuration. Necessary new readers, I/O adapters/repositories, orchestrators, hosted loops and heartbeats are instrumented. Pure mapping/interval/merge/range helpers need no logger.

## Add

- `docs/ReplaceFiles.md`
- `src/Harmonized/Harmonizer.Core/Configuration/EventLifecycleOptions.cs`
- `src/Harmonized/Harmonizer.Core/Interfaces/IEventLifecycleHandler.cs`
- `src/Harmonized/Harmonizer.Core/Interfaces/IHarmonizationHandler.cs`
- `src/Harmonized/Harmonizer.Core/Operations/HarmonizationDispatcher.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Adapters/NormalizedOutageRowMapper.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Adapters/OutageParquetRow.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Adapters/ParquetOutageArtifacts.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Interfaces/IOutageArtifacts.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Interfaces/IOutageControlRepository.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Interfaces/IOutageIdentityCatalog.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Models/OutageArtifacts.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageBatchProcessor.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageCanonicalReader.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageHarmonizationHandler.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageLifecycleHandler.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageLifecycleProcessor.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageRangePlanner.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageScopeProjector.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Repositories/PostgresOutageControlRepository.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Repositories/PostgresOutageIdentityCatalog.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Serialization/OutageWorkKeys.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Serialization/OutageWorkReader.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Production/Operations/ProductionHarmonizationHandler.cs`
- `src/Harmonized/Harmonizer.Worker/Hosting/EventLifecycleWorker.cs`
- `src/Harmonized/Harmonizer.Worker/SourceTypes/Outage/OutageRegistration.cs`
- `src/Shared/Messaging.Contracts/EventWorkV3.cs`
- `tests/Harmonized/Harmonizer.IntegrationTests/OutagePostgresTests.cs`
- `tests/Harmonized/Harmonizer.UnitTests/Fixtures/outage-deleted-events.parquet`
- `tests/Harmonized/Harmonizer.UnitTests/Fixtures/outage-events.parquet`
- `tests/Harmonized/Harmonizer.UnitTests/HarmonizerRegistrationTests.cs`
- `tests/Harmonized/Harmonizer.UnitTests/OutageContractTests.cs`
- `tests/Harmonized/Harmonizer.UnitTests/OutagePipelineFixture.cs`
- `tests/Harmonized/Harmonizer.UnitTests/OutagePipelineTests.cs`

## Replace

- `README.md`
- `database/0004_create_harmonizer_schema.sql`
- `deploy/harmonizer/deployment.example.yaml`
- `docs/Architecture.md`
- `docs/ImplementationStatus.md`
- `docs/OperationCorrelation.md`
- `docs/Operations.sql`
- `docs/Outage.md`
- `docs/StorageIntegration.md`
- `docs/Validation.md`
- `docs/validation/Harmonizer.IntegrationTests.trx`
- `docs/validation/Harmonizer.UnitTests.trx`
- `src/Harmonized/Harmonizer.Core/Configuration/HarmonizerOptions.cs`
- `src/Harmonized/Harmonizer.Core/Observability/HarmonizerDiagnostics.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Models/OutageState.cs`
- `src/Harmonized/Harmonizer.Core/SourceTypes/Outage/Operations/OutageRecordValidator.cs`
- `src/Harmonized/Harmonizer.Worker/DependencyInjection/HarmonizerRegistration.cs`
- `src/Harmonized/Harmonizer.Worker/Hosting/HarmonizerWorker.cs`
- `src/Harmonized/Harmonizer.Worker/SourceTypes/Production/ProductionRegistration.cs`
- `src/Harmonized/Harmonizer.Worker/appsettings.json`
- `tests/Harmonized/Harmonizer.IntegrationTests/PostgresTests.cs`
- `tests/Harmonized/Harmonizer.UnitTests/Harmonizer.UnitTests.csproj`
- `verify.sh`

## Remove superseded unapplied scripts

- `database/0005_harmonizer_operation_correlation.sql`
- `database/0006_harmonizer_event_lifecycle.sql` (if copied from the immediately preceding delivery)

## Retained without modification

Existing shared contracts, provider ports/clients, Production merge/storage/repositories/readers, pure Outage domain/rules except the two listed files, the 0003 reference schema, project package dependencies/locks and remaining build files are retained. IObjectStore.cs is not reintroduced: IRawObjectStore and IHarmonizedArtifactStore remain separate.

Alarm/Adjustment adapters, ten-minute Curtailment mapping, Package Coordinator, Calculator, Serving and ingestors are outside this delivery. These source work kinds are not silently acknowledged as empty. Production destructive refresh retains its previous limitation.

## Validation evidence

The current monorepo Release run passed all 155 unit tests; all 18 PostgreSQL/Npgsql integration tests were skipped because no disposable server was configured. The Release Worker build passed with one inherited warning in the unchanged RawArtifactAvailableV1 contract. The earlier standalone delivery's embedded PostgreSQL smoke report was not rerun in this checkout.

See Validation.md and the current TRX reports for the evidence and its limits. See Outage.md for paths, exact WTG-day event references, pending readiness, snapshot scope and HPA behavior. No database was modified and no deployment was performed.
