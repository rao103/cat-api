# CAT Harmonizer — Production V2/V3 and Outage lifecycle

Production V2/V3 are retained, and Outage consumes EventWorkV3, writes canonical monthly and WTG-day Parquet, and runs its durable event lifecycle inside the same worker. See [the replacement guide](../../../docs/Harmonizer/ReplaceFiles.md), [Outage behavior](../../../docs/Harmonizer/Outage.md), and [validation evidence](../../../docs/Harmonizer/Validation.md).

## Contract and runtime

- Outage accepts the latest supplied EventWorkV3 canonical members, snapshot receipt barriers and empty incremental observations. End times remain null for open events. No active known event means no outage; technical failures remain errors. Canonical events, daily links and cleanup are implemented.
- Snapshot barriers declare `ReceiptOnly` or `Authoritative`. Receipt-only waits for child receipts and never applies absence. Authoritative reconciliation requires an explicit source-version cutoff and commits a scoped fence; observation time and extraction tokens are not substitutes. The current producer must supply a verified cutoff before absence deletion is enabled.
- Curtailment belongs to the 10-minute WTG-day route. Alarm/Adjustment belong to the event route. Their actual source mappings are not registered yet.

- The consumer accepts both `ProductionItemDayWorkV2` (`SchemaVersion: 2`) and `ProductionItemDayWorkV3` (`SchemaVersion: 3`). V2 resolves names and declared scope IDs through the authoritative PostgreSQL catalog; V3 supplies explicit IDs/names and does not need a lookup. Both use the same Production processing rules.
- V2's string farm ID must be a positive decimal integer. Its original message body is retained unchanged for durable Fan-Out plan matching and replay. Names and scope IDs are separate sets, never paired by array position. Missing, ambiguous or mismatched identities fail validation.
- The first registered batch freezes its resolved numeric turbine membership. If a later V2 lookup changes that membership, replay fails validation before processing rather than targeting different turbines under the same batch identity.
- Additive `OperationId` and `ExecutionId` metadata is preserved from the work/original envelope through log scopes, trace attributes, row lineage, durable state and outgoing notifications. Existing V2/V3 messages without these fields remain untracked. See `docs/OperationCorrelation.md`.
- Numeric `WindfarmId` and `WtgId` remain numeric. Names remain `WindfarmName` and `WtgName`. SQL keys use `windfarm_id` and `wtgid`.
- The real refresh messages use `ProductionWtgDays`, `ItemTurbines`, and empty `ScopeTurbines`. Those messages are supported.
- Incremental: one exact immutable Raw part, one UTC date, up to 50 turbines. Per-record merge key is turbine/start interval. `LastUpdatedDate` orders rows; extraction tokens do not order individual rows.
- Refresh: validate the embedded complete ExtractionReady envelope. Read all explicitly declared parts in completed windows relevant to the requested business date. Unrelated months need not be reread. The work message may contain only one of those paths, as in the supplied real examples.
- `AllowRemovalReconciliation=false`: merge present rows, retain absent rows, preserve newer corrections. This is the behavior of the supplied V3 examples. Deletion/replacement refresh is explicitly unsupported until the source-as-of evidence contract is provided.
- Completeness requires 144 distinct aligned UTC slots. Sparse data stays Partial. No synthetic zero rows.
- Outage core: `OutageClassificationId` is the key; `OutageID` is descriptive. Open ends remain null, source timestamps order updates/deletions, tombstones prevent stale resurrection, and daily projections preserve unknown attributes. The registered Outage consumer and autonomous lifecycle apply these rules and write canonical/daily artifacts.
- Object storage remains Azure Blob. All control state is PostgreSQL, in the same database as the supplied Fan-Out ledger. No other database provider is used.

## Merge into your repository

1. Runtime projects are under `src/Harmonized/Harmonizer/Harmonizer.Core` and `Harmonizer.Worker`; tests are under `tests/Harmonized`. Project references point to the repository's shared contracts project.
2. `src/Shared/Messaging.Contracts` contains **unmodified copies** of the supplied contracts for this standalone build. Do not add duplicate types to your real shared project. The existing contract copies remain unchanged; add EventWorkV3 only if your shared project does not already contain the identical latest Fan-Out definition. These files retain their existing namespaces, including `ProductionIngestor.Contracts`; the Harmonizer does not reference an ingestor executable/project.
3. `src/db/migrations/0004_create_harmonizer_schema.sql` is the consolidated fresh-install baseline for Harmonizer tables, operation correlation, event lifecycle, receipts and readiness. Apply it once after the existing Raw Fan-Out prerequisites only where Harmonizer tables do not exist. Remove the superseded, unapplied 0005/0006 scripts from the migration input. Existing installations require forward migrations; do not apply this baseline over them. Worker startup never applies DDL.
4. `0003_create_raw_fanout_schema.reference.sql` is an unchanged test/reference copy. **Do not rerun it against existing Fan-Out tables.** No Fan-Out or ingestor source changes are included.
5. Keep the repository's existing package versions and lock files. Do not replace them with the standalone package versions.

## Run and configure

`src/Harmonized/Harmonizer/Harmonizer.Worker/appsettings.json` contains non-secret defaults and
logging/telemetry settings. It is copied to the worker build and publish outputs.
The included appsettings retains the non-secret development destinations supplied in this update.
Supply environment-specific URLs and the database connection through environment variables or
your existing secret/configuration provider; do not fill secrets into the checked-in file.

| Setting | Meaning |
|---|---|
| `Harmonizer__QueueUrl` | Existing Harmonizer SQS queue URL |
| `Harmonizer__BlobContainerUrl` | Raw Blob container URL; existing setting retained |
| `Harmonizer__OutputBlobContainerUrl` | Required Harmonized Blob container URL, configured independently of Raw |
| `Harmonizer__PostgresConnectionString` | Same PostgreSQL database as Raw Fan-Out |
| `Harmonizer__AwsRegion` | AWS region; default `eu-north-1` |
| `Harmonizer__UseRdsIamAuthentication` | Set `true` for IAM password renewal using the SDK credential chain |
| `Harmonizer__PublishNotifications` | Default `false`; durable outbox intents accumulate for downstream integration |
| `Harmonizer__NotificationQueueUrl` | Required only when notification publishing is enabled |
| `Telemetry__ServiceName` | OTLP service identity; default `cat-harmonizer` |
| `Telemetry__ServiceVersion` | Service version; default `1.0.0`, set to your deployed version |
| `Telemetry__TraceSampleRatio` | Root trace ratio between 0 and 1; default `1.0`; sampled parents are respected |
| `Telemetry__ExportEnabled` | Default `false`; set `true` to export logs, traces and metrics to OTLP |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Required when export is enabled; HTTP(S) collector URI without credentials, query or fragment |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | OTLP protocol supported by your collector, such as `http/protobuf` |
| `OTEL_EXPORTER_OTLP_HEADERS` | Collector authentication headers, supplied through a secret rather than source code |

The host always writes one JSON console log for each accepted log event with a UTC
`Timestamp`, structured properties and scopes. Its OpenTelemetry logging provider
also associates logs with the active trace and span. With `Telemetry__ExportEnabled=true`,
the same host-owned OpenTelemetry configuration exports logs, traces and metrics.
There is no collector connection while export is disabled. Application logs carry
operation, execution, message, batch, request, attempt, turbine and farm IDs in scopes, while metric labels
remain bounded. `Logging__LogLevel__Default` can override the checked-in log level.
Set `Logging__LogLevel__Harmonizer=Debug` temporarily to inspect provider I/O and
state transitions. Core adapters and repositories log operation identity and counts; storage I/O also includes `StorageRole=Raw` or `Harmonized`;
they do not log message bodies, credentials, connection strings, or full Parquet rows.

For RDS IAM, use `Ssl Mode=VerifyFull` and the trusted RDS CA chain; omit Password. Tokens refresh through Npgsql's periodic password provider. Configure the existing App Bundle/IRSA role and cross-account credential chain before running. The code creates no IAM roles. For another established database credential factory, replace the composition-root data-source registration; repositories receive `NpgsqlDataSource` through DI.

Blob uses `DefaultAzureCredential`. EKS IRSA does not by itself authenticate to Azure Blob: supply your existing approved Azure credential configuration. `Harmonizer__BlobConnectionString` (Raw), `Harmonizer__OutputBlobConnectionString` (Harmonized) and `Harmonizer__SqsServiceUrl` are optional development/emulator settings. The output connection string does not fall back to the Raw connection string. Each store uses `DefaultAzureCredential` when its own connection string is absent. Never commit production secrets.

```sh
dotnet run --project src/Harmonized/Harmonizer/Harmonizer.Worker/Harmonizer.Worker.csproj
```

The host long-polls SQS and runs registered source/lifecycle strategies with bounded concurrency and ownership heartbeats. Production retains its existing completed-batch acknowledgment flow. Outage deletes a delivery after durable canonical/plan handoff; its lifecycle loop owns remaining materialization and final Fan-Out acknowledgment. A scope barrier waits for its expected canonical receipts before handoff. Configure the existing redrive policy/DLQ externally; unverified source/version work remains retryable.

## Persistence and recovery

- Each `(windfarm_id, wtgid, data_date, input_type)` has one leased current state. A monotonically increasing token fences both renewal and commit.
- Candidate output: `harmonized/wtg/windfarm=NAME/wtg=NAME/date=YYYY-MM-DD/production/revision=N/attempts/token=T/` with `production-10minute.parquet`, `snapshot.json` and `lineage.json`. Numeric IDs remain in records and PostgreSQL keys; path names are encoded as single segments.
- Each candidate contains Parquet, a complete JSON snapshot with row lineage/version evidence, and a lineage manifest. Only the PostgreSQL pointer selects a committed attempt. Partial uploads create unreferenced objects, never valid pointers.
- One transaction commits the pointer, item receipt and outbox intents. The final batch member calls `control.raw_artifact_ack` in that same transaction. Failure there rolls all of them back.
- Partial input can be durably materialized/acknowledged while remaining ineligible for calculation. Fan-Out materialization does not mean five-input calculation readiness.
- A failed Production contribution pins its original artifact/batch. Later contributions cannot clear that blocker. Redrive the original exact batch body. Healthy members become receipt-based no-ops on retry.
- Durable Harmonizer receipts survive Fan-Out ledger cleanup. Do not delete pending batches/receipts or immutable lineage. Automated Harmonizer retention/orphan collection is not included yet.
- SQS notification delivery is at least once. A send followed by a crash can repeat. Consumers must deduplicate `NotificationId` and recheck current component generations/status. Package Coordinator must check `control.harmonizer_event_day_pending` even if there is no Outage row and consult `control.harmonizer_input_readiness` for existing rows. A known absent Outage result is empty when no accepted work/cleanup is pending; do not introduce a five-physical-row completeness requirement for that rule.

`docs/Harmonizer/Operations.sql` contains read-only failure, replay-body and outbox inspection queries. The downstream `PackageCoordinationRequested` and `ProductionSummaryInvalidated` payloads are proposed integration contracts; the other workers were deliberately not built or changed. Enable publishing only after aligning those consumers.

## Event lifecycle configuration

`Harmonizer:EventLifecycle` shares this same appsettings.json and telemetry resource.
Enabled defaults to true; PollSeconds=30, FarmsPerPoll=20, ConcurrentFarms=2,
DaysPerPass=31, MaximumPartitions=1200, ProcessableDayOffset=-1 and RetrySeconds=30.
Nested options are validated on startup. Disabling polling pauses durable pending work
and future continuation; re-enable it to resume. Keep at least one running replica,
including KEDA minReplicaCount=1, because SQS can be idle while events need another day.
Different farms run in parallel; each Outage farm/source is serialized by a fenced lease.
Review pool/memory budgets across inbound work, lifecycle work, source count and replicas.

Harmonizer does not write `packages` or dispatch CalculationWork. Package Coordinator
owns coherent package publication and calculation dispatch. The notification queue
is deliberately unset and PublishNotifications=false until its actual consumer contract
is aligned; accepted changes still write durable PostgreSQL outbox intents.

## Separate storage update

Raw reads use `IRawObjectStore`. Snapshot reads and candidate uploads use
`IHarmonizedArtifactStore`. Separate singleton Blob clients are registered for their
respective container URLs and credentials. The old `IObjectStore` file/registration is
removed. No database migration is introduced by this storage change.

See `docs/Harmonizer/StorageIntegration.md` for the exact replacement list, required configuration,
telemetry and existing-output migration guidance. Existing outputs in Raw must be copied
to the configured Harmonized container with their relative paths preserved before switching
an already-deployed worker. Previously committed paths are read as stored; new revisions use
the restored WTG-day folder layout.

## Verify

```sh
dotnet restore tests/Harmonized/Harmonizer.UnitTests/Harmonizer.UnitTests.csproj --locked-mode
dotnet restore tests/Harmonized/Harmonizer.IntegrationTests/Harmonizer.IntegrationTests.csproj --locked-mode
dotnet build src/Harmonized/Harmonizer/Harmonizer.Worker/Harmonizer.Worker.csproj -c Release --no-restore
dotnet test tests/Harmonized/Harmonizer.UnitTests/Harmonizer.UnitTests.csproj -c Release --no-restore
dotnet test tests/Harmonized/Harmonizer.IntegrationTests/Harmonizer.IntegrationTests.csproj -c Release --no-restore
```

`tests/Harmonized/verify.sh` and `verify.ps1` perform these checks. PostgreSQL tests require `HARMONIZER_TEST_POSTGRES`; otherwise they report skipped tests. Use only a disposable local test server. Do not give these tests an application database connection.

Optionally run `docker compose -f tests/Harmonized/compose.integration.yml up -d` and set the test connection to `Host=127.0.0.1;Port=54329;Username=harmonizer_test;Password=harmonizer_test;Database=postgres;SSL Mode=Disable`. The Compose credentials are for that local test container only.

Kubernetes/Docker examples under `deploy/harmonizer` need your image registry, App Bundle service account, secret references and resource sizing. They are templates; no deployment was performed.
