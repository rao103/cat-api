# CAT Loss Calculation Platform

## Build and API Documentation

All projects target .NET 10. Install the .NET 10 SDK to build and test:

```powershell
dotnet build O.WP.CAT.LossCalculation.Platform.sln
dotnet test O.WP.CAT.LossCalculation.Platform.sln --no-build
```

The Production and Outage APIs use ASP.NET Core's built-in OpenAPI generation.
Each running API exposes its versioned JSON document at `/openapi/v1.json`.
Swagger UI and the old `/swagger/...` routes have been removed; no interactive
documentation UI is bundled. Documents retain version grouping and Azure AD
OAuth2 metadata when authentication is enabled. As before, documentation is
available in all environments; apply deployment-level access restrictions as needed.

API deployments require the ASP.NET Core 10 runtime; Worker deployments require
the .NET 10 runtime. Deployment pipelines and container definitions are not tracked
in this repository and must provide those runtimes separately.

## Production Ingestor

Production ingestion uses a reusable Core library, a one-shot Worker, and an API
that starts refresh Jobs. Both hosts reference Core; Core does not reference either
host. Interface and model namespaces match their `Interfaces` and `Models` folders.

### Running the Worker

Configure the `Source` section using the deployment's existing secret/configuration
mechanism. Required keys are `SourceSystemName`,
`BlobConnectionString`,
and `BlobContainerName`. SQS settings are separate (see Raw Fan-out SQS below).
Never commit credentials. The shared control-plane schema and administrator-managed
Production assignments must exist before starting workers. Workers initialize missing
cursors only from each assignment's explicit finite UTC checkpoint.

PostgreSQL uses the Worker's `ControlPlane` appsettings section, configured for AWS
IAM against `cat_dev`. Supply the approved `ControlPlane__Username` and trusted
`ControlPlane__RootCertificate` path. See [database connections](localdbconnection/README.md)
for Windows Visual Studio/SSM setup, EKS Pod Identity, and Azure connection-factory
support. Local SSO/tunnel configuration is in the same appsettings file under
`ControlPlane:LocalConnection`. Legacy `Source:ControlPlaneConnectionString` applies
only when `ControlPlane` is absent; explicit configuration never falls back silently.

The worker's `appsettings.json` contains top-level `ConnectionStrings` and
`ServicePrincipals` arrays. The enabled `ProductionDatabase` entry selects the
Synapse `dart_prod` database and references `sp-oi-dart-to-cat-etl-prod` through
`ServicePrincipalKey`. Supply these missing deployment values before running:

- `Azure__TenantId`: the source service principal's Microsoft Entra tenant ID.
- `ServicePrincipals__0__Secret`: its client secret, injected from a Kubernetes
	Secret or another existing secret provider. The index corresponds to the entry
	in the configured array; do not commit the secret.

The ingestor-local SQL connection factory obtains an application token for
`https://database.windows.net/` using MSAL. All three production source queries
use separate connections with the same token callback. SqlClient retains token
expiry information for connection pooling. Token requests are serialized and cached;
when SqlClient requests authentication, tokens within `ExpirationThresholdInMinutes`
of expiry are refreshed. A 120-minute threshold can refresh on every callback when
tokens have a shorter lifetime. Refresh does not interrupt an active query.

Missing/disabled entries, duplicate keys, missing credentials, and conflicting SQL
authentication settings fail validation before ingestion begins. Keyed settings
take precedence over `Source:ConnectionString`. The legacy raw connection string
is supported only when the `ConnectionStrings` array has no entries. The supplied
worker settings already contain a keyed entry, so setting only the legacy value
does not switch authentication modes. Other environments must override the matching
connection and principal entries, including the server and client ID.

The API does not copy its own settings into the worker. Both scheduled and
API-triggered Jobs inherit the CronJob's worker configuration and secret references.
No source authentication settings are required in the API host.

A scheduled Job does not supply a wind-farm ID. Each pod atomically claims one
eligible Production cursor from RDS, processes it, then claims another. Farms
are processed sequentially inside each pod; Job `parallelism` controls the number
of cooperating pods. `Ingestion:WindfarmId` is not read by scheduled execution.
API-triggered refresh Jobs supply `REFRESH_WINDFARM_ID`,
`REFRESH_DATA_DATES` (comma-separated `yyyy-MM-dd` values), and `REFRESH_RUN_ID`.
Refresh requires an enabled registered Production cursor with a finite checkpoint.
It preserves incremental progress, completion/outcome and failure/backoff fields.
Standalone worker requests may omit the run ID (one is generated); API requests must
supply a valid lowercase Kubernetes-compatible RunId. Any partial `REFRESH_*` input
fails closed, including metadata-only inputs.

Exit codes: `0` for successful scheduled attempts/no due work, `1` for failures or
Busy refresh, `2` for invalid configuration/input, and `130` for shutdown cancellation.
No claimable work does not mean every farm completed: logs distinguish registered,
busy and remaining-due counts. Budget exhaustion is reported separately.

### RDS cursor scheduling (v12)

Use [the shared schema](src/db/migrations/0002_create_extraction_schema.sql) for a fresh
database (after `0001_create_reference_schema.sql`; the Raw Fan-out tables are in
`0003_create_raw_fanout_schema.sql`). It defines `extraction_cursor` and `extraction_run` directly.
No application executes DDL. The scripts use plain `CREATE` and are not an upgrade path for an
existing database.
Existing assignment/work tables are no longer read; retire them administratively only
after preserving any required data and reconciling outstanding work. No live drops
were performed by this change.

Administrators register `(source_type, windfarm_id)` with `is_enabled` and an explicit
finite `last_committed_updated_at` plus the adapter's `last_committed_id`. Use
`ON CONFLICT (windfarm_id, source_type) DO NOTHING` for idempotent seeding. Never
reset a committed checkpoint or lease token to restart ingestion. Disabled rows are
retained and excluded from new claims; disabling does not stop a current lease.
Production selection uses `source_type = 'Production'`, without another registry.

Eligibility uses PostgreSQL time: never-run and abandoned incremental attempts are
immediately due; terminal success (including no data) waits `MinimumIngestionInterval`;
failure waits `FailureRetryBackoff`. An active lease blocks both modes. Expired refresh
does not reset the incremental schedule. No `next_eligible_utc` or batch ID is persisted.

Defaults under `Ingestion`: interval `01:00:00`, backoff `00:05:00`,
`MaxClaimsPerInvocation=100`, `MaxInvocationDuration=00:50:00`, lease `00:05:00`.
Settings are captured at process startup; restart Jobs to apply changed policy.
The deadline cancels current work safely with bounded cleanup. Workers never sleep
waiting for future eligibility. The existing daily CronJob schedule is unchanged:
a five-minute backoff does not guarantee a five-minute retry without a running Job.

Claims lock one due cursor with `FOR UPDATE SKIP LOCKED`, increment the token and
create its attempt in one short transaction, committed before external I/O. Finalization
fences identity/token/owner/expiry, records outcome and published dates, updates only
incremental scheduling state when applicable, and releases the lease atomically.
Job retries share request correlation but have distinct attempt IDs. Expired attempts
are marked failed during takeover. Earlier committed windows are never rolled back.

The CronJob retains `Forbid`, `NonIndexed`, one completion/worker, `Never`, finite
backoff and a one-hour Job deadline. `Ingestion__JobName` is audit metadata only.
API-created refresh Jobs clone this template and inject inputs only into
`JobTrigger:WorkerContainerName` (default `production-ingestor-worker`).

`POST /v1/refresh` accepts one farm, explicit dates, reason and RunId; authenticated
identity supplies RequestedBy. It returns HTTP 202 with JobName, RunId and a status
link. Duplicate RunId returns 409. `GET /v1/status/requests/{runId}` combines up to
100 recent attempts with Job state, distinguishing Accepted/Starting, Running,
Busy/Failed/Cancelled/LeaseExpired, StartupFailed, Unknown and IngestionSucceeded.
Missing history is never success. Job lookup requires `get jobs` RBAC. Keep Job
history long enough to investigate pre-startup failures; after deletion without an
attempt, status is Unknown. Ingestion success does not certify downstream calculation.
RunNow and all-farm API operations remain outside this delivery.

Cursor commits remain ordered after Raw persistence and SQS acceptance. The latest
visible source timestamp is included by using the next PostgreSQL-representable
microsecond as the exclusive query end. Configure safety lag for the source's late
commit behavior; source timestamps must accurately expose updates. This is not a
guarantee for unseen late transactions, deletions, or missing source change history.

guarantee of this change.
Recovery is at-least-once: a publish-before-commit crash or zombie can emit duplicate
notifications. Raw Fan-Out deduplicates artifact plans, and the Production and Outage
Harmonizer paths validate and durably process their supported work. This does not certify
end-to-end loss-calculation correctness: other source consumers, downstream coordination
and calculation remain separate rollout prerequisites. See
[docs/Harmonizer](docs/Harmonizer) for supported behavior and remaining boundaries.

### Production telemetry and verification

Worker logs continue through `ILogger`. OpenTelemetry logs, traces, and metrics
use service name `cat-production-ingestor`; scheduling activities and meters use
`ProductionIngestor.Scheduling`. Set `Telemetry__ExportEnabled=true` and the standard
`OTEL_EXPORTER_OTLP_*` environment variables to export. Export is off by default;
secrets belong in injected exporter headers, never assignment rows. SQS carries
W3C `traceparent`/`tracestate` attributes for Raw Fan-Out.

Use the existing local PostgreSQL test service, not RDS:

```powershell
docker compose -f scripts/raw-fanout/compose.integration.yml up -d --wait postgres
$env:PRODUCTION_POSTGRES_INTEGRATION = '1'
dotnet test tests/Raw/ProductionIngestor.UnitTests/ProductionIngestor.UnitTests.csproj
dotnet test tests/Raw/ProductionIngestor.IntegrationTests/ProductionIngestor.IntegrationTests.csproj
```

Database tests use unique disposable schemas and reject databases other than the
isolated local `rawfanout_test`. They default to port 55432; the existing
`RAWFANOUT_TEST_POSTGRES` override is supported. Without the explicit test flag,
database tests are reported as skipped, not passed. No competing Compose environment
is required.

### Raw Fan-out SQS

The worker's `appsettings.json` includes:

```json
"Sqs": {
	"Region": "eu-north-1",
	"RawFanOut": {
		"QueueUrl": "https://sqs.eu-north-1.amazonaws.com/902707423845/cat-raw-dev"
	}
}
```

This is the development Raw Fan-out queue, not the downstream harmonization queue.
Reading the other team's production SQL source while publishing to this development
queue is intentional. ProductionIngestor sends the existing Raw artifact reference
only after storage succeeds and advances the fenced cursor only after SQS accepts
publication. A separate Raw Fan-out Coordinator consumes that reference and creates
Item-Day batches, as defined in architecture Sections 4.7, 5.2-5.4, and 23.1.

Override with `Sqs__Region` and `Sqs__RawFanOut__QueueUrl`. Region and URL must match;
only canonical HTTPS regional standard SQS URLs are supported. Missing/invalid
settings fail at startup. FIFO and emulator endpoints are not supported by this
configuration. Migrate overrides from `Source__SqsQueueUrl`: the Worker no longer
reads that value or falls back to it. The legacy SourceOptions validation overload
remains available to direct callers, but is not used by the Worker.

#### EKS Pod Identity

AWS credentials use the SDK default credential chain, including automatic refresh
of EKS Pod Identity credentials. The current AWS SDK v4 supports Pod Identity; no
application access keys, secrets, role ARN, or AssumeRole code are needed. See the
[AWS SDK requirements](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-minimum-sdk.html).

Before deployment, the infrastructure owner must complete these steps using the
actual cluster, namespace, and service account names:

1. Verify that the EKS cluster is in account `902707423845`, uses supported compute,
	 and has the Pod Identity Agent available (or built-in support in EKS Auto Mode).
	 Ensure the agent/node role has the required `eks-auth:AssumeRoleForPodIdentity`
	 permission and network access to EKS Auth; the worker also needs SQS access.
2. Configure the existing sender role's trust for `pods.eks.amazonaws.com`, with
	 `sts:AssumeRole` and `sts:TagSession`. Restrict trust to the intended cluster,
	 namespace and service account using the supported request-tag conditions. See
	 [AWS Pod Identity trust policy](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-role.html).
3. Associate the worker service account with
	 `arn:aws:iam::902707423845:role/eks/crossaccount/eks-dnd-dev-cat-dev-cat-raw-dev-sender`
	 using an EKS Pod Identity association. Set that service account as
	 `spec.jobTemplate.spec.template.spec.serviceAccountName` in the CronJob.
	 API-triggered Jobs inherit this template. Pod Identity does not use the IRSA
	 service-account role annotation.
4. Grant that role the following send-only identity permission, retaining the
	 supplied queue resource policy. The queue policy does not configure role trust
	 or the Pod Identity association.

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": "sqs:SendMessage",
			"Resource": "arn:aws:sqs:eu-north-1:902707423845:cat-raw-dev"
		}
	]
}
```

If a customer-managed KMS key encrypts the queue, also grant the producer
`kms:GenerateDataKey` and `kms:Decrypt` on that key and configure its key policy.
Do not inject static AWS credentials or mount profiles into the worker pod: earlier
credential providers would override Pod Identity. Restrict pod access to node-role
credentials according to cluster policy. If the cluster topology or compute does
not support this setup, revisit identity provisioning rather than adding static keys.

The publisher makes no queue-discovery, receive, delete, or queue-attribute calls;
these are not permitted by its sender role. Consumer redrive/DLQ settings and
FanOutFailure recovery remain separate deployment/workload responsibilities.
For local development, use an authorized temporary AWS profile through the SDK
credential chain; do not put credentials in JSON. Live verification must use an
authorized dev run with a real Raw artifact, not an arbitrary test message that
could trigger downstream work. These repository changes do not provision IAM,
Pod Identity associations, queue policies, or Kubernetes resources.

### Processing Configuration

- `Ingestion:ParquetRowGroupSize` controls rows per Parquet row group (default 50,000).
- `Ingestion:MaxRowsPerShard` independently limits rows per artifact (default 200,000).
- `Ingestion:MaxArtifactShardSizeBytes` limits serialized shard bytes (default 128 MiB).
- `Ingestion:MaxAffectedDatesPerWindow` limits distinct discovered dates (default 5,000).
	It replaces the former no-op `DiscoverySpillThreshold`; update deployments using that key.
- `Ingestion:MaxSnapshotRows` bounds retained distinct snapshot rows (default 1,000,000).
- `Ingestion:SourceQueryBatchSize` limits each SQL page, not the total snapshot.
- `Ingestion:LeaseDuration`, `SafetyLagInterval`, and `MaxCatchUpWindowSize` control
	ownership and discovery windows. Invalid values fail before claiming work.

Exceeding a date/row limit fails without committing that window; this implementation
does not claim disk-backed spill support. Source pages are enumerated completely.
The configured source must have unique (windfarm, turbine, UTC production timestamp)
keys, stable ordering, and the agreed update/commit-time semantics. SQL change paging
uses update timestamp plus turbine and production timestamp, not a fabricated row ID.

### Artifacts and Recovery

Raw and quarantine artifacts are genuine Parquet. Measurements retain SQL `decimal`
values; missing production values remain null. Decimal column scales are derived
from their values, and UTC timestamps use INT96 to preserve .NET timestamp ticks.
Extra named measurements are retained as their own typed decimal columns
(`LostProductionCapacitykWh`, `SiteAverageWindSpeedMps`), matching the
legacy `ProductionData` field names. Production Parquet payload schema v2 uses the ordered fields
`WtgRdsppId`, `StartTimeInterval`, `EndTimeInterval`, `ActualProductionKWh`, `PossibleProductionKWh`,
`LostProductionCapacitykWh`, `ContractualPoPEColumn`, `SiteAverageWindSpeedMps`, and `LastUpdatedDate`.
The v2 writers preserve source decimal values and INT96 timestamp precision; Fan-Out reads v2 identity
fields and continues to accept legacy `ItemId`/`ProductionTimestampUtc` files. Raw files include
extraction token and payload schema version. Shards are
ordered deterministically and uploads are verified against their generated bytes.

Only a fully handed-off source window advances the cursor. The legacy numeric
`last_committed_id` column is retained for schema compatibility and is set to zero
on new window commits. Lease renewal is bounded; loss of ownership cancels in-flight
work, and commits check token, expiry, expected position, and monotonic progress.
Failed windows can replay already-published days, so downstream consumers must remain
idempotent. External message field names and status JSON timestamp field names are
preserved. An absent lease expiry is now represented as null, not a minimum date.

### Verification

```powershell
dotnet test tests/Raw/ProductionIngestor.UnitTests/ProductionIngestor.UnitTests.csproj
dotnet test tests/Raw/ProductionIngestor.IntegrationTests/ProductionIngestor.IntegrationTests.csproj
dotnet build src/Api/ProductionIngestor.Api/ProductionIngestor.Api.csproj
```

The unit project also builds the Worker. Local integration tests use the real
Parquet.Net writer/reader with in-memory object storage. They do not connect to
SQL Server, PostgreSQL, Azure Blob, SQS, or Kubernetes. Verify source uniqueness,
equal-timestamp SQL page boundaries, concurrent lease takeover, provider retries,
and representative load against the configured environments before deployment.

## Outage Ingestor

Outage ingestion reads `cat_gold.OutageClassification` (live) and
`cat_gold.OutageClassificationPeriodDeleted` (deletion feed) from the same legacy SQL
Server source as `OutageRepository` in `cat-backend-service`, scoped to one
`SiteRDSPPID` (windfarm) per run. `OutageIngestor.Core` is host-independent; no
production Worker/API host exists for it yet in this repository.

The two source tables are treated as two physical feeds forming one logical Outage
incremental extraction: both are read under the same claimed `(Outage, windfarm)`
execution/lease token and the same frozen `[low, end)` window, but are kept as two
separate Raw artifacts rather than merged into one file, so Raw stays close to what
each source table actually supplied:

```
/raw/outage/windfarm=WOW01/mode=incremental/extraction=42/
    events/
		window-20260901T000000Z-to-20260901T010000Z-part-0001.parquet
    deleted-events/
		window-20260901T000000Z-to-20260901T010000Z-part-0001.parquet
```

Refresh writes use `mode=refresh` in the same position, with snapshot parts and
`manifest.json` under `snapshot/`. Quarantine artifacts, run-history output paths,
and logs use the matching mode prefix. Existing artifacts are not moved.

An artifact is written for a feed only when it has at least one valid row that
window; a feed with nothing to report has null paths in the result rather than an
empty placeholder file. The two feeds carry **independent checkpoints** on
`extraction_cursor` (`last_committed_updated_at`/`last_committed_id` for the
live/changed table, `deleted_last_committed_updated_at`/`deleted_last_committed_id`
for the deletion table — see
[`src/db/migrations/0006_extraction_cursor_deleted_feed_checkpoint.sql`](src/db/migrations/0006_extraction_cursor_deleted_feed_checkpoint.sql)),
because their `LastUpdatedUTC` progress is not synchronized. Each feed is discovered,
windowed and published on its own schedule (`SqlOutageSourceAdapter.GetChangedOutagesAsync`
/ `GetDeletedOutagesAsync`), and each written Raw part is announced by its own
`RawOutageBatchAvailableV1` message (`PayloadKind = OutageEvents` or
`OutageDeletedEvents`) rather than one message describing both feeds. Within one
run the live/changed feed is always fully processed before the deletion feed, and
**both checkpoint pairs are committed together in a single fenced update only after
both feeds finish successfully** — there is no per-window interim commit. If either
feed fails, neither checkpoint pair advances; the next attempt replays from the
previously committed checkpoints, which is safe because Raw is append-only and
downstream merge is an idempotent keyed upsert.

Incremental captures new/changed outages as their full current event state (never a
delta) and captures deletions as explicit tombstones — a deleted outage must not
disappear silently from the change feed just because a later poll returns zero live
rows for it. Refresh (`GetAllCurrentEventsAsync`) reads only the live table: it is
the full authoritative current snapshot for the requested scope
(`PayloadKind = OutageSnapshot`), so a downstream consumer infers deletion from an
event's absence from that snapshot (`IsFullSnapshot = true`) rather than from a
per-refresh deletion feed read. Refresh never advances either incremental
checkpoint.

### Outage Source Contract

These are documented, **unverified with the source-system owner** assumptions the
adapter relies on:

- `LastUpdatedUTC` increments on every edit to a row, including edits that only
	touch non-key columns (comment/alarm/category fields). Incremental discovery
	windows by this column; an update that does not bump it is invisible to catch-up
	runs until the next Refresh.
- `(WtgRDSPPID, OutageClassificationID)` is a stable, source-enforced unique identity
	for a live classification row and never changes value across edits to the same
	logical outage.
- `OutageClassificationPeriodDeleted` is retained for the full window a downstream
	consumer might need to catch up (i.e. deletes are never purged sooner than live
	row history); this ingestor has no independent way to detect a purged tombstone.
- `OutageID`'s underlying SQL type is not assumed to be `VARCHAR`; it is read via
	`IDataRecord.GetValue` + invariant `Convert.ToString` (see
	`SqlOutageSourceAdapter.ReadIdentityString`) so a `UNIQUEIDENTIFIER` (or other
	convertible type) round-trips without an `InvalidCastException`. The legacy
	`OutageRepository.CreateOutageData` mapper never read `OutageID` at all, so its
	real column type was never exercised by that code path either.

### Retention floor (`windfarm.min_retrieve_date`)

Both the incremental change query and the Refresh full-snapshot query exclude live
events that already ended at or before the windfarm's `min_retrieve_date`
(`IOutageScopeRepository.GetActiveScopeAsync`), mirroring the legacy
`OutageRepository.GetData` `minRetrivelDate` predicate: an outage entirely before the
windfarm's onboarding/retention floor must not be (re)ingested merely because a bulk
correction on the source touched its `LastUpdatedUTC`. The deletion feed is never
bounded by it, matching `GetDeleteData`, which takes no `minRetriveDate` parameter.
`SqlOutageSourceAdapter` receives this floor as an explicit `minRetrieveDateUtc`
argument on every call rather than resolving it itself; when no scope repository is
wired (e.g. unit tests), the runner falls back to `DateTime.MinValue` (no bound).

### Refresh Snapshot Isolation (prerequisite)

`GetAllCurrentEventsAsync` (Refresh mode, which publishes `IsFullSnapshot=true`) opens
**one** SQL connection and enumerates every keyset page inside a single
`SNAPSHOT`-isolation transaction, so the whole run observes one consistent
point-in-time view instead of a separate connection per page that could straddle
concurrent writes across a page boundary. This **requires**
`ALTER DATABASE ... SET ALLOW_SNAPSHOT_ISOLATION ON` on the source database ahead of
time; the adapter checks `sys.databases.snapshot_isolation_state` first and fails
closed (throws, publishes nothing) rather than silently falling back to a weaker
isolation level.

**Residual limitation, not fully solved by snapshot isolation:** a row that is
updated and moves within the `(WtgRDSPPID, OutageClassificationID)` keyset ordering
*after* the reading page has already passed it, but before the snapshot's logical
read instant closes out later pages, can still be missed or seen in an inconsistent
relative order across pages. Do not treat a Refresh's `IsFullSnapshot=true` as a
fully proven basis for downstream "delete anything absent from this snapshot"
semantics without accounting for that residual window; that guarantee is not made by
this repository.

### Control-Plane Schema

All four ingestors use the shared `extraction_cursor`/`extraction_run` schema with
INTEGER `windfarm_id`, TEXT `last_committed_id`, and one audit row per bounded run in
[`src/db/migrations/0002_create_extraction_schema.sql`](src/db/migrations/0002_create_extraction_schema.sql) —
apply it manually via the deployment's existing schema tooling; no host in this
repository executes DDL automatically. The cursor is authoritative for resuming;
run history records terminal status, committed progress, and the output prefix.
Successful finalization updates the cursor and run together in a fenced transaction.
Numeric source IDs remain numerically compared by their ingestors; Outage preserves
its classification source ID as text. Production retains its time-window frontier.

This script defines the fresh schema, not an in-place upgrade. Before updating an
existing deployment, stop all workers and back up its state. Convert windfarm IDs
to their integer domain IDs (never guess a mapping for source codes), rename the
watermark column, convert committed IDs to TEXT, and copy the source-specific cursor
rows while preserving lease-token maxima. Keep Alarm's known-open state. When
consolidating per-turbine Alarm cursors, rewind to a conservative shared floor
(zero if any configured turbine has no checkpoint), not the maximum. Replay the
Outage boundary timestamp when changing its tie-break ordering. Initialize missing
cursor rows before starting workers. Existing tables are not dropped by this script;
an environment-specific upgrade/backfill must be prepared and verified separately.

### Known Limitations

- **Bounded memory, not spill-safe.** `GetChangedOutagesAsync`/`GetDeletedOutagesAsync`/
	`ReadAllPagesAsync` buffer every discovered page fully in memory per feed; a very
	wide catch-up window or a very large Refresh snapshot is bounded only by
	`MaxCatchUpWindowSize`/available process memory, not by any on-disk spill.
- **Harmonizer source coverage is incomplete.** A Production Harmonizer worker now
	consumes and merges supported production work. Outage currently has transport-independent
	rules only; other source consumers and downstream calculation/coordination remain pending.
	See [docs/Harmonizer](docs/Harmonizer) for the merged implementation, migrations, storage
	configuration and remaining integration boundaries. Deployment files there remain examples.
- **Refresh is not scope-bounded.** Refresh re-reads the full current live-table
	snapshot for the windfarm rather than an explicitly requested historical
	event-time range with a durable snapshot manifest (Incremental Refresh Spec,
	Sections 14–22); this remains a deliberate, simpler full-snapshot model.

### Verification

```powershell
dotnet test tests/Raw/OutageIngestor.UnitTests/OutageIngestor.UnitTests.csproj
```

## Alarm Ingestor

Alarm ingestion follows the legacy `AlarmIntegrator`/`AlarmsRepository`/`AlarmAdaptor`
caller behavior in `cat-backend-service`, adapted to the deployed Alarm HTTP API
(`POST {ApiBaseUri}/search`), one windfarm per run. `AlarmIngestor.Core` is
host-independent (no reference to Production/Outage Core or any host); the one-shot
`AlarmIngestor.Worker` is the only host. There is no API host for this ingestor.

This is an explicit, accepted compatibility exception to the v11 change-watermark
model: the deployed API rejects `LastUpdatedUtc` in both filters and `orderFields`
(confirmed live with a bounded read-only request), so there is no source
watermark/revision to key an incremental cursor on. Instead:

- **Discovery** walks every configured turbine from the same windfarm-level numeric
	`AlarmId` checkpoint, using ascending-ID keyset paging. It first freezes one
	global upper bound, processes all turbines through that cutoff, and advances the
	shared checkpoint only after the entire run succeeds. IDs are assumed globally
	increasing across turbines. Repeated or unordered pages fail closed.
- **Known-open polling** re-queries every durably tracked open alarm ID on *every*
	run — including runs where discovery found nothing new — without any start-date
	restriction, so a long-open alarm is never dropped for being "too old". An ID the
	source does not return in a poll stays tracked; **omission is never treated as
	closure or deletion**.
- **Historical retrieval** is an explicit, separately invoked bounded start-time-range
	re-read. It writes new Raw artifacts and enrolls any open alarms it observes, but
	never advances the routine discovery cursor and is **not** an authoritative
	snapshot — the API gives no coverage guarantee for events starting before the
	requested range, so an event absent from a historical result must never be
	inferred as deleted.

Turbine/code selection (`Ingestion:Windfarms[].Turbines`/`AlarmCodes`) mirrors the
legacy caller's windfarm/other-loss-definition (`EventTypeId = 2`) configuration
input; an empty list fails startup validation rather than defaulting to "all codes"
or "no turbines". Adding a turbine or code later requires an explicit Historical
backfill — an existing `AlarmId` cursor cannot recover records that were previously
filtered out.

### Alarm Source Contract

- Request/response field names (`WtgRdsppId`, `AlarmId`, `AlarmCode`, `TimeOnUtc`,
	`TimeOffUtc`, `AlarmType`, `AlarmState`) mirror the legacy `AlarmsRepository`
	filters exactly. `LastUpdatedUtc` is deliberately never sent — a live bounded
	probe against the deployed Swagger-documented endpoint confirmed HTTP 400 for it
	in both filters and `orderFields` (enum conversion errors).
- Discovery/historical/poll requests select `AlarmType In (Alarm, Warning)` and
	`AlarmState In (Good, Deleted)`, matching the legacy caller.
- **The deployed Swagger publishes no response schema, and a successful live response
	body has not been observed** — a comparison probe without `LastUpdatedUtc` timed
	out before returning a body. Field name guesses in `AlarmSearchRequestBuilder`
	(`id`/`alarmId`, `wtgRdsppId`, `timeOnUtc`, `timeOffUtc`, `alarmCode`, `alarmText`,
	`alarmType`, `alarmState`) are carried over from the legacy dynamic-typed caller
	and **must be confirmed against a real response before this adapter is treated as
	production-verified**. An empty body, an unrecognized JSON shape, a missing
	required identity field, or a response whose `WtgRdsppId` does not match the
	requested turbine all throw rather than being treated as an empty successful page.
- Raw events preserve the source's original `StartTimeUtc`/nullable `EndTimeUtc`
	exactly; there is no duration cap applied here (that is a downstream calculation
	concern per the legacy `AlarmAdaptor`, not a Raw transformation). Each event also
	carries an explicit `ObservedAtUtc`/`SequenceInRun` extraction-ordering pair —
	this is not a source revision, and a later observation must not be overwritten by
	an earlier extraction arriving late downstream.

### Durable State and Fencing

Alarm uses the same `extraction_cursor` and `extraction_run` tables as the other
ingestors. Its single cursor stores the global numeric Alarm ID as TEXT; it does not
invent an updated-time watermark. `alarm_known_open_alarm` remains a separate
operational table tracking open IDs per turbine, not another extraction cursor.

Open-alarm enrollment and resolved-open removal are fenced independently after their
matching publications. Final global checkpoint advancement and run completion share
one database transaction. A failure on any turbine leaves the global checkpoint
unchanged, although already-published pages may have updated the known-open set.
Successful publications can be replayed after a failure, so downstream handling must
remain idempotent.

`AlarmBatchWriter` fails a batch entirely closed if **any** row in it is quarantined
(missing turbine identity/`AlarmId`, malformed `StartTimeUtc`, or `EndTimeUtc` before
`StartTimeUtc`): it writes a quarantine audit artifact for diagnosis, then throws
before producing or publishing any Raw artifact for that page/batch — including its
otherwise-valid rows. A partial-success write (publishing only the valid rows) would
let the cursor/known-open state advance past the still-unresolved quarantined row, so
no such partial path exists; the run fails and the whole page/batch is retried
unchanged on the next run.

### Known Limitations

- **Response shape unverified live** (see Alarm Source Contract above) — this is the
	single largest residual risk before production use.
- **Not an authoritative full-history snapshot.** Routine discovery assumes new
	source alarms have globally increasing IDs across turbines; it does
	not recover a late-visible lower ID for an existing turbine, and routine polling
	stops after a durably published closure/deletion, so it never detects a
	subsequent edit to an already-closed alarm. Full v11 footprint reconciliation is
	downstream work requiring a source completeness contract this API does not
	provide today.
- **No API host.** Only `AlarmIngestor.Core` and the one-shot `AlarmIngestor.Worker`
	exist.

### Verification

```powershell
dotnet test tests/Raw/AlarmIngestor.UnitTests/AlarmIngestor.UnitTests.csproj
dotnet build O.WP.CAT.LossCalculation.Platform.sln
```

Unit tests use fakes for the HTTP adapter, control-plane repository, object store and
publisher — they do not connect to PostgreSQL, Azure Blob, SQS, or the live Alarm API.
Live provider connectivity and the response-shape assumptions above must be verified
separately before production use.
