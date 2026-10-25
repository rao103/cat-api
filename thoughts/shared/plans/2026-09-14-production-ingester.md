# Production Ingestor (Extraction Pod) — Implementation Plan

## Overview

Build the Production Ingestor: the extraction pod that pulls incremental Production
measurements (actual production, possible production, active power, and other
agreed 10-minute tags treated as **one** Production input) from the source system,
writes an immutable Raw Windfarm-Day snapshot to Blob storage, publishes a
`raw-fanout` SQS message, and advances a fenced `ExtractionCursor` — with no
dispatcher stage and no `RawDispatch` ledger.

This is the extraction pod only. The Raw Fan-out Coordinator and Harmonizer
(architecture §5.4–§5.11) are downstream consumers of this pod's output and are
**out of scope** — this plan implements the message contract they consume, not
their logic.

Source documents:
- [01-production-ingester.md](../../../01-production-ingester.md)
- [Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md](../../../Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md) §§4.1–4.9, 5.1–5.12, 6.4, 6.9, 8.6, 14.7, 15.1, 16.3

## Current State

The repository (`cat-dl-backend-service`) contains only the two planning markdown
documents above. There is no source code, no project scaffolding, no CI, and no
existing repo conventions to follow. This plan therefore also proposes the
initial project structure.

## Desired End State

An extraction pod that can be deployed to EKS and, for a configured
`(WindfarmId, SourceType=Production)`:

1. Acquires a fenced lease on `ExtractionCursor`.
2. Computes `EffectiveQueryLow = committed.LastUpdatedDate − SafetyLagInterval`
   and a frozen `HighWatermark`, splits the span into bounded catch-up windows.
3. Discovers changed rows per window via keyset pagination, derives distinct
   affected UTC `ProductionDate`s.
4. Re-reads the complete authoritative Windfarm-Day snapshot per affected date.
5. Writes an immutable, sharded-if-needed Parquet Raw artifact under
   `/raw/production/windfarm=<w>/date=<date>/extraction=<leaseToken>/`.
6. Sends a `raw-fanout` SQS message per artifact/shard-set.
7. Fences the cursor advance on `LeaseToken`, only after every day in the window
   has a durable Raw write AND an accepted SQS send.
8. Exposes Source Availability and Data Availability signals per §14.7.
9. Supports historical `RefreshAndRecalculate` production refresh (§15.1/§16.3)
   using a separate concurrency cap and a new extraction token, without
   requiring `LastUpdatedDate` to have changed.

### Automated verification (end state)
- All unit/integration tests below pass in CI (`dotnet test` / equivalent).
- Load test: full-scale fixture (1,500 WTGs × 144 intervals) completes within
  target latency without exceeding configured memory/connection bounds.

### Manual verification (end state)
- Dual-run against legacy extraction for a sample of windfarms; reconcile
  per-Item-Day counts/values before cutover (no legacy system exists in this
  repo yet — see Open Questions).
- Confirm monitoring dashboards show Source/Data Availability distinctly.

## What We're NOT Doing

- Not implementing the Raw Fan-out Coordinator or Harmonizer (§5.4–§5.11) —
  only the SQS message contract they consume.
- Not implementing other source types (outage, alarm, curtailment, adjustment,
  event-shaped Raw per §4.3.1) — Production only, per the ticket's scope.
- Not implementing `ItemDayInputState`, `WindfarmDayProductionState`,
  `Calculation Package Coordinator`, or any Harmonized-zone writes.
- Not implementing the `FanOutFailure` consumer (§5.9) — that lives with the
  Fan-out Coordinator, not the extraction pod.
- Not building the full monitoring/alerting stack — only emitting the metrics
  and state needed to populate §14.7 views.
- Not implementing the `Application.Api` `RefreshAndRecalculate` endpoint
  that coordinates every source — only the Production-scoped planning
  (`ProductionRefreshPlanner`) and an internal request abstraction it can be
  wired to.
- Not building Terraform/Bicep/other infra-as-code beyond a basic
  Dockerfile + Helm chart skeleton (GitHub Actions CI/CD per Phase 0) —
  cluster/infra provisioning is assumed to exist or is a separate work item
  unless the user says otherwise.

## Open Questions

These require a decision from the user/developer before or during
implementation; do not guess:

1. ~~**Language/framework for the extraction pod.**~~ **Resolved: C#/.NET.**
   Confirmed by the user — the extraction pod is a C# project. .NET version
   left as an implementation detail (default to the current LTS unless the
   repo's CI/deploy conventions dictate otherwise).
2. ~~**Concrete production source system.**~~ **Resolved: SQL Server.**
   Authenticate via service principal (Azure AD / SQL auth with a service
   principal — treat as Azure AD token auth unless the connection string
   implies SQL auth). Rows are keyed by windfarm + WTG + UTC time. Connection
   string is supplied via config file (see Open Question resolution 3/4
   below — config file is now the uniform mechanism for all connection
   strings/credentials except AWS, which uses IAM role).
3. ~~**Default `SafetyLagInterval` value.**~~ **Resolved: zero**, configured
   in the config file (overridable per source/windfarm without a code
   change). Because zero implies the source guarantees commit-time
   stamping, Phase 1's safety-lag re-read logic must still be implemented
   (config-driven, not hard-coded to zero) and exercised by a fixture that
   sets a non-zero value, so the zero default is a configuration choice, not
   a structural removal of the feature.
4. ~~**Cursor/control-plane database.**~~ **Resolved: PostgreSQL, not Azure
   SQL.** The user confirmed the control-plane store (`ExtractionCursor`,
   `FanOutFailure`, etc.) is moving to PostgreSQL, hosted on AWS. This
   supersedes the architecture doc's "Azure SQL" framing for this
   implementation — DDL in this plan must be written in Postgres-compatible
   SQL (e.g. `gen_random_uuid()`/`now()` instead of `SYSUTCDATETIME()`,
   `SELECT ... FOR UPDATE` / row-level locking or a `WHERE ... RETURNING`
   fenced-update pattern instead of `@@ROWCOUNT`). Credentials via config
   file.
5. ~~**Queue/cloud topology.**~~ **Resolved.** Current state: Azure Blob
   (Raw storage) + AWS (PostgreSQL control-plane + SQS `raw-fanout`).
   Planned future state: Blob storage migrates to S3 (AWS-only). The Raw
   writer must therefore sit behind a small storage abstraction
   (`IRawBlobStore` or similar) with an Azure Blob implementation now and an
   S3 implementation added later without touching Raw path/shard logic.
   AWS credentials via IAM role (pod-assigned role, e.g. IRSA on EKS or
   equivalent); Azure Blob credentials via config file per Open Question 3
   pattern (confirm exact mechanism — connection string vs. account
   key/SAS — during Phase 2, defaulting to a config-file-supplied connection
   string).
6. ~~**Blob storage conventions/retention.**~~ **Resolved: config file**
   drives account/container naming. Retention/GC policy for abandoned or
   superseded extraction attempts (§4.9) is not yet specified — treat as a
   deferred follow-up (Phase 2 ships without automated GC; add a TODO/backlog
   item rather than blocking on it).
7. ~~**Legacy dual-run target.**~~ **Resolved: `cat-backend-service`.** This
   new Production Ingestor is one of several applications replacing the
   existing `cat-backend-service`. Phase 5 (dual-run/cutover) should compare
   this ingester's Raw/Harmonized output against `cat-backend-service`'s
   current production data for the same windfarm/day range before cutover.
   Exact comparison tooling/access to `cat-backend-service` output is still
   to be worked out during that phase.
8. ~~**Windfarm/WTG configuration source of truth.**~~ **Resolved:**
   windfarm/WTG membership data comes from a PostgreSQL table (in the same
   AWS PostgreSQL instance as the control-plane per Open Question 4).
9. ~~**Backfill/refresh trigger mechanism.**~~ **Resolved: API endpoint.**
   The Production Ingestor exposes an HTTP endpoint to accept
   `RefreshAndRecalculate` requests (windfarm + date range). Concurrency cap
   value itself is still unspecified — default to a conservative config-file
   value (e.g. 1–2 concurrent backfill windows per windfarm) and make it
   tunable without a code change.

## Approach

Phases follow the three delivery slices in the ticket, split further so each
phase has an independently testable, mergeable unit of work. Language is
**C#/.NET** (confirmed by the user). CI/CD uses **GitHub Actions** (confirmed
by the user) — no Azure Pipelines, no `k8s.service/`, no `docker-helpers/`.

### Folder structure — refined per review feedback

The initial `src/ProductionIngestor/{Cursor,Discovery,Extraction,Raw,FanOut,
Monitoring,Refresh}` layout is refined as follows:

- **Add `Execution/`** with `ProductionIngestionRunner` as the single class
  that owns ordering: lease → discovery → extraction → Raw write → queue
  publication → cursor commit. `Program.cs` stays limited to configuration,
  DI registration, and starting the run — it does not itself sequence the
  ingestion steps.
- **Rename `FanOut/` → `Messaging/`.** This pod publishes a Raw-artifact
  reference message; it is not the Raw Fan-out Coordinator (which creates
  Item-Day batches, and is out of scope). `Messaging/` avoids implying that
  responsibility lives here.
- **`Refresh/` holds only production-refresh planning**
  (`ProductionRefreshPlanner`): validate the windfarm/date range, split it
  into Windfarm-Day work items, and hand that work to
  `ProductionIngestionRunner`. It is a folder of planning logic, not a
  second pod or pipeline. The public `RefreshAndRecalculate` HTTP endpoint
  that coordinates *all* sources belongs in a separate `Application.Api`
  component, which does not exist in this repo yet and is out of scope here
  — this repo only implements the Production part reachable once such a
  request arrives (see `IRefreshRequestSource` abstraction, Phase 4).
- **`ProductionIngestor.Contracts` carries only the external message
  schema** (`RawArtifactAvailableV1`, the `raw-fanout`-equivalent message).
  Internal configuration classes (`IngestionOptions`, `SourceOptions`) and
  internal source-shaped models move into the worker project's
  `Configuration/` and `Extraction/` folders — they are not a public
  contract.
- **`Monitoring/` reports extraction-time observations only**
  (`IngestionTelemetry`: source contact success/failure, rows retrieved,
  cursor advancement). It does not decide authoritative Item-Day
  completeness — that readiness decision belongs to the Harmonizer
  (`ItemDayInputState`, out of scope). Phase 5's `DataAvailability` table is
  therefore framed as "what this pod observed being written to Raw", not a
  claim of downstream calculation-readiness.
- **PostgreSQL migrations are unaffected** by this restructuring — they
  remain the chosen control-plane database per the earlier Open Questions
  resolution; only the folder/class names above change.

### Execution model — scheduled Job, not a long-lived poller

Per review feedback: if EKS starts this as a scheduled Job, the process
should perform its assigned unit of work and **exit** — it must not become
an accidental endless polling worker. `ProductionIngestionRunner` is
designed to run one bounded pass (one windfarm's catch-up windows, or one
refresh request) and return/exit; `Program.cs` does not host a
`BackgroundService` poll loop. Both normal-cadence and historical-refresh
runs invoke the same runner and the same extraction/writing components —
they differ only in how the set of `(WindfarmId, ProductionDate)` work
items is selected:

| Mode | How it selects days | Cursor effect |
|---|---|---|
| Incremental / catch-up | Discover changed production dates within bounded `LastUpdatedDate` windows | Advances the incremental cursor after successful handoff |
| Historical refresh | Explicitly requested UTC production dates | Preserves normal incremental progress (never regresses/skips it) |

Reloading January must not move the September incremental cursor backward —
refresh work items reuse the same source adapter, Parquet writer, and
message publisher, but track their own run/token state independently of
`ExtractionCursor.LastUpdatedDate/LastCommittedId` (Phase 4).

Proposed repo layout:

```text
cat-dl-backend-service/
  ProductionIngestor.sln
  README.md
  Dockerfile

  src/
    ProductionIngestor/
      ProductionIngestor.csproj

      Execution/
        ProductionIngestionRunner.cs   owns lease→discovery→write→publish→commit ordering
        IngestionRequest.cs
        IngestionMode.cs                Incremental | Refresh

      Cursor/
        IExtractionCursorRepository.cs
        PostgresExtractionCursorRepository.cs
        ExtractionLease.cs

      Discovery/
        ChangeDiscoveryService.cs
        ExtractionWindowPlanner.cs

      Extraction/
        IProductionSource.cs
        ProductionSourceAdapter.cs
        WindfarmDayExtractor.cs

      Raw/
        ProductionParquetWriter.cs
        RawPathBuilder.cs
        IRawObjectStore.cs
        AzureBlobRawObjectStore.cs        S3 implementation added later behind this interface

      Messaging/
        IRawArtifactPublisher.cs
        SqsRawArtifactPublisher.cs

      Refresh/
        ProductionRefreshPlanner.cs        planning only; public endpoint lives in Application.Api (out of scope)

      Monitoring/
        IngestionTelemetry.cs              extraction-time observations, not authoritative readiness

      Configuration/
        IngestionOptions.cs
        SourceOptions.cs

      Program.cs                            config, DI, start one bounded run, then exit

    ProductionIngestor.Contracts/
      ProductionIngestor.Contracts.csproj
      RawArtifactAvailableV1.cs              external message schema only

  tests/
    ProductionIngestor.UnitTests/
    ProductionIngestor.IntegrationTests/     lease takeover, stale-token rejection,
                                              partial queue-send failure, crash recovery

  db/
    migrations/                              extraction_cursor DDL, indices (Postgres)

  deploy/
    helm/

  docs/
    production-ingester-plan.md
    operations-runbook.md

  .github/
    workflows/
      ci.yml
      release.yml

  thoughts/shared/plans/
```

---

## Phase 0 — Project scaffolding

### Overview
Stand up the buildable skeleton so subsequent phases add real logic to real
projects instead of design-only code.

### Changes
- `src/ProductionIngestor/ProductionIngestor.csproj` (worker project,
  `net8.0`), with the `Execution/`, `Cursor/`, `Discovery/`, `Extraction/`,
  `Raw/`, `Messaging/`, `Refresh/`, `Monitoring/`, `Configuration/` folders
  scaffolded (empty/interface stubs where later phases add real logic).
- `src/ProductionIngestor.Contracts/ProductionIngestor.Contracts.csproj`
  containing only `RawArtifactAvailableV1` (the external message DTO).
- `tests/ProductionIngestor.UnitTests`, `tests/ProductionIngestor.IntegrationTests`
  (xUnit).
- `ProductionIngestor.sln` at repo root wiring all projects.
- `Dockerfile` at repo root — multi-stage build, `mcr.microsoft.com/dotnet/aspnet:8.0`
  (or an internally-approved base image if one exists; the JFrog/Alpine base
  used by `cat-backend-service` was tied to its now-removed Azure Pipelines
  flow, so re-confirm the base image with the platform team rather than
  assuming it), non-root user.
- `.github/workflows/ci.yml` — restore/build/test on PR.
- `.github/workflows/release.yml` — build/push image, deploy trigger (exact
  deploy target TBD — see Open Questions; this repo no longer assumes AKS/ACR
  specifically since the Azure-Pipelines-specific conventions were dropped).
- `README.md` — project overview, local dev setup.
- `docs/production-ingester-plan.md` (copy/reference of this plan) and
  `docs/operations-runbook.md` (stub, filled in as failure/runbook details
  solidify in later phases).
- `deploy/helm/` — Helm chart skeleton (values, deployment/Job template) once
  the scheduled-Job-vs-Deployment decision is confirmed (see Execution model
  above).
- Basic `appsettings.json` / options binding (`IngestionOptions`,
  `SourceOptions`) for: connection strings, SQS queue URL, Blob/S3 container,
  `SafetyLagInterval`, catch-up window size, per-source concurrency
  semaphore size, Parquet row-group/shard size limit, fan-out batch page
  size (even though batching itself is downstream, the ingester needs the
  size knob documented per requirement).
- `db/migrations/0001_create_extraction_cursor.sql` — DDL (see Phase 1).

### Success Criteria

**Automated**
- `dotnet build` succeeds across the solution.
- `dotnet test` runs (zero tests, green) confirming test projects wire up.
- `.github/workflows/ci.yml` runs restore/build/test successfully on a PR.

**Manual**
- Language/framework confirmed as C#/.NET; CI confirmed as GitHub Actions —
  no further confirmation needed before starting this phase.
- Confirm base container image and deployment target (Kubernetes
  Deployment/Job vs. something else) with the platform team before
  finalizing the Dockerfile and `deploy/helm/` templates.

---

## Phase 1 — `ExtractionCursor` schema, fenced lease, extract-without-publish proof

### Overview
Implement the cursor/lease primitive and prove bounded discovery + Windfarm-Day
read-back against a source adapter abstraction, without writing Raw or
publishing SQS (delivery slice 1).

### Data model — `ExtractionCursor`

```sql
CREATE TABLE extraction_cursor (
    windfarm_id            VARCHAR(64)   NOT NULL,
    source_type            VARCHAR(32)   NOT NULL,   -- 'Production'

    last_updated_date      TIMESTAMPTZ   NOT NULL,
    last_committed_id      BIGINT        NOT NULL,

    lease_owner            VARCHAR(128)  NULL,
    lease_token            BIGINT        NOT NULL DEFAULT 0,  -- monotonic
    lease_expires_at       TIMESTAMPTZ   NULL,

    last_run_started_utc   TIMESTAMPTZ   NULL,
    last_run_completed_utc TIMESTAMPTZ   NULL,
    consecutive_failures   INT           NOT NULL DEFAULT 0,
    last_error             VARCHAR(2000) NULL,

    CONSTRAINT pk_extraction_cursor PRIMARY KEY (windfarm_id, source_type)
);
```

(Primary key doubles as the UNIQUE constraint from the architecture doc; no
separate constraint needed in Postgres.)

Lease acquisition (atomic, single statement, using `RETURNING` in place of
SQL Server's `@@ROWCOUNT`/`OUTPUT`):

```sql
UPDATE extraction_cursor
   SET lease_owner = @newOwner,
       lease_token = lease_token + 1,
       lease_expires_at = now() + (@leaseSeconds || ' seconds')::interval,
       last_run_started_utc = now()
 WHERE windfarm_id = @windfarmId
   AND source_type = @sourceType
   AND (lease_expires_at IS NULL OR lease_expires_at < now())
RETURNING lease_token;
-- zero rows returned => another owner holds a live lease; back off
```

The returned `lease_token` becomes the run's fencing token, reused for the Raw
path `extraction=<token>` and the final fenced commit (Phase 3).

Lease heartbeat (renewal during long runs): same predicate but `AND
lease_owner = @owner AND lease_token = @token`, extending `lease_expires_at`.

### Windowing / discovery logic (`Discovery/`)

- `CatchUpWindowPlanner`: given `committed.LastUpdatedDate`, `SafetyLagInterval`,
  and a frozen `HighWatermark` (queried once at run start from the source),
  compute `EffectiveQueryLow` and split `[EffectiveQueryLow, HighWatermark)`
  into half-open windows of configurable max size (default 1 month).
- `ChangeDiscoveryReader`: per window, issues
  `WHERE WindfarmId=@w AND LastUpdatedDate>=@low AND LastUpdatedDate<@end
  ORDER BY LastUpdatedDate, Id`, keyset-paginated using
  `(LastUpdatedDate, Id)` as the continuation key (never OFFSET). Spills
  distinct affected `ProductionDate`s to bounded temp storage (e.g. local
  disk-backed set or a scratch table) once an in-memory threshold is exceeded.
- Explicitly does **not** filter using the previous `LastCommittedId` inside
  the lag-overlap window.

### Source adapter interface (`Extraction/`)

```csharp
public interface IProductionSourceAdapter
{
    Task<DateTime> GetHighWatermarkAsync(string windfarmId, CancellationToken ct);

    IAsyncEnumerable<ChangedProductionRow> GetChangedRowsAsync(
        string windfarmId, DateTime low, DateTime end,
        (DateTime LastUpdatedDate, long Id)? continuationKey, CancellationToken ct);

    IAsyncEnumerable<ProductionRow> GetWindfarmDaySnapshotAsync(
        string windfarmId, DateTime dayStartUtc, DateTime dayEndUtc, CancellationToken ct);
}
```

A fake/in-memory implementation backs unit tests; a stub for the real adapter
is deferred until Open Question 2 is resolved.

### Success Criteria

**Automated**
- Unit tests: lease acquisition rejects concurrent owner; heartbeat renews;
  expired lease reacquired by a new owner with incremented `LeaseToken`.
- Unit tests: window planner produces correct half-open windows, honors max
  window size, computes `EffectiveQueryLow` correctly, does not exceed frozen
  `HighWatermark`.
- Unit tests (fixtures from ticket):
  - Equal `LastUpdatedDate` across page boundary — no row skipped or
    duplicated, verified via `(LastUpdatedDate, Id)` keyset.
  - A row committed after a prior poll but stamped inside the safety-lag
    window — is discovered on next cycle's overlap re-read.
  - A correction to a date months before the current `LastUpdatedDate` — is
    correctly identified as an old affected `ProductionDate`.
  - Large changed-date set spills to bounded temp storage without unbounded
    memory growth (integration test with a fake adapter emitting >10k rows).
- Integration test: full extract-without-publish run against a fake adapter
  seeded with 1,500 WTGs × 144 intervals for one day; assert affected-date
  derivation and day read-back row counts match expected 216,000 rows,
  without any Blob or SQS calls occurring (mocks assert zero invocations).

**Manual**
- Confirm with source-system owner that windfarm+changed-time and
  windfarm+UTC-day indexes exist and perform acceptably (Open Question 2)
  before writing the real adapter.

---

## Phase 2 — Raw writer, path/shard logic, queue send-before-commit ordering

### Overview
Add the immutable Raw Parquet writer, the `raw-fanout` publisher, and enforce
mandatory sequencing, without yet wiring the fenced cursor commit (delivery
slice 2, minus final commit which lands in Phase 3 for testability).

### Raw path convention

```text
/raw/production/windfarm=<windfarmId>/date=<UTC-yyyy-MM-dd>/extraction=<leaseToken>/production.parquet
```

Sharded variant when a day's artifact exceeds the configured size limit:

```text
/raw/production/windfarm=<w>/date=<d>/extraction=<token>/production-shard-0001.parquet
/raw/production/windfarm=<w>/date=<d>/extraction=<token>/production-shard-0002.parquet
```

Shard count/boundaries are deterministic per run (e.g. split by a stable
ordering key such as `ItemId`) so a retry before cursor advance rewrites
identical shard contents at identical paths (idempotent retry requirement,
§4.9 rule 2).

### Raw schema (Parquet columns)

```text
WindfarmId              string
ItemId                  string
ProductionTimestampUtc  timestamp
ActualProduction        double (unit: kW or MW — confirm with source, Open Q2)
PossibleProduction      double
<additional agreed measurement columns, unit-suffixed>
LastUpdatedDate         timestamp   (source change timestamp)
Id                      long        (source row id)
SourceRowRevision       string/long nullable
ExtractionTimeUtc       timestamp
SchemaVersion           int
ExtractionToken         long
```

Validation/quarantine at write time (not full Harmonizer validation, only
extraction-time reject-with-reason):
- Malformed/missing `ProductionTimestampUtc` → quarantine row, log reason
  tied to `(ItemId, ProductionDate)`, continue with remaining rows.
- Duplicate contradictory keys (`ItemId + ProductionTimestampUtc` colliding
  with different values, no clear "latest" signal) → quarantine both,
  record reason.
- Invalid/unexpected units → quarantine, record reason.
- Quarantined rows are written to a sibling quarantine artifact (e.g.
  `.../production-quarantine.parquet`) rather than silently dropped, so the
  reason is auditable.

### Writer/publisher components (`Raw/`, `Messaging/`)

- `ProductionParquetWriter`: streams rows per affected day into size-bounded
  Parquet row groups, tracks in-memory row count against a configured bound,
  decides shard boundaries deterministically, uploads via `IRawObjectStore`,
  then verifies count/schema/checksum before treating the write as durable.
- `SqsRawArtifactPublisher` (implements `IRawArtifactPublisher`): sends one
  SQS message per artifact (or one message referencing a shard-set as one
  logical day — message contract below), with bounded in-flight sends
  (semaphore).

### `raw-fanout` SQS message contract

```json
{
  "sourceType": "Production",
  "windfarmId": "WF01",
  "dataDate": "2026-06-03",
  "extractionToken": 41,
  "blobPath": "/raw/production/windfarm=WF01/date=2026-06-03/extraction=41/production.parquet",
  "shardPaths": null,
  "runId": "b3c1...-guid",
  "traceId": "w3c-traceparent-or-correlation-id"
}
```

`shardPaths` (array, nullable) populated instead of/alongside `blobPath` when a
day is split; downstream fan-out must treat all shards as one logical day.

### Mandatory sequencing (enforced in an orchestrator, not left to caller discipline)

```text
per affected day:
  write Raw (Blob) → confirm durable → SendMessage(raw-fanout) → confirm accepted
only after ALL days in the window succeed the above:
  (Phase 3) fenced cursor advance
```

Implement as a single `ProductionIngestionRunner` (`Execution/`) method
sequence with explicit early-return/exception on any step failure, so
partial-window failure never proceeds to commit.

### Success Criteria

**Automated**
- Unit tests: shard boundary logic is deterministic given the same input set
  (retry produces identical shard assignment).
- Unit tests: quarantine logic isolates malformed timestamp, duplicate
  contradictory key, and invalid unit cases with correct reason codes,
  without failing the rest of the day's write.
- Integration test: full-day write for 1,500 WTGs × 144 intervals produces
  expected row count, row-group sizes within configured bound, and (if over
  the size threshold in test config) correct deterministic shard split.
- Integration test (fake Blob + fake SQS): write success + send success is
  recorded before any commit call is invoked; write failure prevents SQS
  send; send failure leaves artifact durable but no cursor advance call
  occurs (verified via mock — commit not yet wired, so assert the runner
  stops before that stage).
- Integration test: empty affected day (all rows corrected to nothing) still
  produces a snapshot artifact (zero data rows) and a fan-out message — not
  silently skipped.

**Manual**
- Confirm target Blob storage account/container and unit conventions with
  the user (Open Questions 2, 6) before wiring real Blob/SQS clients (as
  opposed to fakes used in this phase's tests).

---

## Phase 3 — Fenced cursor commit, full run orchestration, failure/zombie handling

### Overview
Wire the fenced final commit and complete the end-to-end run: acquire lease →
read cursor → discover → read-back → write Raw → publish → fenced commit →
release lease, including multi-day/multi-window commit rules and zombie-lease
handling.

### Fenced commit

```sql
UPDATE extraction_cursor
   SET last_updated_date = @newLastUpdatedDate,
       last_committed_id = @newLastCommittedId,
       last_run_completed_utc = now(),
       consecutive_failures = 0
 WHERE windfarm_id = @windfarmId
   AND source_type = @sourceType
   AND lease_token = @leaseToken
RETURNING lease_token;
-- zero rows returned => zombie: discard run, do not retry, do not release "lease"
--                        (it's no longer this run's lease to release)
```

### Multi-day / multi-window commit rule

- Within one window, require every affected day's Raw write + SQS send to
  succeed before persisting that window's boundary as the new committed
  position.
- If day 3 of 5 in a window fails, persist only the boundary as of the last
  fully handed-off day (i.e., do not advance past day 2's `(LastUpdatedDate,
  Id)` watermark), and stop the run — do not attempt day 4/5.
- Never jump directly to the frozen `HighWatermark`; the committed position
  is always the most recent fully-succeeded boundary.
- No-change window (zero changed rows): still allowed to advance the
  verified horizon; log a distinct "zero changed rows" metric separate from
  "zero production rows" (an empty snapshot day).

### Release lease

- On success: clear `lease_owner`/`lease_expires_at` (or leave lease to expire
  naturally — pick one; releasing immediately reduces contention for
  same-windfarm backfill runs).
- On zombie detection (fenced commit returns zero rows): do not touch the
  lease row at all (it belongs to the superseding owner); just discard local
  run state and exit/log.

### Failure handling

| Scenario | Behavior |
|---|---|
| Blob write fails | Cursor unadvanced; retry same token while lease valid, or new token after lease expiry/takeover |
| SQS send fails | Same as above |
| Send succeeds, crash before commit | Cursor unadvanced; next run re-derives and rewrites under a new token; downstream absorbs duplicate as content-hash no-op |
| Fenced commit returns zero rows | Zombie discards run; no retry of the commit itself |
| Lease acquisition fails (already held) | No-op this cycle, back off, retry later |

### Success Criteria

**Automated**
- Unit test: fenced commit with correct token succeeds and updates all
  fields; fenced commit with stale/superseded token affects 0 rows and code
  path discards the run without retrying the commit.
- Integration test (recovery-matrix fixtures from ticket):
  - Pod dies before lease acquired → next cycle proceeds normally (no state
    corruption, no error).
  - Pod dies after Raw write, before SQS send → cursor unadvanced; simulated
    next run re-derives and rewrites same day under new token.
  - Pod dies after SQS send, before cursor advance → simulated replay is
    absorbed (test asserts idempotent path is exercised, i.e., new run
    re-sends without error).
  - Lease expires mid-run (simulate concurrent takeover) → original run's
    fenced commit returns 0 rows affected → run discards, does not throw
    an unhandled retry loop.
  - Multi-day window, one SQS send rejected after earlier days succeeded →
    cursor persists only up to the last fully-succeeded day's boundary, not
    the frozen high watermark.
  - Stalled worker losing its lease mid-run → detected at commit time via
    fencing, not via a separate heartbeat-timeout exception (belt-and-braces:
    both should be tested if heartbeat renewal exists).
- Load test: full run across bounded catch-up windows spanning >1 month of
  simulated backlog completes, producing the expected number of windows and
  per-window commits.

**Manual**
- Verify via logs/metrics that a manually-triggered zombie scenario (kill
  pod mid-run, let lease expire, let a second instance run) never double-
  commits or loses data, using a real (or realistic staging) SQL instance.

---

## Phase 4 — Historical refresh (`RefreshAndRecalculate` / production refresh)

### Overview
Add the backfill/refresh path (§15.1, §16.3): re-read explicitly requested
UTC Windfarm-Days regardless of `LastUpdatedDate`, using a new token, isolated
concurrency cap, preserving old Raw revisions.

### Design

- `RefreshRequest`: `{ WindfarmId, SourceType=Production, DataDates: [...],
  RequestedBy, Reason, RunId }` — trigger mechanism TBD (Open Question 9);
  design the worker to consume from an abstraction (`IRefreshRequestSource`)
  so the actual trigger (API call, internal queue, CLI) can be swapped in.
- `ProductionRefreshPlanner` (`Refresh/`) validates the requested
  windfarm/date range, splits it into Windfarm-Day work items, and passes
  them to `ProductionIngestionRunner` in `IngestionMode.Refresh`. The runner
  reuses `ProductionParquetWriter` and `SqsRawArtifactPublisher` from Phase 2
  directly (same write/publish path, same idempotent shard logic) — only the
  discovery step is bypassed (no `LastUpdatedDate` filter; each requested
  date is read via the day-snapshot query unconditionally).
- Reuses the same lease mechanism, but against a **separate concurrency
  semaphore/queue** so backfill work never starves normal-cadence extraction
  (a configurable `BackfillConcurrencyLimit` distinct from
  `NormalCadenceConcurrencyLimit`).
- Does **not** advance `ExtractionCursor.LastUpdatedDate/LastCommittedId` for
  requested dates that are older than the current committed position — the
  refresh's own token/run tracking is independent of the incremental cursor
  (confirm: refresh should not regress or skip-ahead the incremental cursor;
  needs explicit code-level guard test).
- Pure calculation re-execution (`ForceRecalculate` mode) must never reach
  this pod at all — enforced by only wiring `RefreshAndRecalculate` requests
  to this worker; `ForceRecalculate` is entirely downstream (Calculation
  Zone) and out of scope here.

### Success Criteria

**Automated**
- Unit test: refresh worker reads a requested date even when no
  `LastUpdatedDate` change exists for it (mock adapter returns identical
  snapshot; test asserts day-snapshot query invoked unconditionally).
- Unit test: refresh writes under a brand-new `extraction=<token>` distinct
  from any prior token for that day; old Raw path/object is untouched
  (fake Blob store asserts old key still present and unmodified).
- Unit test: refresh concurrency respects `BackfillConcurrencyLimit`
  independent of normal-cadence semaphore (concurrent normal + backfill runs
  for same windfarm do not exceed combined expected concurrency, and do not
  deadlock on the lease).
- Integration test: refresh run publishes fan-out messages for each
  requested date and does not regress the incremental `ExtractionCursor`
  position.

**Manual**
- Confirm refresh trigger mechanism and `BackfillConcurrencyLimit` default
  with the user (Open Question 9).

---

## Phase 5 — Monitoring: Source Availability & Data Availability

### Overview
Emit the two distinct monitoring views required by §14.7.

### Data model

```sql
CREATE TABLE source_availability (
    source_system                    VARCHAR(64)   NOT NULL,
    source_type                      VARCHAR(32)   NOT NULL,
    last_successful_contact_utc      TIMESTAMPTZ   NULL,
    last_successful_data_utc         TIMESTAMPTZ   NULL,
    committed_cursor_value           TIMESTAMPTZ   NULL,
    committed_cursor_advanced_at_utc TIMESTAMPTZ   NULL,
    status                           VARCHAR(16)   NOT NULL, -- Healthy|Degraded|Unavailable|Unknown
    consecutive_failures             INT           NOT NULL DEFAULT 0,
    last_error                       VARCHAR(2000) NULL,
    checked_at_utc                   TIMESTAMPTZ   NOT NULL,
    CONSTRAINT pk_source_availability PRIMARY KEY (source_system, source_type)
);

CREATE TABLE data_availability (
    dataset               VARCHAR(64)  NOT NULL,   -- 'Production'
    windfarm_id            VARCHAR(64)  NOT NULL,
    wtg_id                 VARCHAR(64)  NOT NULL,
    data_date              DATE         NOT NULL,
    expected_count          INT         NOT NULL,   -- ~144 for 10-min production
    actual_count            INT         NOT NULL,
    latest_timestamp_utc    TIMESTAMPTZ NULL,
    status                  VARCHAR(16) NOT NULL,   -- Expected|Partial|Complete|Late|Missing|Invalid
    source_revision         VARCHAR(64) NULL,
    checked_at_utc          TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_data_availability PRIMARY KEY (dataset, windfarm_id, wtg_id, data_date)
);
```

### Emission points

- `SourceAvailability` updated: on every source contact attempt (contact
  success/failure timestamp + status), and separately whenever the cursor
  actually advances (`CommittedCursorAdvancedAtUtc`) — a healthy source with
  a stalled cursor must be independently detectable (distinct alert
  condition per requirement).
- `DataAvailability` updated per WTG-day after a Raw write, computed from the
  actual row count read back per `ItemId` for that day vs. `ExpectedCount`
  (144, configurable per source cadence). Missing/partial WTG-days are
  recorded as `Partial`/`Missing`/`Invalid` — never auto-filled with zero.

### Success Criteria

**Automated**
- Unit test: successful contact + zero changed rows updates
  `LastSuccessfulContactUtc` but not `CommittedCursorAdvancedAtUtc` beyond
  the no-change-window advance rule from Phase 3.
- Unit test: source contact failure increments `ConsecutiveFailures` and
  transitions `Status` per configured thresholds (e.g. 3 consecutive
  failures → `Degraded`, N → `Unavailable`).
- Unit test: a full 144-row day yields `DataAvailability.Status = Complete`;
  a partial day yields `Partial` with correct `ActualCount`; a day with zero
  rows for an otherwise-expected WTG yields `Missing`, not silently absent.
- Integration test: "healthy source, stalled cursor" scenario (source
  contact always succeeds, but no changed rows ever satisfy a window) is
  distinguishable in `SourceAvailability` from "source unreachable".

**Manual**
- Confirm alert thresholds (consecutive-failure counts, stalled-cursor
  duration) with the user/ops team; none are specified in the source docs.

---

## Testing Strategy

- **Unit tests**: pure logic — window planning, keyset pagination, shard
  determinism, quarantine rules, fencing predicate logic, availability status
  transitions. Use in-memory/fake adapters, fake Blob store, fake SQS client,
  and a real (or SQLite/LocalDB-backed) `ExtractionCursor` table for
  fencing-race tests where atomicity matters.
- **Integration tests**: exercise the full `ProductionIngestionRunner`
  sequence against fakes for Blob/SQS but a real SQL instance (test
  container) for `ExtractionCursor`/`SourceAvailability`/`DataAvailability`,
  to validate the fenced-commit SQL itself, not just mocked behavior. Cover,
  at minimum, lease takeover, stale-token rejection, partial queue-send
  failure, and crash recovery — these behaviors matter more than exhaustive
  per-class coverage.
- **Load test**: 1,500 WTGs × 144 intervals full-day fixture, run through
  discovery + read-back + write + publish, asserting bounded memory
  (streaming, not full materialization) and row-group size limits.
- **Fixture matrix** (from the ticket, mapped to phases above): equal
  `LastUpdatedDate` across pages (Phase 1); late-visible safety-lag row
  (Phase 1); correction months before current `LastUpdatedDate` (Phase 1);
  full-scale load (Phase 1/2); empty/partial day (Phase 2); stalled
  worker/lost lease (Phase 3); one SQS send rejected mid-window (Phase 2/3).
- All phases' automated criteria are runnable via `dotnet test` and are
  wired into the GitHub Actions `ci.yml` workflow added in Phase 0.

## References

- [01-production-ingester.md](../../../01-production-ingester.md)
- [Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md](../../../Final_Distributed_Loss_Calculation_Architecture_Plan_v11.md)
  §§4.1–4.9 (extraction/Raw), 5.1–5.12 (fan-out contract, recovery matrix),
  6.4/6.9 (Harmonized storage & Windfarm-Day aggregate context), 8.6
  (`ItemDayInputState` — downstream consumer of this pod's output), 14.7
  (monitoring), 15.1 (production backfill), 16.3 (production refresh)
