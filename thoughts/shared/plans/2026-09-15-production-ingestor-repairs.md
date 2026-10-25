# Production ingestor review repairs

Approved by the user on 2026-09-15, including preservation of SQL decimal precision.

## Architecture contract

Keep API -> Core, Worker -> Core, and Core -> Messaging.Contracts. Core owns
ingestion, provider implementations, and processing models. The Worker performs
one bounded request and exits; the API schedules it. Keep existing package
versions, database schema, and external message field names. Align interfaces
and models with their existing folders and retain the Model naming suffix.

## Implementation sequence

- [x] Align Interfaces and Models namespaces and update all consumers.
- [x] Use source composite paging keys, complete page iteration, nullable UTC
      watermarks, and decimal measurement values without timestamp surrogate IDs.
- [x] Write genuine Parquet with decimal values, separate row groups from shards,
      and verify uploaded bytes. Validate numeric and required configuration.
- [x] Commit only fully handed-off windows, renew leases, cancel on lost ownership,
      release in cleanup, and preserve cancellation instead of reporting success.
- [x] Parse incremental/refresh requests in the Worker, invoke one run, and return
      meaningful exit codes. Keep HTTP/message field names compatible.
- [x] Add regression tests in the existing unit and integration projects and
      build Core, Worker, and API.

## Behavior and verification

Flow: request -> validate -> acquire/renew lease -> bounded discovery -> full-day
snapshot -> decimal Parquet -> durable upload verification -> SQS acceptance ->
fenced window commit. Refresh preserves the incremental cursor. Failure before
all days are handed off leaves that window uncommitted; replay can duplicate
messages and must remain safe for downstream consumers.

Use the documented source key (windfarm, turbine, UTC production timestamp) with
LastUpdatedUtc for change paging. Preserve the database's legacy numeric cursor
column for compatibility, but do not use it as a fictitious source row ID.

Unit checks cover request validation, UTC and decimal models, ordered paging
boundaries, failed-window replay, cancellation, and lease loss. Local integration
checks exercise real Parquet read/write and host wiring without cloud credentials.
Live SQL Server/PostgreSQL/Blob/SQS semantics and source uniqueness/indexes require
the configured environments and must be reported separately if unavailable.

Commands: dotnet test for both existing ProductionIngestor test projects and
dotnet build for the Worker and API, initially using --no-restore.

## Verified results

- Unit tests: 24 passed, including decimals, request parsing, missing watermark,
      failed-window cursor behavior, refresh, cancellation, mid-publication lease loss,
      source deduplication/bounds, window boundaries, and status JSON field names.
- Local Parquet integration tests: 5 passed, including decimal/timestamp round-trip,
      multiple row groups, empty files, row-limited shards, quarantine, and corrupted uploads.
- Worker/Core and API builds succeeded. Worker process smoke test with invalid
      ParquetRowGroupSize returned configuration exit code 2 before provider work.
- Editor diagnostics and git diff --check were clean.
- Live source SQL paging, PostgreSQL fencing, Blob/SQS/Kubernetes integration and
      load testing were not executed. No provider deployment or database migration occurred.

The former discovery spill placeholder was replaced with an explicit fail-fast
date limit, and retained snapshot rows are capped. This is bounded processing, not
disk-backed streaming. Additional measurement values are stored as JSON inside a
Parquet string column; the two primary measurements use decimal columns. Existing
nullable measurement semantics and external message field names are unchanged.