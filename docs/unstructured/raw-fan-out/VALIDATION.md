# Validation status

## Executed in the authoring environment

- Checked all project and package XML, project-reference paths and migration-copy paths.
- Checked every project explicitly targets net10.0, independently of Directory.Build.props discovery.
- Checked the four user-specified dependency pins and absence of old Parquet ReadColumnAsync/WriteColumnAsync and NoopTokenCredential references.
- Reviewed supplied Parquet.Net 6.1 reader/writer XML signatures; incorporated async reader/writer disposal and buffer-based APIs.
- Reviewed durable-plan ordering, artifact-row serialization, idempotent acknowledgement and rollback boundaries.
- Validated JSON examples/configuration, ZIP integrity and file hashes.

## NOT executed here

The .NET SDK, Docker and PostgreSQL executables are unavailable. Restore, C# compilation, SQL execution and all .NET tests remain unexecuted. Static inspection is not a substitute for those gates. Do not treat the source package as a verified production build.

## Included automated tests

Unit tests cover envelope validation, Production dates, identity sorting/deduplication, adjustment parents with no current members, shard-read failure, limits, cancellation, batch-send retry/response integrity, heartbeat/delete behavior, DLQ handling, artifact completion ordering and database-before-publication ordering.

Local integration tests write/read actual Parquet files using the pinned library, including production timestamps, int64 alarms, event tombstones/shards and adjustment parents without ItemId. Fixtures exercise identity columns, not every producer encoding or membership representation.

Opt-in external tests cover PostgreSQL failure-store transactions, dispatch-before-ack and ack-before-dispatch, partial acknowledgement, unknown-child rejection, immutable envelope conflicts, concurrent replica plan selection, invalid-plan rollback, rollback of acknowledgement, and the Blob/Parquet/SQS pipeline with a real PostgreSQL artifact store.

External tests are skipped unless RAWFANOUT_INTEGRATION=1. See README commands. All PostgreSQL test DDL is restricted to the isolated localhost rawfanout_test database. No supplied test connects to company services.

Before rollout, run restore/build/all tests using the approved feed and .NET 10 SDK; then exercise sanitized fixtures from each real ingestor. Confirm downstream acknowledgement integration and the existing producer all-shards-before-message guarantee before enabling readiness checks. EKS federation, cross-account RDS IAM, CA/TLS and live queue redrive require your environment-specific validation.
