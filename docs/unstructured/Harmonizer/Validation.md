# Validation report — 6 October 2026 UTC

Fresh validation in the monorepo used .NET SDK 10.0.2 on Windows. Both test projects restored in locked mode. The Release Worker build and unit-test build succeeded. The build emitted one inherited CS8618 warning in the unchanged `Messaging.Contracts/RawArtifactAvailableV1.cs` `WindfarmName` property; Harmonizer projects emitted no warnings.

| Check | Result |
|---|---|
| Locked restore: UnitTests and IntegrationTests | Passed |
| Release Worker build | Passed; one inherited shared-contract warning |
| Release Harmonizer.UnitTests | 155 passed, 0 failed, 0 skipped |
| Release Harmonizer.IntegrationTests | 18 skipped, 0 executed, 0 failed |

Current TRX reports are `validation/Harmonizer.UnitTests.trx` and `validation/Harmonizer.IntegrationTests.trx`. The Npgsql tests were skipped because `HARMONIZER_TEST_POSTGRES` was unset. This is not a passing integration result; no database was contacted or modified.

## Verified behavior

- Existing Production V2/V3 validation, identity resolution, merge, 144-slot UTC coverage, correlation, separate storage routing and Parquet round trips remain covered by the unit suite.
- Outage tests exercise the supplied Parquet fixtures, separate Raw/output stores, canonical/month pointers, typed WTG-day links, null-ended events, moves, deletions, bounded lifecycle continuation, retries, expected-child barriers, scoped snapshots and empty incremental observations.
- Snapshot fence regression cases verify that an in-scope source change strictly older than, or exactly equal to, the committed authoritative cutoff is rejected. A newer source change remains applicable.
- The Raw Fan-Out builder emits `Authoritative` only when a full snapshot has a declared roster, expected row count and explicit `SourceVersionCutoffUtc`; otherwise the barrier is `ReceiptOnly`. The Outage manifest observation timestamp is not used as a source cutoff.

## Not rerun here

The prior standalone delivery reported an embedded PGlite/PostgreSQL SQL smoke harness over 25 repository statements. That harness and its output were not available or rerun in this monorepo validation, so those claims are historical delivery evidence, not a current verified check. The test project compile does not establish SQL behavior, Npgsql transport behavior or concurrent fencing.

Set `HARMONIZER_TEST_POSTGRES` to a disposable local PostgreSQL instance and run the integration project before deployment. Never use an application, development, or production database for these schema-creating tests.

Live SQS, Azure Blob, RDS IAM, Kubernetes/HPA, Docker, sustained load, Package Coordinator integration and target-environment permissions were not exercised. No database migration was applied and no deployment was performed. Alarm/Adjustment/Curtailment mappings and Production destructive refresh remain outside this change; see [ImplementationStatus.md](ImplementationStatus.md).
