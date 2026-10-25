# ADR-0006 — Company catalogue PostgreSQL for control and latest serving

**Recorded:** 2026-10-11 (Asia/Kuala_Lumpur). **Evidence:** user-confirmed company constraints plus saved platform specifications; public technical references checked on this date. No live repository, service catalogue, company Kafka configuration or deployed infrastructure was inspected. Proposed rationale is not company approval or performance evidence.

## Status

**Company product constraint confirmed by the user.** Application roles reflect existing discussed decisions; detailed catalogue capabilities/provisioning/deployment validation remain pending. This is not a freely chosen PostgreSQL-versus-Azure-SQL benchmark result.

## Context

PostgreSQL is the company's chosen database product. The team must obtain it through the product catalogue and cannot create its own PostgreSQL instance. The company is moving application services away from Azure towards AWS. Exact product name, engine version, RDS/Aurora backing implementation, service tiers and approved features were not supplied in this clarification.

Historical work already references cross-account EKS/RDS access and approved infrastructure workflows, but it does not establish every catalogue product setting. Use returned product documentation/configuration instead of inferring an engine edition or privileges.

## Recorded decision

Use the company catalogue PostgreSQL service for platform transactional/control state and current/latest serving results. Preserve the separation from immutable lake artifacts and historical query selection. Do not independently provision an unmanaged database or select an unsupported PostgreSQL distribution to obtain extra privileges/extensions.

The primary rationale is company-standard service consumption, supported provisioning/ownership and AWS direction. PostgreSQL application suitability is demonstrated by implementation/migration validation, not by asserting it is inherently faster, cheaper or better than Azure SQL.

## Responsibility and schema

| Application responsibility | Catalogue/provider responsibility to confirm |
|---|---|
| Control tables, business schema, migrations, query/index design | Product provisioning, supported engine/features and access process |
| Lease/fence/outbox correctness and idempotency | Service availability, backups/restore and patch responsibilities per real contract |
| Latest serving transaction/pointer recovery | Supported HA/tier/capacity policy and maintenance behavior |
| Credentials/connection pooling/retry integration | Approved authentication/network and runtime role permissions |
| Retention of application histories/lineage | Product retention guarantees, recovery options and customer actions |

The right column is a contract checklist, not an assertion that every duty is already handled automatically. No environment column: independent deployment databases remain DL-012.

Saved 0003/0004 control schema uses PostgreSQL tables/functions for Fan-Out/Harmonizer ownership, receipts/outbox and committed state. Supplied Adjustment package uses explicit complete aggregate revisions rather than claiming PostgreSQL duplicates SQL Server system-versioned parent/child history. Current/latest serving tables remain an application contract requiring actual Publisher implementation.

## Alternatives

| Alternative | Reason / consequence |
|---|---|
| Continue Azure SQL as long-term platform target | Conflicts with stated move and catalogue-selected product unless an approved exception is recorded |
| Transitional Azure SQL source/authoring dependency | Existing Adjustment/source cutover can require transition; record exit/identity/history requirements instead of deleting support prematurely |
| Self-created PostgreSQL/RDS/Aurora | Outside the confirmed product catalogue provisioning constraint |
| Different catalogue database | Would require an actual approved alternative and migration rationale; none supplied |
| Warehouse as all transactional state | Different transaction/workload/provisioning design; not justified by analytical use alone |

This is not a decision to migrate every upstream source database controlled by another service. Application-owned dependencies and consumed service-owned implementations have different responsibilities, as ADR-0001 explains for Data Lake.

## Migration and implementation consequences

Port provider-specific queries and schema behavior deliberately: SQL types, UTC handling, identity sequences, transaction isolation, concurrency, upsert patterns, temporal/history behavior and permissions. Use current PostgreSQL migration tooling and forward migrations for deployed schemas; historical DACPAC practices do not prove PostgreSQL deployment support.

Catalogue permissions may limit DDL, extensions and admin operations. Confirm migration/runtime roles before generating changes that require unsupported privileges. Workers must not create the product or execute startup DDL. Align connection pools/retries with real failover and authentication behavior.

Adjustment migration preserves numeric IDs, UUIDs/revisions and full membership history where captured, retains legacy parent periods separately and runs one authoritative writer at cutover (DL-024). Current SQL plus file publication remains a recovery protocol rather than a distributed transaction guaranteed by PostgreSQL.

## Acceptance and review trigger

Obtain actual product identifier, engine/version, provisioning route, allowed extensions, migrations/roles, authentication, network, connection limits, backup/restore and service ownership. Validate representative control transactions, concurrent leases/outbox claims, failure/restart and serving publication with supported permissions. Prove identity/history preservation for actual SQL Server/Azure SQL cutover data.

Review if the company product contract changes or the application cannot meet supported transactional/serving needs. Such evidence may justify another approved product, not silent self-provisioning.

## Related records

ADR-0001/0004/0005, DL-008, DL-012, DL-019, DL-024, DL-025, Q-019 and [Adjustment](../discovery/loss-calculation/adjustment.md). Company selection/provisioning facts are user-confirmed internal context; no public product-page claim substitutes for the company's actual catalogue contract.
