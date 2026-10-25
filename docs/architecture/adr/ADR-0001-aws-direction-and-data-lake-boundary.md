# ADR-0001 — AWS direction and the Data Lake service boundary

**Recorded:** 2026-10-11 (Asia/Kuala_Lumpur). **Evidence:** user-confirmed company constraints plus saved platform specifications; public technical references checked on this date. No live repository, service catalogue, company Kafka configuration or deployed infrastructure was inspected. Proposed rationale is not company approval or performance evidence.

## Status and scope

Company constraints confirmed by the user; application boundary/migration implementation proposed for review. This ADR records reported company direction, not a newly issued corporate policy.

## Context

The company is moving application services away from Azure towards AWS. Its existing Kafka service is encouraged for cloud-agnostic integration. The platform is a customer of a company Data Lake service: today that service uses Azure Blob, and its backing storage is expected to move to AWS in **2027**. The exact destination product, endpoint, cutover date and access protocol were not supplied.

PostgreSQL is a company-selected product available only through its product catalogue. The application team cannot provision an arbitrary PostgreSQL instance independently.

AWS direction and cloud-agnostic shared services coexist. AWS direction alone does not mandate choosing an AWS-specific implementation for every integration.

## Recorded constraint and proposed application decision

Use company-supported AWS-aligned application services and consume the Data Lake through its supported contract. Keep storage-specific access outside business processing. The Data Lake provider owns backing-store migration; this application owns verifying that its reads, writes, references, publication/replay and queries continue to work.

Avoid making domain handlers responsible for copying Blob data to S3 or choosing migration tooling. Do not create an application-owned replacement lake solely because the company service currently uses Azure.

This does not promise zero code/configuration changes. If the service exposes only physical Azure locations/SDKs today, the current storage adapters are an exposed dependency that must be aligned with the service's future access model.

## Ownership and contract

| Concern | Data Lake service/provider | Loss Calculation application |
|---|---|---|
| Storage migration | Backing store, copy/cutover and supported access contract | Validate customer compatibility and agreed cutover behavior |
| Artifact identity | Define reference stability/resolution guarantees | Preserve exact Raw/canonical/lineage references and replay meaning |
| Credentials/network | Publish approved access model | Configure catalogue/workload credentials in adapters |
| Data/schema | Support storage/delivery guarantees | Own business schemas, revisions, merge rules and source validation |
| Analytics | Explain supported query access/integrations | Select/validate provider through ADR-0005 |
| Publication | Define write/read/conditional operation behavior | Own fenced committed pointers and receipt/outbox consistency |

A stable logical artifact reference plus service-owned physical resolution is the proposed preferred contract. It is not a claim the service already provides such identifiers. Existing persisted `parquet_path`/manifest pointers must be examined: if they embed cloud URLs, agree a mapping or migration before cutover, including historical references and queued messages.

## Alternatives

| Alternative | Assessment |
|---|---|
| Business handlers call Azure SDK/URLs directly | Current provider detail leaks into application behavior; larger migration impact |
| Application copies/migrates the lake itself | Duplicates the service provider's stated responsibility; needs a separate requirement |
| Service boundary with narrow storage adapters | Preferred direction; conditional on real service contract and identity guarantees |
| Assume every underlying engine migrates automatically | Rejected assumption; an analytics engine may need a different provider/backend |

## Consequences and checks

Current Azure Blob provider implementations can remain transitional behind the boundary. Separate Raw reads and Harmonized reads/writes remain useful. Do not rename Raw/Harmonized business folders just because physical cloud storage changes.

Before integration, obtain the actual customer contract: read/write/list or query capabilities; stable references; immutable bytes; retry/conditional writes; read-after-write and cutover behavior; permissions; format/version compatibility; access for server-side engines. These are questions, not invented guarantees.

Prove that pre-migration references, pending outbox/batch bodies, historical artifacts and committed snapshots resolve after migration. Verify failed writes do not publish pointers and provider/config changes do not select uncommitted revisions. A transparent business API is the objective; storage/access compatibility is evidence required to achieve it.

## Links and review trigger

[Query abstraction](ADR-0005-query-engine-abstraction.md), [Parquet](ADR-0004-parquet-artifact-storage.md), [PostgreSQL](ADR-0006-catalogue-postgresql.md), DL-025 and Q-017 in the [Decision Log](../decision-log.md).

Review when the Data Lake publishes its 2027 migration/customer contract or changes how customers obtain analytics access. Company direction and timing are user-provided internal context, not publicly verified facts.
