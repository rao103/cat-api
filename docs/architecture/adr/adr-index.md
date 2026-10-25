# Loss Calculation ADR index

**Recorded:** 2026-10-11 (Asia/Kuala_Lumpur). **Evidence:** user-confirmed company constraints plus saved platform specifications; public technical references checked on this date. No live repository, service catalogue, company Kafka configuration or deployed infrastructure was inspected. Proposed rationale is not company approval or performance evidence.

## Purpose

These six ADRs capture technology/service decisions with the company's actual constraints. They complement [Discovery specifications](../discovery/loss-calculation/README.md) and the [Decision Log](../decision-log.md). Keep implementation behavior in the specifications and rationale/alternatives here.

| ADR | Subject | Status / limits |
|---|---|---|
| [ADR-0001](ADR-0001-aws-direction-and-data-lake-boundary.md) | AWS direction; customer boundary for Data Lake migration | Company facts confirmed; service/access boundary proposed and contract unresolved |
| [ADR-0002](ADR-0002-sqs-versus-company-kafka.md) | SQS versus available company Kafka | Proposed rationale; SQS justification/preference exception unconfirmed |
| [ADR-0003](ADR-0003-eks-processing-runtime.md) | EKS C# runtime versus orchestration/warehouse alternatives | Current direction; comparative rationale/performance unverified |
| [ADR-0004](ADR-0004-parquet-artifact-storage.md) | Immutable Parquet plus current SQL serving | Discussed direction formalized as draft; publication/as-of/migration evidence still needed |
| [ADR-0005](ADR-0005-query-engine-abstraction.md) | Business query contract; Synapse/Athena/DuckDB adapters | Proposed boundary; provider selection and company lake integration unresolved |
| [ADR-0006](ADR-0006-catalogue-postgresql.md) | Catalogue PostgreSQL | Company engine/provisioning constraint confirmed; exact product/features unverified |

## Company context recorded 11 October 2026

Company Kafka already runs, is accessible by requested AD-group/topic authorization and is encouraged for cloud agnosticism. Data Lake currently uses Azure Blob and is expected to move its backing storage to AWS in 2027; this customer should consume its abstraction rather than own that migration. PostgreSQL is the company choice, provisioned only through its product catalogue.

These facts do not automatically approve SQS, require every customer to rewrite storage to S3, or establish RDS versus Aurora. Existing AWS direction and cloud-agnostic shared services must both inform the design.

## How to review

Confirm internal service contracts, then evaluate the stated functional/operational alternatives. Mark a draft accepted only after an actual decision; attach repo/test/runtime evidence separately. Do not backdate newly written rationale as an original approval.

The next concrete evidence is company Kafka consumer/retry guidance, Data Lake customer/query migration contract and the PostgreSQL catalogue product profile. DL-025 records confirmed context; DL-026 links these drafts. Q-016–Q-019 list gaps.
