# ADR-0003 — EKS runtime for application processing

**Recorded:** 2026-10-11 (Asia/Kuala_Lumpur). **Evidence:** user-confirmed company constraints plus saved platform specifications; public technical references checked on this date. No live repository, service catalogue, company Kafka configuration or deployed infrastructure was inspected. Proposed rationale is not company approval or performance evidence.

## Status

Existing EKS/Core/Worker direction recorded in platform discussions; comparative rationale **Proposed**, operating cost/load and alternative execution proof not supplied. No claim that EKS is mandatory company policy or universally superior.

## Context

Production ingestion, Fan-Out, Harmonizer and loss calculation contain C# application behavior: version-aware merges, membership validation, leases/fences, retries, canonical event cleanup, durable pending plans and business calculations. Queue workers and autonomous lifecycle continuation need different execution from occasional source-copy or analytics queries.

The company is moving application services towards AWS. That makes new direct application dependency on ADF or Azure Synapse an exit-plan concern; a transitional company-provided service behind a stable boundary is a separate case. Do not use cloud direction alone to conclude Kubernetes is the only AWS runtime.

## Proposed decision

Keep business processing in testable containerized C# Core/Worker applications on the current EKS execution path. Use scheduled execution for eligible ingestion and long-running workers for queue/lifecycle work as established by each component. Keep durable state in PostgreSQL/lake and enforce ownership in application transactions; a healthy pod is not proof of business completion.

Use existing platform deployment, identity and telemetry mechanisms. Do not assume these are fully implemented simply because templates exist. Keep at least one Harmonizer instance for accepted lifecycle continuation (DL-021); scale-to-zero based solely on queue depth would miss this work.

## Compare responsibilities, not product labels

| Alternative | Role / evaluation |
|---|---|
| EKS C# workers | Current application-runtime direction; preserves domain logic and stage-specific recovery |
| ADF orchestration/copy plus custom compute | Can orchestrate pipelines and custom activities; custom code still needs an execution environment. Adds an Azure runtime dependency unless supplied transparently by another service |
| Synapse pipelines/Spark/SQL | Different execution designs; evaluate which transformations/calculations can actually move and what owns retries/canonical state |
| Redshift SQL/warehouse-centric processing | Potential analytics/set-based alternative; requires actual re-expression of business calculations, cleanup and workflow—not just replacing a worker deployment |
| Other approved AWS batch/container runtime | Credible simpler alternative for bounded jobs or workers; verify catalogue availability and long-running continuation/support fit |

ADF can run custom activity through Azure Batch. Redshift and Athena address warehouse and lake-query needs respectively; this distinction does not mean SQL cannot perform business transformations. We have no equivalence or performance evidence proving our current C# event/calculation design is cheaper or better when rewritten in a warehouse.

## Why this is a reasonable current direction

Preserve existing C# behavior and unit/integration boundaries; allow independent stage release/resource allocation; run queue reception and bounded autonomous lifecycle processing together; use the same telemetry/correlation as API/run records. These are application-fit reasons, not Kubernetes-exclusive capabilities.

A SQL/Spark rewrite should demonstrate correctness for shortening/deleting/moving events, open continuation, stale versions and partial publication before substituting it. If only a subset is simple extraction or aggregation, delegate that subset when an approved service provides a measurable advantage; avoid making an entire platform decision from one query benchmark.

## Consequences and risks

EKS entails resource sizing, workload upgrades, secrets/identity, observability and runbook ownership. Cluster availability or central operations reduces but does not eliminate team workload. Container portability does not make IAM, messaging, networking or managed databases automatically portable.

HPA changes replica count from metrics; it does not provide business fencing/idempotency. Kubernetes Jobs represent bounded execution; scheduled and always-on worker deployment selection must follow actual component behavior. Avoid creating one pod per 10-minute row or assuming autoscaling removes same-key serialization.

## Evidence and reconsideration

Measure queue/lifecycle throughput, backlog age, memory, same-farm serialization, DB connection limits, restart recovery and cost of sustained minimum replicas. Compare approved simpler runtimes with the same executable and durability requirements before expanding deployment complexity.

Reconsider if workload becomes predominantly supported SQL transformations, another catalogue runtime meets continuation/recovery with less operational burden, or EKS cost/support cannot meet delivery needs.

## References

- [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [Horizontal Pod Autoscaling](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/)
- [ADF custom activity execution](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-using-custom-activity)
- [AWS comparison of Athena and Redshift roles](https://docs.aws.amazon.com/athena/latest/ug/when-should-i-use-ate.html)

Related: ADR-0001, ADR-0005, DL-014, DL-021, DL-026.
