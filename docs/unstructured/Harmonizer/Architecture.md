# Harmonizer architecture

The solution keeps `Harmonizer.Worker -> Harmonizer.Core -> Messaging.Contracts`. No additional executable or domain assembly is introduced. Worker owns the composition root, queue lifetime, lifecycle lifetime and telemetry configuration. Core owns source use cases, provider ports, pure rules and provider adapters.

## Source organization

| Folder | Role |
|---|---|
| Core/SourceTypes/Production/{Models,Interfaces,Adapters,Repositories,Serialization,Operations} | Existing Production V2/V3 implementation |
| Core/SourceTypes/Outage/{Models,Interfaces,Adapters,Repositories,Serialization,Operations} | Outage mapping, canonical storage, lifecycle and daily output |
| Core/Interfaces | Shared queue/storage/notification/telemetry/source strategy ports |
| Core/Operations | Shared dispatcher and ownership heartbeat |
| Core/{Models,Serialization,Configuration,Observability,Adapters,Repositories} | Existing shared identity/configuration/provider support |
| Worker/SourceTypes/{Production,Outage} | Source-specific DI registration |
| Worker/{Hosting,DependencyInjection,Observability} | Generic hosted loops and shared clients/telemetry |

A new source adds its verified handler and registration. Inbound and lifecycle hosts depend on strategy interfaces rather than concrete source processors. Source-specific I/O ports express meaningful operations; pure rule helpers have no artificial interfaces or logging dependencies. There is no universal generic repository, service locator or exporter dependency in business operations.

Models use sealed reference records/classes. Numeric IDs, DateOnly, DateTime and counters remain appropriate CLR value types. Immutable candidate paths, explicit nullable ends, read-only collections, deterministic identity, bounded work and cancellation make state transitions reviewable.

## Production

SQS work is strictly validated; V2 names/scope IDs resolve through the authoritative catalog and V3 supplies explicit identities. Both create ProductionWork while retaining the original frozen body. The existing per-WTG-day lease, keyed snapshot merge, 144-slot completeness, immutable output, failure pinning and atomic state/receipt/outbox/final Fan-Out acknowledgment remain intact.

## Outage

EventWorkV3 selects event/member input; it never supplies final affected dates. Harmonizer merges canonical event state, publishes changed months and resumable old/new footprint plans atomically, and hands accepted SQS work to its durable lifecycle loop. Daily pointers, outbox intents and progress commit together. Final Fan-Out materialization acknowledgment occurs only when frozen targets are complete. Event history and row data remain in files; two control tables retain only current pointers/ownership/progress.

The farm/source publication lease intentionally serializes Outage work within a farm, including the bounded day pass. Different farms can run in parallel. This implementation chooses simple serialized publication instead of a more complicated optimistic catalog-generation/advisory-lock protocol. Database fencing and immutable attempt paths reject expired owners; publication cannot race a new overlapping event in the same farm/source.

`Outage.md` describes the exact schema, paths, linkage, snapshot semantics, SQL readiness integration, telemetry and HPA behavior. `ReplaceFiles.md` lists all additions/replacements against the prior saved project.

## External boundaries

IRawObjectStore and IHarmonizedArtifactStore remain separate. Blob clients are created independently with their configured credential chains; no Raw credential fallback is used for output. SQS and notification publisher ports stay shared. Npgsql is confined to repository/provider composition classes. Runtime code never executes DDL. The transactional outbox is a PostgreSQL table, not another SQS queue.

Harmonizer emits proposed coordination notifications, not CalculationWork or package manifests. Package Coordinator, Calculation, Summary and Serving publication remain separate. The package consumer must recheck current readiness/generations and deduplicate notifications because SQS publishing is at least once.

## Telemetry

The shared ITelemetry adapter supplies structured logging. One host configuration produces UTC timestamped JSON console logs and OpenTelemetry logs/traces/metrics. Necessary I/O/orchestration classes log reception, validation, selection, publication, progress, completion, ownership and failure. Pure mapping/interval/range/merge functions do not log; their calling use cases record outcomes. Operation/execution IDs flow through scopes, origins, plans, database receipts and notifications. Autonomous clock work preserves originating evidence without reusing a completed source operation as its active root.
