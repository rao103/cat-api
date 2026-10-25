# Loss Calculation — Discovery specifications

> Updated: 2026-10-11 (Asia/Kuala_Lumpur). Status: specification baseline from saved design and source evidence; not a declaration of deployment or end-to-end validation. Accepted decisions, observed source behavior, proposed requirements and unresolved questions are identified separately.

## Purpose and location

These topic files describe the current Loss Calculation design for implementation and handoff. Keep the existing Platform Discovery as the overview and one shared Decision Log as the rationale/history. Topic specifications are part of Discovery, not a second standards hierarchy.

Repository location: `docs/architecture/discovery/loss-calculation/`. This uses the existing `docs/architecture/discovery/` root rather than creating a parallel `docs/discovery/` tree.

| Specification | Responsibility | Decisions |
|---|---|---|
| [Production source](production-source.md) | Source queries, paging, row mapping and Raw contract | DL-001, DL-003, DL-013 |
| [Outage source](outage-source.md) | Live/deleted queries, identity, refresh scope and authority | DL-003, DL-007, DL-022 |
| [Raw ingestion](raw-ingestion.md) | Cursor/run tables, lease/checkpoint, immutable Raw and ready messages | DL-002–DL-005 |
| [Raw Fan-Out](raw-fan-out.md) | Dispatch/batch/DLQ tables, frozen plans and downstream work | DL-013, DL-022 |
| [Harmonizer](harmonizer.md) | Tables, source publication, receipts and readiness | DL-014–DL-016, DL-021 |
| [Harmonizer event management](harmonizer-event-management.md) | Canonical event merge, cleanup, continuation and snapshot boundaries | DL-007, DL-014, DL-022 |
| [Operation tracking](operation-tracking.md) | Operation/run identity, stage evidence and monitoring | DL-002, DL-009, DL-011 |

Related: [Platform Discovery](../platform-discovery.md), [Decision Log](../../decision-log.md).

## Pipeline boundaries

Production produces daily-series work; Outage produces event/member work. Harmonizer owns canonical event history and affected-day materialization. Package Coordinator owns coherent packages and calculation dispatch. Publisher owns latest serving publication.

Ingestor completion, Fan-Out dispatch completion, canonical acceptance, daily materialization, package readiness, calculation completion and serving publication are different facts. Do not treat an empty queue or one stage ACK as end-to-end completion.

## Evidence and version vocabulary

Inspected for this edition: existing Platform Discovery and Decision Log; `SqlProductionSourceAdapter(1).cs`; `OutageSqlQueryBuilder.cs`; `SqlOutageSourceAdapter.cs`; saved migrations `0003_create_raw_fanout_schema.sql` and `0004_create_harmonizer_schema.sql`; source-only contents/README of `FanOut.zip` and `Harmonizer.zip` uploaded 11 October.

The archives are aligned delivery snapshots, not proof of the active Git branch. Their alignment READMEs explicitly report that .NET compilation/tests were not run in the generating environment. This documentation task did not run application tests or migrations.

| Layer | Latest aligned delivery |
|---|---|
| Raw notification envelope | `RawArtifactAvailableV2`, version 2 |
| Production / Outage payload | Schema 2; documented legacy readers retained |
| Shared refresh manifest | `RawSnapshotManifestV1`, schema 1 |
| Fan-Out work | Schema 3; Production V2 reader retained |

These versions describe separate layers. Legacy `Item`/`wtgid` names are compatibility identifiers, not permission to rename frozen messages or database columns without migration.

## Maintenance rule

Keep component-owned tables with component behavior. Keep executable queries in source adapters/QueryBuilders and explain their contracts here. Change the relevant specification and Decision Log in the same architecture PR. Use existing Micro-RPI/standards for task execution. Proposed safeguards in these files are not new accepted decisions.

Open questions remain linked to Q-001–Q-014 in the log. Exact Ingestor/operation DDL, downstream publisher evidence, source snapshot authority, retention and live integration remain unverified.
