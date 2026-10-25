# ADR-0004 — Parquet artifacts, revisions and current SQL serving

**Recorded:** 2026-10-11 (Asia/Kuala_Lumpur). **Evidence:** user-confirmed company constraints plus saved platform specifications; public technical references checked on this date. No live repository, service catalogue, company Kafka configuration or deployed infrastructure was inspected. Proposed rationale is not company approval or performance evidence.

## Status

Existing discussed Parquet/immutable revision direction; formal ADR draft for review. New measurements, Data Lake compatibility and historical publication-index proof remain pending.

## Context

The platform needs source capture replay, canonical history, deterministic daily materialization and historical/as-of reconstruction. Company Data Lake currently uses Azure Blob and will move backing storage to AWS in 2027. PostgreSQL stores transactional control and latest serving results; it is not intended to duplicate every historical Raw/Harmonized row.

## Recorded decision and rationale

Use versioned Parquet artifacts within the company Data Lake contract for Raw and immutable canonical/daily source data. Store provenance and publication metadata separately. Publish candidates, then commit authoritative pointers with fenced control transactions. Keep latest business serving data in the company catalogue PostgreSQL product (DL-008).

Apache Parquet is an open columnar format with compression/encoding and multiple reader implementations. That supports a shared data representation across candidate engines; it does not guarantee identical feature support or provider interoperability without tests.

The platform's immutability, lineage, replay and snapshot selection come from its publication protocol, not from the Parquet format alone. A historical file existing in a folder does not make it committed or identify when it was authoritative.

## Alternatives

| Alternative | Assessment |
|---|---|
| JSON/CSV as primary analytical artifacts | Useful for interchange/debugging; would require format/query evaluation instead of assuming equivalent typed columnar behavior |
| Avro-first artifacts | Another open representation; no measured need supplied to replace the existing Parquet readers/writers |
| All business rows/history in PostgreSQL | Would put historical volume/revision retention in transactional serving; not current agreed scope |
| Warehouse-managed storage only | Could simplify some analytics but changes capture/replay/access ownership and warehouse dependence; validate full requirements |
| Parquet plus a table format/catalogue | Candidate if transactional table updates, compaction or catalogue features justify it; plain files do not forbid using those services later |
| Parquet with application manifest/pointers | Current direction; application explicitly owns consistent revision selection and cleanup |

Do not claim Parquet is always smaller/faster, or that lake storage avoids all operational work. Compression/row group sizing/schema and query patterns determine performance.

## Partitioning, publication and references

Raw preserves extraction/source identity and exact contributing parts. Production remains daily-series downstream; events retain canonical identity and old/new footprint history. Daily outputs use WTG/date/source/revision/attempt structure. Package manifests select coherent input revisions and are Package Coordinator-owned.

Committed database pointers select candidates; source snapshot files hold merge state, lineage provides origins, package manifests declare input sets. Preserve complete source snapshots/revisions where required, including valid empty inputs. Do not scan every attempt/revision with a wildcard and aggregate duplicate or uncommitted rows.

Logical lake identity/storage adapter boundaries must follow ADR-0001. The company storage migration should not change merge keys, business schemas or lineage meaning. Existing physical URL references may still require mapping; no stable service identity is assumed implemented.

## Historical query and restore limitations

A requested historical time needs retained publication evidence identifying the authoritative revisions at that time across dates/sources, not just today's pointer plus row timestamps. Clarify source effective time versus publication time (Q-010). Bring-forward of an earlier version creates a new processing operation; it is not a silent overwrite of current SQL.

Avoid adding a mandatory historical Query Engine to the ordinary pipeline solely because artifacts can be queried. Current API reads can use PostgreSQL while historical query work is developed.

## Consequences and verification

File sizing/row groups, schema evolution, null/decimal/UTC semantics, path encoding, small-file growth, orphan candidates and retained references require management. Do not purge Raw/history merely because SQS messages expired. Retention must cover pending work, lineage and audit; Q-012 remains open.

Prove writer/reader compatibility with representative Production/Outage/Adjustment schema, retry candidates, empty outputs and long histories. Measure bytes scanned, memory and latency for relevant queries. Validate migration resolves previously committed artifacts and does not duplicate revisions. Reconsider a richer table format/warehouse only when concrete requirements or measurements justify it.

## References

- [Apache Parquet overview and implementation compatibility](https://parquet.apache.org/docs/overview/)
- [Athena columnar storage guidance](https://docs.aws.amazon.com/athena/latest/ug/columnar-storage.html)

Related: ADR-0001, ADR-0005, DL-005/DL-013, DL-008, DL-015, DL-018 and DL-026.
