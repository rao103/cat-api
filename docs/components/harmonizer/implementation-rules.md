# Harmonizer Component Rules

These rules extend the shared platform standards.

## Primary responsibility

Harmonizer converts exact immutable Raw input into canonical Harmonized component state.

It owns source-specific normalization/merge/reconciliation required to produce correct Harmonized inputs.

It does not perform final loss calculation.

## Input

Harmonizer consumes versioned work messages from Raw Fan-Out.

Input should identify enough information to load exact immutable Raw data or a complete refresh manifest/scope.

Relevant identity may include:

- source type,
- windfarm ID/name,
- WTG ID/name,
- data date,
- extraction token,
- batch/work version,
- attempt/run identity,
- exact blob path or manifest path,
- operation/correlation identifiers.

Numeric IDs remain numeric fields.

## Processing boundary

Typical flow:

```text
receive
→ validate contract/version/identity
→ load exact Raw input or complete refresh scope
→ acquire required scoped state protection
→ normalize
→ merge or reconcile canonical state
→ write immutable Harmonized artifact + lineage
→ commit authoritative control state
→ create/publish downstream intent
→ acknowledge message
```

## Production

Production is WTG-day oriented.

The Harmonizer must merge changed rows into the current WTG-day canonical revision correctly.

Multiple Raw artifacts may contribute to the same WTG-day.

Do not replace correct existing rows merely because a later extraction contains only changed rows.

A ten-minute UTC day normally contains 144 slots; completeness/validation rules should be explicit rather than assumed from file presence.

## Event sources

Outage, Alarm, Curtailment, and Adjustment are lifecycle/event sources.

Use:

- stable event identity,
- source revision/order,
- deletion/tombstone evidence,
- canonical current event state.

Do not use extraction token alone as the business-version ordering authority.

## Event footprint reconciliation

A changed event may affect:

- old dates,
- new dates,
- old WTG membership,
- new WTG membership.

Therefore event processing must reconcile old + new footprint.

Examples requiring cleanup:

- event moved,
- event shrank,
- event extended,
- WTG removed,
- event deleted,
- open event later closed.

This is why event lifecycle logic belongs where prior canonical state is available.

## Deletion safety

Older changes must not resurrect a newer deletion.

Duplicate deletion must be safe.

Equal event key/version with conflicting content is an incident unless the source defines a valid deterministic tie-breaker.

## Refresh

A complete authoritative refresh scope must be treated as a scope-level truth.

Zero-event refresh can be authoritative.

Do not mistake multipart refresh delivery for independent partial snapshots.

Use complete manifest/scope semantics before reconciliation.

## Harmonized artifacts

Harmonized output is immutable by revision/token.

Maintain approved paths and schemas.

Lineage must preserve enough provenance to identify contributing Raw artifacts/extraction identity.

## Snapshot / manifest / lineage

Where the accepted architecture uses:

- `snapshot.json`
- `lineage.json`
- package/revision manifests

their responsibilities must remain distinct.

Do not let a filename substitute for durable control-state correctness.

## Control state

Commit state with explicit concurrency/fencing semantics.

A failed or missing input component must not be interpreted as an empty successful component.

## Downstream handoff

Harmonizer emits or records downstream calculation/package intent only after the Harmonized state required by that contract is durable.

Do not acknowledge input before that durable boundary.

## Source handlers

One Harmonizer worker may route multiple source types through explicit handlers/strategies.

Do not duplicate the whole worker architecture per source type.

Use Strategy/Factory only where it reflects real source-specific behavior.

## Non-goals

Harmonizer must not:

- own Ingestor checkpoints,
- calculate final loss,
- publish final serving rows,
- silently redefine message contracts,
- create a second conflicting control authority,
- infer source ordering from arrival order.
