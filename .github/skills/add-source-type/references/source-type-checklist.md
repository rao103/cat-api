# Add Source Type Checklist

## Semantics

- [ ] Stable source/business identity defined
- [ ] Update/version ordering defined
- [ ] Delete/tombstone semantics defined
- [ ] Business-time scope/footprint defined
- [ ] Incremental semantics defined
- [ ] Refresh semantics defined
- [ ] Cursor requirement explicitly decided

## Ingestor / producer

- [ ] Source adapter/query implemented
- [ ] Raw schema/path defined
- [ ] Retry/cursor/fencing behavior verified
- [ ] Message contract/version handled

## Raw Fan-Out

- [ ] Work identity defined
- [ ] Source-specific planning strategy added only where needed
- [ ] Duplicate parent/child delivery safe

## Harmonizer

- [ ] Canonicalization/merge/reconciliation defined
- [ ] Source ordering handled
- [ ] Delete/old-footprint cleanup handled where relevant
- [ ] Lineage/control state updated

## Verification

- [ ] Unit tests
- [ ] provider/persistence integration tests
- [ ] duplicate/retry tests
- [ ] stale/out-of-order tests when relevant
- [ ] refresh tests when relevant
- [ ] end-to-end contract compatibility checked
