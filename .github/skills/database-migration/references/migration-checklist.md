# Database Migration Checklist

## Current state

- [ ] Existing schema verified
- [ ] Migration mechanism verified
- [ ] All important readers/writers identified
- [ ] Data volume / lock risk considered

## Design

- [ ] Forward migration defined
- [ ] Backfill defined if required
- [ ] Compatibility window defined
- [ ] Constraint/index timing defined
- [ ] Retry/idempotency of migration understood
- [ ] Roll-forward/recovery strategy defined

## Verification

- [ ] Migration applies on representative baseline
- [ ] Existing data migrates correctly
- [ ] Constraints behave as intended
- [ ] Application build/tests pass
- [ ] Integration tests cover changed persistence behavior
- [ ] Deployment ordering is documented
