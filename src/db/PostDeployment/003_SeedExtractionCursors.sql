INSERT INTO public.extraction_cursor
(
    windfarm_id,
    source_type,
    is_enabled,
    last_committed_updated_at,
    last_committed_id,
    lease_token,
    lease_owner,
    lease_expires_at,
    last_run_started_utc,
    last_committed_utc,
    active_run_id,
    last_run_completed_utc,
    last_run_outcome,
    consecutive_failures,
    last_error,
    deleted_last_committed_updated_at,
    deleted_last_committed_id
)
VALUES
    (1,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (2,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (3,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (4,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (5,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (6,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (7,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (8,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (9,  'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (10, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (11, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (12, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (13, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (14, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (15, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (16, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (17, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (18, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL),
    (19, 'Production', TRUE, TIMESTAMPTZ '2025-12-31 23:59:59+00', '0', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL, NULL)
ON CONFLICT (windfarm_id, source_type)
DO NOTHING;

-- Backfill deleted_last_committed_updated_at/deleted_last_committed_id for every row that
-- existed before this column pair was added (ON CONFLICT DO NOTHING above never touches them).
-- Outage has a real deletion feed, so it starts its checkpoint at the same point as the live
-- feed; every other source_type has no delete feed and keeps NULL (not applicable).
UPDATE public.extraction_cursor
   SET deleted_last_committed_updated_at = CASE WHEN source_type = 'Outage' THEN last_committed_updated_at ELSE deleted_last_committed_updated_at END,
       deleted_last_committed_id = CASE WHEN source_type = 'Outage' THEN last_committed_id ELSE deleted_last_committed_id END
 WHERE deleted_last_committed_updated_at IS NULL
    OR deleted_last_committed_id IS NULL;