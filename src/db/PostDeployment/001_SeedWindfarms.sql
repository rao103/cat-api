INSERT INTO public.windfarm
(
    id,
    windfarm_name,
    windfarm_name_in_source,
    time_offset,
    min_retrieve_date,
    is_active
)
VALUES
    (1, 'WOW01', 'WOW01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (2, 'WOW02', 'WOW02', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (3, 'BKR01', 'BKR01', 1, TIMESTAMP '2025-12-31 23:59:59.0000000', FALSE),
    (4, 'LIC01', 'LIC01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (5, 'WDS01', 'WDS01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (6, 'WMR01', 'WMR01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (7, 'GFS01', 'GFS01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (8, 'GFS02', 'GFS02', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (9, 'CHW01', 'CHW01', 8, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (10, 'BSW01', 'BSW01', 1, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (11, 'ROW01', 'ROW01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (12, 'WOW03', 'WOW03', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (13, 'WOW04', 'WOW04', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (14, 'HOW01E', 'HOW01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (15, 'HOW01W', 'HOW01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (16, 'HOW01C', 'HOW01', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (17, 'HOW02', 'HOW02', 0, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (18, 'CHW02', 'CHW02', 8, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE),
    (19, 'CHW04', 'CHW04', 8, TIMESTAMP '2025-12-31 23:59:59.0000000', TRUE)
ON CONFLICT (id)
DO UPDATE SET
    windfarm_name           = EXCLUDED.windfarm_name,
    windfarm_name_in_source = EXCLUDED.windfarm_name_in_source,
    time_offset             = EXCLUDED.time_offset,
    min_retrieve_date       = EXCLUDED.min_retrieve_date,
    is_active               = EXCLUDED.is_active;