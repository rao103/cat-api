-- Requires the existing control schema and public.extraction_run.operation_id.
BEGIN;

CREATE TABLE IF NOT EXISTS control.operation (
    operation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requested_by TEXT NOT NULL,
    created_date_utc TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    inserted_by TEXT,
    updated_by TEXT
);

CREATE FUNCTION control.resolve_operation(p_operation_id TEXT, p_created_by TEXT)
RETURNS UUID LANGUAGE plpgsql AS $$
DECLARE selected_id UUID;
BEGIN
    selected_id := COALESCE(p_operation_id::UUID, gen_random_uuid());
    INSERT INTO control.operation (operation_id, requested_by)
    VALUES (selected_id, p_created_by)
    ON CONFLICT (operation_id) DO NOTHING;
    RETURN selected_id;
END;
$$;

CREATE INDEX ix_extraction_run_operation ON public.extraction_run (operation_id, id)
    WHERE operation_id IS NOT NULL;

COMMIT;
