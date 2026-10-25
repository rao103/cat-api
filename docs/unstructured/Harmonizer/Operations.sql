-- Read-only diagnostics. Use the existing authenticated PostgreSQL connection.
-- First unresolved work; Raw references and complete replay body are durable.
SELECT s.windfarm_id, s.wtgid, s.data_date, s.input_type, s.status,
       s.first_failed_raw_paths, s.last_error_code,
       b.artifact_id, b.batch_id, b.message_body
FROM control.wtg_day_input_state AS s
JOIN control.harmonizer_batch AS b
  ON b.artifact_id=s.first_failed_artifact_id AND b.batch_id=s.first_failed_batch_id
WHERE s.status='Failed'
ORDER BY s.updated_date_utc;

-- Expired owners eligible for replay/reclaim. A live lease must never be bypassed manually.
SELECT windfarm_id, wtgid, data_date, input_type, lease_token, lease_expires_at_utc
FROM control.wtg_day_input_state
WHERE lease_expires_at_utc <= clock_timestamp();

-- Pending batch exact bodies: publish message_body unchanged to the Harmonizer queue.
SELECT artifact_id,batch_id,created_date_utc,message_body
FROM control.harmonizer_batch WHERE completed_at_utc IS NULL ORDER BY created_date_utc;

-- Outbox backlog. PublishNotifications=false intentionally retains these integration requests.
SELECT count(*) AS pending_count, min(created_date_utc) AS oldest_pending
FROM control.harmonizer_outbox WHERE sent_at_utc IS NULL;

-- Operation movement. Bind :operation_id using your SQL client.
SELECT artifact_id,batch_id,operation_id,execution_id,created_date_utc,completed_at_utc
FROM control.harmonizer_batch
WHERE operation_id=:operation_id ORDER BY created_date_utc;

SELECT id,deduplication_key,operation_id,execution_id,created_date_utc,sent_at_utc
FROM control.harmonizer_outbox
WHERE operation_id=:operation_id ORDER BY id;


-- New lifecycle backlog and failed source/control work.
SELECT windfarm_id,source_type,work_id,event_id,event_revision,planning_generation,
       has_pending,needs_continuation,continued_through,due_at_utc,last_error_code,plan_path
FROM control.event_materialization_state
WHERE has_pending OR needs_continuation OR last_error_code IS NOT NULL
ORDER BY due_at_utc,windfarm_id,work_id;

SELECT artifact_id,batch_id,windfarm_id,source_type,work_kind,
       canonical_applied_at_utc,completed_at_utc,last_error_code,message_body
FROM control.harmonizer_batch
WHERE source_type='Outage' AND completed_at_utc IS NULL
ORDER BY created_date_utc;

-- Check this function even when there is no current Outage component row.
-- A false result plus no known event is empty Outage; do not require five physical rows.
SELECT control.harmonizer_event_day_pending(2,11,DATE '2026-07-18','Outage');

-- Existing rows expose the same pending predicate through readiness.
SELECT windfarm_id,wtgid,data_date,input_type,status,readiness_status,component_revision
FROM control.harmonizer_input_readiness
WHERE readiness_status='Pending';
