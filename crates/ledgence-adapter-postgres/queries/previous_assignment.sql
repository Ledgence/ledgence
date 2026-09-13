SELECT
    t.*,
    a.attempt_id AS a_attempt_id,
    a.task_id AS a_task_id,
    a.generation AS a_generation,
    a.lease_id AS a_lease_id,
    a.worker_session_id AS a_worker_session_id,
    a.consumer_id AS a_consumer_id,
    a.event_source AS a_event_source,
    a.event_id AS a_event_id,
    a.event_bytes AS a_event_bytes,
    a.expires_at_ms AS a_expires_at_ms,
    a.deadline_ms AS a_deadline_ms,
    a.authority_deadline_ms AS a_authority_deadline_ms,
    a.state AS a_state,
    a.execution_may_have_started AS a_execution_may_have_started,
    trunc(a.last_renew_sequence)::text AS a_last_renew_sequence_text,
    a.last_renew_intent AS a_last_renew_intent,
    a.quiescence AS a_quiescence,
    a.finished_at_ms AS a_finished_at_ms,
    s.accepted_command AS a_accepted_command,
    s.accepted_at AS a_accepted_at,
    s.operation_id AS a_accepted_operation_id
FROM tasks t
JOIN attempts a ON a.task_id=t.task_id
LEFT
JOIN accepted_settlements s ON s.attempt_id=a.attempt_id
WHERE t.tenant_id=$1 AND t.namespace=$2 AND t.task_id=$3 AND a.attempt_id=$4
