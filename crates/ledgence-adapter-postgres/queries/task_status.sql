SELECT
    t.tenant_id, t.namespace, t.task_id, t.run_id, t.queue, t.correlation_key,
    t.state, t.attempt_count, t.current_attempt_id, t.submitted_at_ms,
    t.available_at_ms, t.terminal_at_ms, t.cancel_requested_at_ms, t.next_expiry_ms,
    a.attempt_id AS latest_attempt_id, a.state AS latest_attempt_state
FROM tasks t
LEFT JOIN attempts a ON a.task_id=t.task_id AND a.generation=t.attempt_count
WHERE t.tenant_id=$1 AND t.namespace=$2 AND t.task_id=$3
