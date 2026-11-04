SELECT
    a.*,
    LEAST(a.deadline_ms,h.deadline_ms,h.stop_requested_at_ms,h.released_at_ms) AS effective_deadline_ms,
    trunc(a.last_renew_sequence)::text AS last_renew_sequence_text,
    s.accepted_command,
    s.accepted_at,
    s.operation_id AS accepted_operation_id
FROM attempts a
LEFT JOIN queue_concurrency_holds h ON h.attempt_id=a.attempt_id
LEFT JOIN accepted_settlements s ON s.attempt_id=a.attempt_id
WHERE a.task_id=$1 AND a.attempt_id=$2
