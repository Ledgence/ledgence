SELECT
    a.*,
    trunc(a.last_renew_sequence)::text AS last_renew_sequence_text,
    s.accepted_command,
    s.accepted_at,
    s.operation_id AS accepted_operation_id
FROM attempts a
LEFT
JOIN accepted_settlements s USING(attempt_id)
WHERE a.task_id=$1 AND a.attempt_id=$2
