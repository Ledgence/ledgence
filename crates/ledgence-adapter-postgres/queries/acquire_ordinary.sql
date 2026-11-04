SELECT t.*
FROM tasks AS t
WHERE tenant_id = $1 AND namespace = $2 AND queue = $3
  AND state = 'queued' AND cancel_requested_at_ms IS NULL
  AND dispatch_destination IS NULL AND admission_resource IS NULL
  AND available_at_ms <= $4
ORDER BY available_at_ms, submitted_at_ms, task_id
LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED
