-- A stable statement-time cutoff permits an index range on tasks_expiry.
-- expire_one still validates fresh wall time after locking each candidate.
SELECT task_id
FROM tasks
WHERE state = 'active'
  AND next_expiry_ms <= floor(extract(epoch FROM statement_timestamp()) * 1000)::bigint
ORDER BY next_expiry_ms, task_id
LIMIT $1
