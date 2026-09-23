WITH due AS (
    SELECT subscription_id
    FROM completion_subscriptions
    WHERE tenant_id = $1 AND namespace = $2 AND destination = $3
      AND state IN ('pending', 'retrying', 'delivering')
      AND coalesce(next_attempt_at_ms, lease_until_ms) <= $4
    ORDER BY coalesce(next_attempt_at_ms, lease_until_ms), subscription_id
    LIMIT $5
    FOR UPDATE SKIP LOCKED
)
UPDATE completion_subscriptions s
SET state = CASE WHEN s.attempts >= 8 THEN 'exhausted' ELSE 'delivering' END,
    attempts = CASE WHEN s.attempts >= 8 THEN s.attempts ELSE s.attempts + 1 END,
    total_attempts = CASE WHEN s.attempts >= 8 THEN s.total_attempts ELSE s.total_attempts + 1 END,
    next_attempt_at_ms = NULL,
    lease_token = CASE WHEN s.attempts >= 8 THEN NULL ELSE 'ntflease_' || gen_random_uuid()::text END,
    lease_until_ms = CASE WHEN s.attempts >= 8 THEN NULL ELSE $6 END,
    exhausted_at_ms = CASE WHEN s.attempts >= 8 THEN $4 ELSE NULL END,
    last_failure = CASE
        WHEN s.state = 'delivering' THEN 'delivery lease expired; acknowledgment unknown'
        ELSE s.last_failure
    END
FROM due
WHERE s.subscription_id = due.subscription_id
RETURNING s.*
