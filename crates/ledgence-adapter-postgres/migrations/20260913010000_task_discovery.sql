-- Task discovery uses immutable submission order within a tenant and namespace.
-- Each supported exact filter has an ordered access path; combined filters use
-- the most selective path and apply remaining predicates to its candidates.
CREATE INDEX tasks_discovery ON tasks
    (tenant_id, namespace, submitted_at_ms DESC, task_id DESC);
CREATE INDEX tasks_discovery_state ON tasks
    (tenant_id, namespace, state, submitted_at_ms DESC, task_id DESC);
CREATE INDEX tasks_discovery_queue ON tasks
    (tenant_id, namespace, queue, submitted_at_ms DESC, task_id DESC);
CREATE INDEX tasks_discovery_correlation ON tasks
    (tenant_id, namespace, correlation_key, submitted_at_ms DESC, task_id DESC)
    WHERE correlation_key IS NOT NULL;
