-- Retirement is explicit and irreversible. Markers prevent partially collected
-- records from being presented as intact public executions.
ALTER TABLE tasks ADD COLUMN retiring_at_ms bigint CHECK (retiring_at_ms >= 0);
ALTER TABLE tasks ADD CONSTRAINT tasks_retiring_terminal CHECK (retiring_at_ms IS NULL OR terminal_at_ms IS NOT NULL);
ALTER TABLE workflow_runs ADD COLUMN retiring_at_ms bigint CHECK (retiring_at_ms >= 0);
ALTER TABLE workflow_runs ADD CONSTRAINT workflows_retiring_terminal CHECK (retiring_at_ms IS NULL OR terminal_at_ms IS NOT NULL);
CREATE INDEX tasks_retention ON tasks(tenant_id,namespace,terminal_at_ms,task_id) WHERE terminal_at_ms IS NOT NULL AND retiring_at_ms IS NULL;
CREATE INDEX tasks_retiring ON tasks(tenant_id,namespace,retiring_at_ms,task_id) WHERE retiring_at_ms IS NOT NULL;
CREATE INDEX workflows_retention ON workflow_runs(tenant_id,namespace,terminal_at_ms,workflow_id) WHERE terminal_at_ms IS NOT NULL AND retiring_at_ms IS NULL;
CREATE INDEX workflows_retiring ON workflow_runs(tenant_id,namespace,retiring_at_ms,workflow_id) WHERE retiring_at_ms IS NOT NULL;
CREATE INDEX sessions_retention ON worker_sessions(tenant_id,namespace,expires_at_ms,session_id);
-- Every collection lookup and foreign-key check has an identity-prefixed index.
CREATE INDEX workflow_runs_parent ON workflow_runs(parent_workflow_id) WHERE parent_workflow_id IS NOT NULL;
CREATE INDEX tasks_workflow_retention ON tasks(workflow_id,state,task_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX tasks_workflow_activation ON tasks(workflow_activation_id) WHERE workflow_activation_id IS NOT NULL;
CREATE INDEX workflow_waits_activation_retention ON workflow_waits(activation_id);
CREATE INDEX owned_links_activation_retention ON owned_workflow_links(creating_activation_id);
CREATE INDEX workflow_work_child_retention ON workflow_work(child_workflow_id) WHERE child_workflow_id IS NOT NULL;
CREATE INDEX local_results_attempt ON workflow_local_results(attempt_id);
CREATE INDEX workflow_work_retention ON workflow_work(workflow_id,processed_at_ms,id);
CREATE INDEX workflow_work_task_retention ON workflow_work(task_id);
CREATE INDEX dispatch_receipts_task ON dispatch_claim_receipts(task_id);
CREATE INDEX completion_task_retention ON completion_subscriptions(task_id) WHERE task_id IS NOT NULL;
CREATE INDEX completion_workflow_retention ON completion_subscriptions(workflow_id) WHERE workflow_id IS NOT NULL;
CREATE INDEX attempts_session_active ON attempts(worker_session_id) WHERE state='active';
-- Small, durable scan positions rotate past protected records. No OFFSET, no
-- unbounded cascade and no permanent head-of-line blocking by an idle cursor.
CREATE TABLE retention_scans (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    lane text NOT NULL CHECK(lane IN ('task','workflow','session')),
    after_ms bigint NOT NULL DEFAULT 0 CHECK(after_ms >= 0),
    after_id text COLLATE "C" NOT NULL DEFAULT '',
    retiring_after_ms bigint NOT NULL DEFAULT 0 CHECK(retiring_after_ms >= 0),
    retiring_after_id text COLLATE "C" NOT NULL DEFAULT '',
    process_retiring boolean NOT NULL DEFAULT false,
    PRIMARY KEY(tenant_id,namespace,lane)
);
CREATE TABLE retention_turn (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    lane smallint NOT NULL CHECK(lane BETWEEN 0 AND 2),
    PRIMARY KEY(tenant_id,namespace)
);
