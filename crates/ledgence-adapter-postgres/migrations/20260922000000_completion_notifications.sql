-- Optional one-shot completion subscriptions. Immutable destination aliases are
-- scoped so an executable cannot silently redirect a pending obligation.
CREATE TABLE completion_destinations (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    destination text COLLATE "C" NOT NULL,
    binding text NOT NULL CHECK (octet_length(binding) BETWEEN 1 AND 4096),
    PRIMARY KEY (tenant_id,namespace,destination)
);

CREATE TABLE completion_subscriptions (
    subscription_id text COLLATE "C" PRIMARY KEY,
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    task_id text COLLATE "C" REFERENCES tasks(task_id),
    workflow_id text COLLATE "C" REFERENCES workflow_runs(workflow_id),
    destination text COLLATE "C" NOT NULL,
    idempotency_key text COLLATE "C" NOT NULL,
    command_bytes bytea NOT NULL CHECK (octet_length(command_bytes) BETWEEN 1 AND 4096),
    state text NOT NULL CHECK (state IN ('waiting','pending','delivering','retrying','delivered','exhausted')),
    generation bigint NOT NULL DEFAULT 1 CHECK (generation BETWEEN 1 AND 1000),
    attempts bigint NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 8),
    total_attempts bigint NOT NULL DEFAULT 0 CHECK (total_attempts BETWEEN 0 AND 8000),
    created_at_ms bigint NOT NULL CHECK (created_at_ms >= 0),
    activated_at_ms bigint CHECK (activated_at_ms >= 0),
    next_attempt_at_ms bigint CHECK (next_attempt_at_ms >= 0),
    lease_token text COLLATE "C",
    lease_until_ms bigint CHECK (lease_until_ms >= 0),
    delivered_at_ms bigint CHECK (delivered_at_ms >= 0),
    exhausted_at_ms bigint CHECK (exhausted_at_ms >= 0),
    last_failure text CHECK (octet_length(last_failure) BETWEEN 1 AND 256),
    event_bytes bytea CHECK (octet_length(event_bytes) BETWEEN 1 AND 16384),
    FOREIGN KEY (tenant_id,namespace,destination)
        REFERENCES completion_destinations(tenant_id,namespace,destination),
    CHECK ((task_id IS NOT NULL) <> (workflow_id IS NOT NULL)),
    CHECK ((state='waiting') = (event_bytes IS NULL)),
    CHECK ((state='waiting') = (activated_at_ms IS NULL)),
    CHECK ((state IN ('pending','retrying')) = (next_attempt_at_ms IS NOT NULL)),
    CHECK ((state='delivering') = (lease_token IS NOT NULL)),
    CHECK ((state='delivering') = (lease_until_ms IS NOT NULL)),
    CHECK ((state='delivered') = (delivered_at_ms IS NOT NULL)),
    CHECK ((state='exhausted') = (exhausted_at_ms IS NOT NULL)),
    CHECK (total_attempts >= attempts),
    CHECK (state<>'waiting' OR (generation=1 AND attempts=0 AND total_attempts=0)),
    CHECK (state NOT IN ('delivering','retrying','delivered','exhausted') OR attempts>0),
    CHECK (state<>'exhausted' OR attempts=8)
);
CREATE UNIQUE INDEX completion_task_key
    ON completion_subscriptions(tenant_id,namespace,task_id,idempotency_key)
    WHERE task_id IS NOT NULL;
CREATE UNIQUE INDEX completion_workflow_key
    ON completion_subscriptions(tenant_id,namespace,workflow_id,idempotency_key)
    WHERE workflow_id IS NOT NULL;
CREATE INDEX completion_task_waiting ON completion_subscriptions(task_id)
    WHERE state='waiting' AND task_id IS NOT NULL;
CREATE INDEX completion_workflow_waiting ON completion_subscriptions(workflow_id)
    WHERE state='waiting' AND workflow_id IS NOT NULL;
CREATE INDEX completion_due ON completion_subscriptions(
    tenant_id,namespace,destination,
    (coalesce(next_attempt_at_ms,lease_until_ms)),subscription_id)
    WHERE state IN ('pending','retrying','delivering');
