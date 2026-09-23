-- Workflow coordination is separate from ordinary task identity and delivery.
CREATE TABLE workflow_runs (
    workflow_id text COLLATE "C" PRIMARY KEY,
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    idempotency_key text COLLATE "C" NOT NULL,
    correlation_key text COLLATE "C",
    submission_bytes bytea NOT NULL CHECK (octet_length(submission_bytes) BETWEEN 1 AND 2097152),
    controller_bytes bytea NOT NULL CHECK (octet_length(controller_bytes) BETWEEN 1 AND 2097152),
    state text NOT NULL CHECK (state IN ('running','waiting','failing','cancelling','succeeded','failed','cancelled')),
    revision ldg_u64 NOT NULL DEFAULT 0,
    continuation text COLLATE "C" NOT NULL,
    checkpoint_bytes bytea NOT NULL CHECK (octet_length(checkpoint_bytes) BETWEEN 1 AND 65536),
    current_activation_id text COLLATE "C",
    wait_activation_id text COLLATE "C",
    wait_keys_bytes bytea,
    outcome_bytes bytea,
    submitted_at_ms bigint NOT NULL CHECK (submitted_at_ms >= 0),
    terminal_at_ms bigint CHECK (terminal_at_ms >= 0),
    history_sequence ldg_u64 NOT NULL DEFAULT 0,
    UNIQUE (tenant_id, namespace, idempotency_key),
    CHECK ((state IN ('succeeded','failed','cancelled')) = (terminal_at_ms IS NOT NULL)),
    CHECK ((wait_activation_id IS NULL) = (wait_keys_bytes IS NULL))
);

CREATE TABLE workflow_activations (
    activation_id text COLLATE "C" PRIMARY KEY,
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    revision ldg_u64 NOT NULL,
    task_id text COLLATE "C" NOT NULL UNIQUE REFERENCES tasks(task_id),
    context_bytes bytea NOT NULL CHECK (octet_length(context_bytes) BETWEEN 1 AND 655360),
    applied_at_ms bigint CHECK (applied_at_ms >= 0),
    error_bytes bytea,
    UNIQUE (workflow_id, revision),
    UNIQUE (workflow_id, activation_id)
);

ALTER TABLE tasks ADD COLUMN workflow_activation_id text COLLATE "C";
ALTER TABLE tasks ADD COLUMN workflow_id text COLLATE "C" REFERENCES workflow_runs(workflow_id);
ALTER TABLE tasks ADD CONSTRAINT tasks_workflow_activation
    FOREIGN KEY (workflow_activation_id) REFERENCES workflow_activations(activation_id)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_current_activation
    FOREIGN KEY (workflow_id,current_activation_id)
    REFERENCES workflow_activations(workflow_id,activation_id)
    DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_wait_activation
    FOREIGN KEY (workflow_id,wait_activation_id)
    REFERENCES workflow_activations(workflow_id,activation_id);

CREATE TABLE workflow_task_links (
    task_id text COLLATE "C" PRIMARY KEY REFERENCES tasks(task_id),
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    activation_id text COLLATE "C" NOT NULL,
    is_activation boolean NOT NULL,
    consumed boolean NOT NULL DEFAULT false,
    terminal boolean NOT NULL DEFAULT false,
    command_key text COLLATE "C" NOT NULL,
    FOREIGN KEY (workflow_id,activation_id)
        REFERENCES workflow_activations(workflow_id,activation_id),
    UNIQUE (activation_id,is_activation,command_key)
);
CREATE UNIQUE INDEX workflow_child_key ON workflow_task_links(workflow_id,command_key) WHERE NOT is_activation;
CREATE INDEX workflow_task_links_owned ON workflow_task_links(workflow_id,task_id);
CREATE INDEX workflow_task_links_live ON workflow_task_links(workflow_id,task_id) WHERE NOT terminal;
CREATE INDEX workflow_task_links_pending ON workflow_task_links(workflow_id,task_id) WHERE NOT is_activation AND terminal AND NOT consumed;

CREATE TABLE workflow_local_results (
    activation_id text COLLATE "C" NOT NULL REFERENCES workflow_activations(activation_id),
    step_key text COLLATE "C" NOT NULL,
    record_bytes bytea NOT NULL CHECK (octet_length(record_bytes) BETWEEN 1 AND 131072),
    attempt_id text COLLATE "C" NOT NULL REFERENCES attempts(attempt_id),
    accepted_at_ms bigint NOT NULL CHECK (accepted_at_ms >= 0),
    PRIMARY KEY (activation_id,step_key)
);

-- Only terminal task transitions create these obligations. A processing lease
-- is advisory scheduling ownership; apply verifies its token under the run lock.
CREATE TABLE workflow_work (
    id text COLLATE "C" PRIMARY KEY,
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    task_id text COLLATE "C" NOT NULL REFERENCES tasks(task_id),
    kind text NOT NULL DEFAULT 'terminal' CHECK (kind IN ('terminal','drain')),
    available_at_ms bigint NOT NULL CHECK (available_at_ms >= 0),
    created_at_ms bigint NOT NULL CHECK (created_at_ms >= 0),
    lease_token text COLLATE "C",
    lease_until_ms bigint CHECK (lease_until_ms >= 0),
    failures bigint NOT NULL DEFAULT 0 CHECK (failures >= 0),
    last_error text,
    processed_at_ms bigint CHECK (processed_at_ms >= 0),
    CHECK ((lease_token IS NULL) = (lease_until_ms IS NULL))
);
CREATE UNIQUE INDEX workflow_terminal_work ON workflow_work(task_id) WHERE kind='terminal';
CREATE INDEX workflow_work_due ON workflow_work(available_at_ms,id)
    WHERE processed_at_ms IS NULL;

CREATE TABLE workflow_history (
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    sequence ldg_u64 NOT NULL,
    activation_id text COLLATE "C",
    at_ms bigint NOT NULL CHECK (at_ms >= 0),
    reason text NOT NULL,
    PRIMARY KEY (workflow_id,sequence)
);
