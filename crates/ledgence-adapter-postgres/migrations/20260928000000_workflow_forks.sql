-- Workflow-wide immutable fork receipts. Collection drains this boundedly
-- before deleting the accepting activation/attempt or the parent run.
CREATE TABLE workflow_forks (
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    fork_key text COLLATE "C" NOT NULL,
    request_bytes bytea NOT NULL CHECK (octet_length(request_bytes) BETWEEN 1 AND 131072),
    accepting_activation_id text COLLATE "C" NOT NULL,
    accepting_attempt_id text COLLATE "C" NOT NULL REFERENCES attempts(attempt_id),
    accepted_at_ms bigint NOT NULL CHECK (accepted_at_ms >= 0),
    PRIMARY KEY (workflow_id,fork_key),
    FOREIGN KEY (workflow_id,accepting_activation_id)
        REFERENCES workflow_activations(workflow_id,activation_id)
);
CREATE INDEX workflow_forks_activation ON workflow_forks(accepting_activation_id);
CREATE INDEX workflow_forks_attempt ON workflow_forks(accepting_attempt_id);
ALTER TABLE owned_workflow_links ADD COLUMN fork_key text COLLATE "C";
ALTER TABLE owned_workflow_links ADD CONSTRAINT owned_workflow_fork
    FOREIGN KEY (parent_workflow_id,fork_key) REFERENCES workflow_forks(workflow_id,fork_key);
CREATE INDEX owned_workflow_fork_members ON owned_workflow_links(parent_workflow_id,fork_key)
    WHERE fork_key IS NOT NULL;
