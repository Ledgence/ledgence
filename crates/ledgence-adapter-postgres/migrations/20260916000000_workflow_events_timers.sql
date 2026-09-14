-- One-shot external waits keep their identity after completion so late callbacks
-- cannot wake a different continuation that happens to reuse a business key.
ALTER TABLE workflow_runs ADD COLUMN external_wait_key text COLLATE "C";
ALTER TABLE workflow_runs ADD COLUMN pending_event_count integer NOT NULL DEFAULT 0
    CHECK (pending_event_count BETWEEN 0 AND 128);
ALTER TABLE workflow_runs ADD COLUMN pending_event_bytes integer NOT NULL DEFAULT 0
    CHECK (pending_event_bytes BETWEEN 0 AND 262144);
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_external_wait_anchor
    CHECK (external_wait_key IS NULL OR wait_activation_id IS NOT NULL);

CREATE TABLE workflow_waits (
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    wait_key text COLLATE "C" NOT NULL,
    activation_id text COLLATE "C" NOT NULL,
    kind text NOT NULL CHECK (kind IN ('event','timer')),
    deadline_ms bigint CHECK (deadline_ms >= 0),
    registered_at_ms bigint NOT NULL CHECK (registered_at_ms >= 0),
    closed_at_ms bigint CHECK (closed_at_ms >= 0),
    PRIMARY KEY (workflow_id,wait_key),
    UNIQUE (workflow_id,activation_id),
    FOREIGN KEY (workflow_id,activation_id)
        REFERENCES workflow_activations(workflow_id,activation_id),
    CHECK (kind <> 'timer' OR deadline_ms IS NOT NULL),
    CHECK (deadline_ms IS NULL OR deadline_ms >= registered_at_ms)
);
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_external_wait
    FOREIGN KEY (workflow_id,external_wait_key)
    REFERENCES workflow_waits(workflow_id,wait_key);

-- Canonical bytes retain the immutable receipt even after consumption. Only
-- pending entries count toward the bounded inbox; retained receipts are not a
-- runnable backlog and require a future explicit retention policy.
CREATE TABLE workflow_events (
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    event_key text COLLATE "C" NOT NULL,
    event_source text COLLATE "C" NOT NULL,
    event_id text COLLATE "C" NOT NULL,
    event_bytes bytea NOT NULL CHECK (octet_length(event_bytes) BETWEEN 1 AND 65536),
    accepted_at_ms bigint NOT NULL CHECK (accepted_at_ms >= 0),
    pending boolean NOT NULL DEFAULT true,
    PRIMARY KEY (workflow_id,event_key),
    UNIQUE (workflow_id,event_source,event_id)
);
CREATE INDEX workflow_events_pending ON workflow_events(workflow_id,event_key)
    WHERE pending;

ALTER TABLE workflow_work DROP CONSTRAINT workflow_work_kind_check;
ALTER TABLE workflow_work ADD CONSTRAINT workflow_work_kind_check
    CHECK (kind IN ('terminal','drain','wait'));
CREATE UNIQUE INDEX workflow_wait_work ON workflow_work(task_id) WHERE kind='wait';
