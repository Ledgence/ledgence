-- Approvals share the one-shot wait identity and workflow mutation lock. A
-- decision can exist only after the request's checkpoint/wait has committed.
ALTER TABLE workflow_waits DROP CONSTRAINT workflow_waits_kind_check;
ALTER TABLE workflow_waits ADD CONSTRAINT workflow_waits_kind_check
    CHECK (kind IN ('event','timer','approval'));
ALTER TABLE workflow_waits ADD CONSTRAINT workflow_approval_deadline
    CHECK (kind <> 'approval' OR deadline_ms IS NOT NULL);

CREATE TABLE workflow_approvals (
    workflow_id text COLLATE "C" NOT NULL,
    wait_key text COLLATE "C" NOT NULL,
    snapshot_bytes bytea NOT NULL CHECK (octet_length(snapshot_bytes) BETWEEN 1 AND 98304),
    decision_bytes bytea CHECK (octet_length(decision_bytes) BETWEEN 1 AND 98304),
    PRIMARY KEY (workflow_id,wait_key),
    FOREIGN KEY (workflow_id,wait_key) REFERENCES workflow_waits(workflow_id,wait_key)
);
