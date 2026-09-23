-- Ownership is immutable. Only direct parent/child relationships coordinate;
-- descendant execution never takes a mutable root-workflow lock.
ALTER TABLE workflow_runs ADD COLUMN parent_workflow_id text COLLATE "C" REFERENCES workflow_runs(workflow_id);
ALTER TABLE workflow_runs ADD COLUMN root_workflow_id text COLLATE "C";
ALTER TABLE workflow_runs ADD COLUMN nesting_depth integer NOT NULL DEFAULT 0 CHECK (nesting_depth BETWEEN 0 AND 16);
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_lineage CHECK (
    (parent_workflow_id IS NULL AND root_workflow_id IS NULL AND nesting_depth=0)
    OR (parent_workflow_id IS NOT NULL AND root_workflow_id IS NOT NULL AND nesting_depth>0
        AND parent_workflow_id<>workflow_id AND root_workflow_id<>workflow_id));
ALTER TABLE workflow_runs DROP CONSTRAINT workflow_runs_tenant_id_namespace_idempotency_key_key;
CREATE UNIQUE INDEX workflow_root_submission_key ON workflow_runs(tenant_id,namespace,idempotency_key)
    WHERE parent_workflow_id IS NULL;

CREATE TABLE owned_workflow_links (
    parent_workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    command_key text COLLATE "C" NOT NULL,
    child_workflow_id text COLLATE "C" NOT NULL UNIQUE REFERENCES workflow_runs(workflow_id),
    creating_activation_id text COLLATE "C" NOT NULL,
    terminal boolean NOT NULL DEFAULT false,
    consumed boolean NOT NULL DEFAULT false,
    cancel_enqueued boolean NOT NULL DEFAULT false,
    PRIMARY KEY (parent_workflow_id,command_key),
    FOREIGN KEY (parent_workflow_id,creating_activation_id)
        REFERENCES workflow_activations(workflow_id,activation_id),
    CHECK (parent_workflow_id<>child_workflow_id)
);
CREATE INDEX owned_workflow_live ON owned_workflow_links(parent_workflow_id,child_workflow_id) WHERE NOT terminal;
CREATE INDEX owned_workflow_pending ON owned_workflow_links(parent_workflow_id,child_workflow_id) WHERE terminal AND NOT consumed;
CREATE INDEX owned_workflow_cancel ON owned_workflow_links(parent_workflow_id,child_workflow_id) WHERE NOT terminal AND NOT cancel_enqueued;

ALTER TABLE tasks ADD COLUMN parent_workflow_id text COLLATE "C";
ALTER TABLE tasks ADD COLUMN root_workflow_id text COLLATE "C";
ALTER TABLE tasks ADD CONSTRAINT task_workflow_lineage CHECK (
    (parent_workflow_id IS NULL AND root_workflow_id IS NULL)
    OR (workflow_id IS NOT NULL AND parent_workflow_id IS NOT NULL AND root_workflow_id IS NOT NULL
        AND parent_workflow_id<>workflow_id AND root_workflow_id<>workflow_id));

ALTER TABLE workflow_work ADD COLUMN child_workflow_id text COLLATE "C" REFERENCES workflow_runs(workflow_id);
ALTER TABLE workflow_work DROP CONSTRAINT workflow_work_kind_check;
ALTER TABLE workflow_work ADD CONSTRAINT workflow_work_kind_check
    CHECK (kind IN ('terminal','drain','wait','workflow_terminal','cancel_owned'));
ALTER TABLE workflow_work ADD CONSTRAINT workflow_work_child_source
    CHECK ((kind='workflow_terminal') = (child_workflow_id IS NOT NULL));
CREATE UNIQUE INDEX workflow_child_terminal_work ON workflow_work(child_workflow_id) WHERE kind='workflow_terminal';
CREATE UNIQUE INDEX workflow_owned_cancel_work ON workflow_work(workflow_id) WHERE kind='cancel_owned';
