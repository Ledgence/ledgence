SELECT
    NOT EXISTS(SELECT 1 FROM workflow_runs WHERE parent_workflow_id=$1)
    AND NOT EXISTS(SELECT 1 FROM workflow_approvals a WHERE workflow_id=$1 AND effect_evidence_state IN ('legacy','open') AND NOT EXISTS(SELECT 1 FROM approved_effect_occurrences e WHERE e.workflow_id=a.workflow_id AND e.approval_key=a.wait_key AND e.resolution_id IS NOT NULL))
    AND NOT EXISTS(SELECT 1 FROM tasks WHERE workflow_id=$1 AND state IN ('queued','active'))
    AND NOT EXISTS(SELECT 1 FROM workflow_work WHERE (workflow_id=$1 OR child_workflow_id=$1) AND processed_at_ms IS NULL)
    AND NOT EXISTS(SELECT 1 FROM workflow_runs w JOIN workflow_runs root ON root.workflow_id=w.root_workflow_id WHERE w.workflow_id=$1 AND (root.terminal_at_ms IS NULL OR root.terminal_at_ms>$2))
