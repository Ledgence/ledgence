-- Explicit maintenance migration: materialize compact metadata once. Existing
-- payloads are decoded only during backfill and are never persisted as JSONB.
-- PostgreSQL JSON extraction decodes every string and rejects U+0000, which
-- is legal in application data. Normalize only actual NUL escapes in this
-- temporary parsing copy. The negative lookbehind starts at a backslash run;
-- captured pairs remain literal backslashes, so escaped "\\u0000" metadata is
-- unchanged. Queue/callable identifiers prohibit actual control characters.
-- No normalized value is written to an application payload column.
CREATE FUNCTION pg_temp.ldg_console_metadata(payload bytea) RETURNS json
LANGUAGE sql IMMUTABLE STRICT AS $function$
SELECT regexp_replace(convert_from(payload,'UTF8'),
    $pattern$(?<!\\)((?:\\\\)*)\\u0000$pattern$,
    $replacement$\1\\u0001$replacement$, 'g')::json
$function$;
ALTER TABLE workflow_runs ADD COLUMN queue text COLLATE "C";
UPDATE workflow_runs SET queue=(pg_temp.ldg_console_metadata(submission_bytes)->'input'->>'queue');
ALTER TABLE workflow_runs ALTER COLUMN queue SET NOT NULL;
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_queue_length CHECK(octet_length(queue) BETWEEN 1 AND 128);
ALTER TABLE workflow_local_results ADD COLUMN callable text;
UPDATE workflow_local_results SET callable=(pg_temp.ldg_console_metadata(record_bytes)->>'callable');
DROP FUNCTION pg_temp.ldg_console_metadata(bytea);
ALTER TABLE workflow_local_results ALTER COLUMN callable SET NOT NULL;
ALTER TABLE workflow_local_results ADD CONSTRAINT workflow_callable_length CHECK(octet_length(callable) BETWEEN 1 AND 512);
ALTER TABLE workflow_task_links ADD COLUMN creating_revision ldg_u64;
UPDATE workflow_task_links l SET creating_revision=a.revision FROM workflow_activations a WHERE a.activation_id=l.activation_id;
ALTER TABLE workflow_task_links ALTER COLUMN creating_revision SET NOT NULL;
ALTER TABLE owned_workflow_links ADD COLUMN creating_revision ldg_u64;
UPDATE owned_workflow_links l SET creating_revision=a.revision FROM workflow_activations a WHERE a.activation_id=l.creating_activation_id;
ALTER TABLE owned_workflow_links ALTER COLUMN creating_revision SET NOT NULL;

CREATE INDEX workflows_console_discovery ON workflow_runs(tenant_id,namespace,submitted_at_ms DESC,workflow_id DESC) WHERE retiring_at_ms IS NULL;
CREATE INDEX workflows_console_state ON workflow_runs(tenant_id,namespace,state,submitted_at_ms DESC,workflow_id DESC) WHERE retiring_at_ms IS NULL;
CREATE INDEX workflows_console_correlation ON workflow_runs(tenant_id,namespace,correlation_key,submitted_at_ms DESC,workflow_id DESC) WHERE retiring_at_ms IS NULL AND correlation_key IS NOT NULL;
CREATE INDEX workflows_console_parent ON workflow_runs(tenant_id,namespace,parent_workflow_id,submitted_at_ms DESC,workflow_id DESC) WHERE retiring_at_ms IS NULL;
CREATE INDEX workflow_tasks_console_children ON workflow_task_links(workflow_id,creating_revision,command_key,task_id) WHERE NOT is_activation;
CREATE INDEX owned_workflows_console_children ON owned_workflow_links(parent_workflow_id,creating_revision,command_key,child_workflow_id);
CREATE INDEX workflow_waits_console_order ON workflow_waits(workflow_id,registered_at_ms,wait_key);
CREATE INDEX history_console_attempt_times ON task_history(task_id,attempt_id,reason,at_ms) WHERE reason IN ('claimed','dispatch_authorized');
