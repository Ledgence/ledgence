-- Payload-free read projection. Its lifetime is the parent workflow's retained
-- lifetime; it preserves recorded child identities after target collection.
-- Cleanup explicitly drains rows in bounded pages before deleting the parent.
CREATE TABLE workflow_explorer_records (
    workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
    revision ldg_u64 NOT NULL,
    kind text COLLATE "C" NOT NULL CHECK (kind IN ('phase','child','fork','local','child_wait','external_wait')),
    record_key text COLLATE "C" NOT NULL,
    activation_id text COLLATE "C" NOT NULL,
    entrypoint text NOT NULL CHECK (octet_length(entrypoint) BETWEEN 1 AND 128),
    metadata_bytes bytea NOT NULL CHECK (octet_length(metadata_bytes) BETWEEN 1 AND 32768),
    PRIMARY KEY (workflow_id,revision,kind,record_key),
    UNIQUE (activation_id,kind,record_key)
);

-- Maintenance backfill only. Application payloads are not copied or modified;
-- NUL normalization permits metadata extraction from otherwise legal JSON data.
CREATE FUNCTION pg_temp.ldg_explorer_json(payload bytea) RETURNS json
LANGUAGE sql IMMUTABLE STRICT AS $function$
SELECT regexp_replace(convert_from(payload,'UTF8'),
    $pattern$(?<!\\)((?:\\\\)*)\\u0000$pattern$,
    $replacement$\1\\u0001$replacement$, 'g')::json
$function$;

INSERT INTO workflow_explorer_records
SELECT a.workflow_id,a.revision,'phase','',a.activation_id,
       pg_temp.ldg_explorer_json(a.context_bytes)->>'continuation',
       convert_to(json_build_object('kind','phase','state',NULL,'availability','available',
         'submitted_at',t.submitted_at_ms,'terminal_at',t.terminal_at_ms,
         'applied_at',NULL,'decision_kind',NULL,'error',NULL,'resumed_activation_id',NULL)::text,'UTF8')
FROM workflow_activations a JOIN tasks t ON t.task_id=a.task_id;

-- applied_at alone also marks rejection. Only recorded successful application
-- establishes effects. Decode the already retained accepted controller outcome.
CREATE TEMP TABLE ldg_explorer_decisions ON COMMIT DROP AS
SELECT a.workflow_id,a.activation_id,a.revision,a.applied_at_ms,
       pg_temp.ldg_explorer_json(s.accepted_command)->'report'->'report'->'outcome'->'output' AS decision,
       resumed.activation_id AS resumed_activation_id
FROM workflow_activations a JOIN tasks t ON t.task_id=a.task_id
JOIN attempts attempt ON attempt.task_id=t.task_id AND attempt.generation=t.attempt_count
JOIN accepted_settlements s ON s.attempt_id=attempt.attempt_id
LEFT JOIN workflow_activations resumed ON resumed.workflow_id=a.workflow_id AND resumed.revision=a.revision+1
WHERE a.applied_at_ms IS NOT NULL AND a.error_bytes IS NULL
  AND EXISTS(SELECT 1 FROM workflow_history h WHERE h.workflow_id=a.workflow_id AND h.activation_id=a.activation_id AND h.reason='decision_applied');

UPDATE workflow_explorer_records r SET metadata_bytes=convert_to(
  (convert_from(r.metadata_bytes,'UTF8')::jsonb || jsonb_build_object(
    'applied_at',d.applied_at_ms,'decision_kind',d.decision->>'kind',
    'resumed_activation_id',CASE WHEN d.decision->>'kind' IN ('continue','suspend','wait') THEN d.resumed_activation_id END))::text,'UTF8')
FROM ldg_explorer_decisions d WHERE r.activation_id=d.activation_id AND r.kind='phase';

INSERT INTO workflow_explorer_records
SELECT d.workflow_id,d.revision,'child_wait','',d.activation_id,p.entrypoint,
       convert_to(json_build_object('kind','child_wait','member_keys',d.decision->'until',
         'resume',d.decision->>'continuation','applied_at',d.applied_at_ms,
         'resumed_activation_id',d.resumed_activation_id)::text,'UTF8')
FROM ldg_explorer_decisions d JOIN workflow_explorer_records p ON p.activation_id=d.activation_id AND p.kind='phase'
WHERE d.decision->>'kind'='suspend';

INSERT INTO workflow_explorer_records
SELECT l.workflow_id,l.creating_revision,'child',l.command_key,l.activation_id,p.entrypoint,
       convert_to(json_build_object('kind','child','key',l.command_key,
         'execution',json_build_object('kind','task','id',l.task_id),
         'program',pg_temp.ldg_explorer_json(t.descriptor_bytes)->'program','fork_key',NULL,
         'availability','available','state',NULL,'submitted_at',t.submitted_at_ms,'terminal_at',t.terminal_at_ms)::text,'UTF8')
FROM workflow_task_links l JOIN tasks t ON t.task_id=l.task_id
JOIN workflow_explorer_records p ON p.activation_id=l.activation_id AND p.kind='phase'
WHERE NOT l.is_activation;

INSERT INTO workflow_explorer_records
SELECT l.parent_workflow_id,l.creating_revision,'child',l.command_key,l.creating_activation_id,p.entrypoint,
       convert_to(json_build_object('kind','child','key',l.command_key,
         'execution',json_build_object('kind','workflow','id',l.child_workflow_id),
         'program',pg_temp.ldg_explorer_json(w.controller_bytes)->'program','fork_key',l.fork_key,
         'availability','available','state',NULL,'submitted_at',w.submitted_at_ms,'terminal_at',w.terminal_at_ms)::text,'UTF8')
FROM owned_workflow_links l JOIN workflow_runs w ON w.workflow_id=l.child_workflow_id
JOIN workflow_explorer_records p ON p.activation_id=l.creating_activation_id AND p.kind='phase';

INSERT INTO workflow_explorer_records
SELECT f.workflow_id,p.revision,'fork',f.fork_key,f.accepting_activation_id,p.entrypoint,
       convert_to(json_build_object('kind','fork','key',f.fork_key,'branch_keys',
         (SELECT json_agg(b->>'key' ORDER BY n) FROM json_array_elements(pg_temp.ldg_explorer_json(f.request_bytes)->'branches') WITH ORDINALITY AS branches(b,n)),
         'accepted_at',f.accepted_at_ms,'accepting_attempt_id',f.accepting_attempt_id)::text,'UTF8')
FROM workflow_forks f JOIN workflow_explorer_records p ON p.activation_id=f.accepting_activation_id AND p.kind='phase';

INSERT INTO workflow_explorer_records
SELECT p.workflow_id,p.revision,'local',l.step_key,l.activation_id,p.entrypoint,
       convert_to(json_build_object('kind','local','key',l.step_key,'callable',l.callable,
         'accepted_at',l.accepted_at_ms,'accepting_attempt_id',l.attempt_id,'observation',NULL)::text,'UTF8')
FROM workflow_local_results l JOIN workflow_explorer_records p ON p.activation_id=l.activation_id AND p.kind='phase';

INSERT INTO workflow_explorer_records
SELECT w.workflow_id,p.revision,'external_wait',w.wait_key,w.activation_id,p.entrypoint,
       convert_to(json_build_object('kind','external_wait','key',w.wait_key,'wait_kind',w.kind,
         'deadline',w.deadline_ms,'registered_at',w.registered_at_ms,'closed_at',w.closed_at_ms,
         'wake_reason',CASE WHEN context.value->'wake'->>'key'=w.wait_key THEN context.value->'wake'->>'kind' END,
         'resumed_activation_id',CASE WHEN context.value->'wake'->>'key'=w.wait_key THEN resumed.activation_id END)::text,'UTF8')
FROM workflow_waits w JOIN workflow_explorer_records p ON p.activation_id=w.activation_id AND p.kind='phase'
LEFT JOIN workflow_activations resumed ON resumed.workflow_id=w.workflow_id AND resumed.revision=p.revision+1
LEFT JOIN LATERAL (SELECT pg_temp.ldg_explorer_json(resumed.context_bytes) AS value) context ON true;

DROP FUNCTION pg_temp.ldg_explorer_json(bytea);
