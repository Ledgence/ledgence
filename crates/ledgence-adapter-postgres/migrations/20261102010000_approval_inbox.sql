-- Project only routing, display and seek fields. The snapshot/decision bytes
-- remain authoritative for inspection, exact argument binding and replay.
ALTER TABLE workflow_approvals
    ADD COLUMN tenant_id text COLLATE "C",
    ADD COLUMN namespace text COLLATE "C",
    ADD COLUMN program_id text COLLATE "C",
    ADD COLUMN program_version text COLLATE "C",
    ADD COLUMN activation_id text COLLATE "C",
    ADD COLUMN action_name text COLLATE "C",
    ADD COLUMN action_version text COLLATE "C",
    ADD COLUMN created_at_ms bigint,
    ADD COLUMN deadline_ms bigint,
    ADD COLUMN status text COLLATE "C";

CREATE FUNCTION ldg_approval_inbox_projection() RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE snapshot json;
BEGIN
    -- PostgreSQL JSON field extraction rejects \u0000 even in unrelated user
    -- arguments. Protect literal escaped backslashes first, then substitute
    -- genuine NUL escapes only in this transient parsing copy. Selected metadata
    -- identifiers forbid control characters. Never normalize the stored bytes.
    snapshot := replace(replace(convert_from(NEW.snapshot_bytes,'UTF8'),
                        $s$\\$s$, $s$\u005c$s$), $s$\u0000$s$, $s$\u0001$s$)::json;
    IF TG_OP = 'INSERT' OR OLD.tenant_id IS NULL THEN
        SELECT w.tenant_id, w.namespace, w.program_id, w.program_version
        INTO STRICT NEW.tenant_id, NEW.namespace, NEW.program_id, NEW.program_version
        FROM workflow_runs w WHERE w.workflow_id=NEW.workflow_id;
        NEW.activation_id := snapshot->>'activation_id';
        NEW.action_name := snapshot#>>'{action,name}';
        NEW.action_version := snapshot#>>'{action,version}';
        NEW.created_at_ms := (snapshot->>'created_at')::bigint;
        NEW.deadline_ms := (snapshot->>'deadline')::bigint;
    END IF;
    NEW.status := snapshot->>'status';
    RETURN NEW;
END
$function$;

-- All existing and older-compatible insert/update paths already persist the
-- snapshot in the same transaction as its wait, decision, expiry or cancellation.
CREATE TRIGGER approval_inbox_projection BEFORE INSERT OR UPDATE OF snapshot_bytes ON workflow_approvals
    FOR EACH ROW EXECUTE FUNCTION ldg_approval_inbox_projection();
UPDATE workflow_approvals SET snapshot_bytes=snapshot_bytes;
ALTER TABLE workflow_approvals
    ALTER COLUMN tenant_id SET NOT NULL, ALTER COLUMN namespace SET NOT NULL,
    ALTER COLUMN program_id SET NOT NULL, ALTER COLUMN program_version SET NOT NULL,
    ALTER COLUMN activation_id SET NOT NULL, ALTER COLUMN action_name SET NOT NULL,
    ALTER COLUMN action_version SET NOT NULL, ALTER COLUMN created_at_ms SET NOT NULL,
    ALTER COLUMN deadline_ms SET NOT NULL, ALTER COLUMN status SET NOT NULL,
    ADD CONSTRAINT approval_inbox_status CHECK (status IN ('pending','approved','rejected','cancelled','expired'));

CREATE INDEX approval_inbox_scope ON workflow_approvals(tenant_id,namespace,deadline_ms,workflow_id,wait_key);
CREATE INDEX approval_inbox_status_order ON workflow_approvals(tenant_id,namespace,status,deadline_ms,workflow_id,wait_key);
CREATE INDEX approval_inbox_program ON workflow_approvals(tenant_id,namespace,program_id,deadline_ms,workflow_id,wait_key);
CREATE INDEX approval_inbox_program_status ON workflow_approvals(tenant_id,namespace,program_id,status,deadline_ms,workflow_id,wait_key);
