-- Compact immutable descriptor fields are decoded at write/backfill time, never
-- from application input or per row during discovery. Explicit UTF-8 conversion
-- and JSON member selection depend only on the supplied descriptor bytes.
CREATE FUNCTION ldg_descriptor_program(payload bytea, field text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $function$
SELECT convert_from(payload, 'UTF8')::json->'program'->>field
$function$;
ALTER TABLE tasks
    ADD COLUMN program_id text COLLATE "C",
    ADD COLUMN program_version text COLLATE "C";
ALTER TABLE workflow_runs
    ADD COLUMN program_id text COLLATE "C",
    ADD COLUMN program_version text COLLATE "C";
UPDATE tasks SET program_id=ldg_descriptor_program(descriptor_bytes,'id'),program_version=ldg_descriptor_program(descriptor_bytes,'version');
UPDATE workflow_runs SET program_id=ldg_descriptor_program(controller_bytes,'id'),program_version=ldg_descriptor_program(controller_bytes,'version');
CREATE FUNCTION ldg_execution_program_projection() RETURNS trigger LANGUAGE plpgsql AS $function$
DECLARE descriptor bytea;
BEGIN
    IF TG_TABLE_NAME='tasks' THEN descriptor=NEW.descriptor_bytes; ELSE descriptor=NEW.controller_bytes; END IF;
    NEW.program_id=ldg_descriptor_program(descriptor,'id');
    NEW.program_version=ldg_descriptor_program(descriptor,'version');
    RETURN NEW;
END
$function$;
CREATE TRIGGER task_program_projection BEFORE INSERT OR UPDATE OF descriptor_bytes,program_id,program_version ON tasks
    FOR EACH ROW EXECUTE FUNCTION ldg_execution_program_projection();
CREATE TRIGGER workflow_program_projection BEFORE INSERT OR UPDATE OF controller_bytes,program_id,program_version ON workflow_runs
    FOR EACH ROW EXECUTE FUNCTION ldg_execution_program_projection();

CREATE INDEX tasks_execution_roots ON tasks(tenant_id,namespace,submitted_at_ms DESC,task_id DESC)
    WHERE retiring_at_ms IS NULL AND workflow_activation_id IS NULL AND workflow_id IS NULL;
CREATE INDEX tasks_execution_all ON tasks(tenant_id,namespace,submitted_at_ms DESC,task_id DESC)
    WHERE retiring_at_ms IS NULL AND workflow_activation_id IS NULL;
CREATE INDEX tasks_execution_program ON tasks(tenant_id,namespace,program_id,submitted_at_ms DESC,task_id DESC)
    WHERE retiring_at_ms IS NULL AND workflow_activation_id IS NULL;
CREATE INDEX tasks_execution_version ON tasks(tenant_id,namespace,program_id,program_version,submitted_at_ms DESC,task_id DESC)
    WHERE retiring_at_ms IS NULL AND workflow_activation_id IS NULL;
CREATE INDEX workflows_execution_roots ON workflow_runs(tenant_id,namespace,submitted_at_ms DESC,workflow_id DESC)
    WHERE retiring_at_ms IS NULL AND parent_workflow_id IS NULL;
CREATE INDEX workflows_execution_program ON workflow_runs(tenant_id,namespace,program_id,submitted_at_ms DESC,workflow_id DESC)
    WHERE retiring_at_ms IS NULL;
CREATE INDEX workflows_execution_version ON workflow_runs(tenant_id,namespace,program_id,program_version,submitted_at_ms DESC,workflow_id DESC)
    WHERE retiring_at_ms IS NULL;
CREATE INDEX workflows_execution_queue ON workflow_runs(tenant_id,namespace,queue,submitted_at_ms DESC,workflow_id DESC)
    WHERE retiring_at_ms IS NULL;

-- Registry flags are maintained under the existing per-program registration
-- lock. They represent any-version membership, not editable summary metadata.
CREATE FUNCTION ldg_catalog_kind(payload bytea) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $function$
SELECT coalesce(convert_from(payload, 'UTF8')::json->>'kind', 'unspecified')
$function$;
ALTER TABLE console_program_versions ADD COLUMN declared_kind text COLLATE "C";
UPDATE console_program_versions SET declared_kind=ldg_catalog_kind(metadata_bytes);
ALTER TABLE console_program_versions ALTER COLUMN declared_kind SET NOT NULL;
CREATE FUNCTION ldg_catalog_kind_projection() RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
    NEW.declared_kind=ldg_catalog_kind(NEW.metadata_bytes);
    RETURN NEW;
END
$function$;
CREATE TRIGGER catalog_kind_projection BEFORE INSERT OR UPDATE OF metadata_bytes,declared_kind ON console_program_versions
    FOR EACH ROW EXECUTE FUNCTION ldg_catalog_kind_projection();
ALTER TABLE console_program_versions ADD CONSTRAINT catalog_declared_kind
    CHECK (declared_kind IN ('task','workflow','unspecified'));
CREATE INDEX catalog_version_kind ON console_program_versions(tenant_id,namespace,program_id,declared_kind);
ALTER TABLE console_programs
    ADD COLUMN has_task boolean NOT NULL DEFAULT false,
    ADD COLUMN has_workflow boolean NOT NULL DEFAULT false,
    ADD COLUMN has_unspecified boolean NOT NULL DEFAULT false;
UPDATE console_programs p SET
    has_task=EXISTS(SELECT 1 FROM console_program_versions v WHERE (v.tenant_id,v.namespace,v.program_id)=(p.tenant_id,p.namespace,p.program_id) AND v.declared_kind='task'),
    has_workflow=EXISTS(SELECT 1 FROM console_program_versions v WHERE (v.tenant_id,v.namespace,v.program_id)=(p.tenant_id,p.namespace,p.program_id) AND v.declared_kind='workflow'),
    has_unspecified=EXISTS(SELECT 1 FROM console_program_versions v WHERE (v.tenant_id,v.namespace,v.program_id)=(p.tenant_id,p.namespace,p.program_id) AND v.declared_kind='unspecified');
CREATE INDEX catalog_program_tasks ON console_programs(tenant_id,namespace,program_id) WHERE has_task;
CREATE INDEX catalog_program_workflows ON console_programs(tenant_id,namespace,program_id) WHERE has_workflow;
CREATE INDEX catalog_program_unspecified ON console_programs(tenant_id,namespace,program_id) WHERE has_unspecified;
CREATE FUNCTION ldg_catalog_membership_projection() RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
    UPDATE console_programs p SET
        has_task=EXISTS(SELECT 1 FROM console_program_versions v WHERE (v.tenant_id,v.namespace,v.program_id)=(p.tenant_id,p.namespace,p.program_id) AND v.declared_kind='task'),
        has_workflow=EXISTS(SELECT 1 FROM console_program_versions v WHERE (v.tenant_id,v.namespace,v.program_id)=(p.tenant_id,p.namespace,p.program_id) AND v.declared_kind='workflow'),
        has_unspecified=EXISTS(SELECT 1 FROM console_program_versions v WHERE (v.tenant_id,v.namespace,v.program_id)=(p.tenant_id,p.namespace,p.program_id) AND v.declared_kind='unspecified')
    WHERE (p.tenant_id,p.namespace,p.program_id)=(NEW.tenant_id,NEW.namespace,NEW.program_id);
    RETURN NEW;
END
$function$;
CREATE TRIGGER catalog_membership_projection AFTER INSERT OR UPDATE OF metadata_bytes ON console_program_versions
    FOR EACH ROW EXECUTE FUNCTION ldg_catalog_membership_projection();
