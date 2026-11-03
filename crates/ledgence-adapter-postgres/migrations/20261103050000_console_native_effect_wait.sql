-- Native recovery waits have a distinct identity and never reuse event keys.
ALTER TABLE workflow_explorer_records DROP CONSTRAINT workflow_explorer_records_kind_check;
ALTER TABLE workflow_explorer_records ADD CONSTRAINT workflow_explorer_records_kind_check
    CHECK (kind IN ('entrypoint','child','fork','local','child_wait','external_wait','native_effect_wait'));

-- Bounded, collision-free Console pagination across both wait namespaces.
DROP INDEX workflow_waits_console_order;
CREATE INDEX workflow_waits_console_order ON workflow_waits(workflow_id,registered_at_ms,kind,wait_key,activation_id);
CREATE INDEX workflow_effect_waits_console_order ON workflow_effect_waits(workflow_id,registered_at_ms,logical_key,parking_activation_id);
