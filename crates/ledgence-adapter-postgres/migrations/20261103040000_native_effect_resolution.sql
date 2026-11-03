-- Operator handling is independent of journal confirmation and provider truth.
ALTER TABLE approved_effect_occurrences ADD COLUMN current_assessment_bytes bytea CHECK(current_assessment_bytes IS NULL OR octet_length(current_assessment_bytes) BETWEEN 1 AND 262144);
ALTER TABLE approved_effect_occurrences ADD COLUMN assessment_result_bytes bytea CHECK(assessment_result_bytes IS NULL OR octet_length(assessment_result_bytes) BETWEEN 1 AND 131072);
ALTER TABLE approved_effect_occurrences ADD COLUMN resolution_id text COLLATE "C" CHECK(resolution_id IS NULL OR octet_length(resolution_id) BETWEEN 1 AND 128);
ALTER TABLE approved_effect_occurrences ADD COLUMN disputed boolean NOT NULL DEFAULT false;
ALTER TABLE approved_effect_occurrences ADD CONSTRAINT effect_assessment_projection CHECK((resolution_id IS NULL)=(current_assessment_bytes IS NULL) AND (NOT disputed OR resolution_id IS NOT NULL));
CREATE INDEX approved_effect_inbox_disputed ON approved_effect_occurrences(tenant_id,namespace,updated_at_ms DESC,workflow_id DESC,activation_id DESC,approval_key DESC) WHERE disputed;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT approved_effect_evidence_entries_entry_kind_check;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT approved_effect_evidence_entries_operation_check;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT approved_effect_evidence_entries_entry_bytes_check;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT approved_effect_evidence_entries_command_bytes_check;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT approved_effect_evidence_entries_check;
ALTER TABLE approved_effect_evidence_entries ADD COLUMN acceptance_event_id text COLLATE "C" UNIQUE REFERENCES operator_audit_events(event_id);
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_kind CHECK(entry_kind IN ('claim','observation','assessment'));
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_operation CHECK(operation IN ('claim','confirm','observe','legacy_confirm','resolve'));
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_bound CHECK(octet_length(entry_bytes) BETWEEN 1 AND CASE WHEN entry_kind='assessment' THEN 262144 ELSE 147456 END);
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_command_bound CHECK(command_bytes IS NULL OR octet_length(command_bytes) BETWEEN 1 AND CASE WHEN entry_kind='assessment' THEN 262144 ELSE 147456 END);
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_relationship CHECK(
 (operation='claim' AND entry_kind='claim' AND claim_sequence IS NULL AND claim_kind IS NULL AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL AND acceptance_event_id IS NULL)
 OR (operation IN ('confirm','observe') AND entry_kind='observation' AND claim_sequence IS NOT NULL AND claim_kind='claim' AND claim_sequence<sequence AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL AND acceptance_event_id IS NULL)
 OR (operation='legacy_confirm' AND entry_kind='observation' AND claim_sequence IS NULL AND claim_kind IS NULL AND command_bytes IS NULL AND receipt_bytes IS NULL AND acceptance_event_id IS NULL)
 OR (operation='resolve' AND entry_kind='assessment' AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL AND acceptance_event_id IS NOT NULL AND ((claim_sequence IS NULL AND claim_kind IS NULL) OR (claim_sequence<sequence AND claim_kind='assessment'))));
ALTER TABLE approved_effect_occurrences ADD COLUMN resolution_kind text GENERATED ALWAYS AS (CASE WHEN resolution_id IS NOT NULL THEN 'assessment'::text END) STORED;
ALTER TABLE approved_effect_occurrences ADD CONSTRAINT effect_latest_assessment FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,resolution_kind,resolution_id) REFERENCES approved_effect_evidence_entries(tenant_id,namespace,workflow_id,activation_id,approval_key,entry_kind,command_id);
ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_events_event_kind_check;
ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_events_check;
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_event_kind CHECK(event_kind IN ('approval_accepted','sensitive_response','effect_resolved'));
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_command CHECK((event_kind IN ('approval_accepted','effect_resolved') AND resource_kind='workflow' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='sensitive_response' AND operation_key IS NULL AND command_digest IS NULL));
CREATE TABLE workflow_effect_waits (
 workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
 parking_activation_id text COLLATE "C" NOT NULL REFERENCES workflow_activations(activation_id),
 logical_key text COLLATE "C" NOT NULL CHECK(octet_length(logical_key) BETWEEN 1 AND 128),
 wait_bytes bytea NOT NULL CHECK(octet_length(wait_bytes) BETWEEN 1 AND 4096),
 registered_at_ms bigint NOT NULL CHECK(registered_at_ms BETWEEN 0 AND 253402300799999),
 closed_at_ms bigint CHECK(closed_at_ms BETWEEN registered_at_ms AND 253402300799999),
 wake_bytes bytea CHECK(wake_bytes IS NULL OR octet_length(wake_bytes) BETWEEN 1 AND 147456),
 PRIMARY KEY(workflow_id,parking_activation_id), UNIQUE(parking_activation_id), UNIQUE(workflow_id,logical_key),
 CHECK(wake_bytes IS NULL OR closed_at_ms IS NOT NULL)
);
ALTER TABLE workflow_runs ADD COLUMN native_wait_activation_id text COLLATE "C";
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_native_wait_anchor CHECK(native_wait_activation_id IS NULL OR (external_wait_key IS NULL AND wait_activation_id IS NOT NULL AND native_wait_activation_id=wait_activation_id));
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_native_wait FOREIGN KEY(workflow_id,native_wait_activation_id) REFERENCES workflow_effect_waits(workflow_id,parking_activation_id);
ALTER TABLE workflow_work DROP CONSTRAINT workflow_work_kind_check;
ALTER TABLE workflow_work ADD CONSTRAINT workflow_work_kind_check CHECK(kind IN ('terminal','drain','wait','workflow_terminal','cancel_owned','effect_wait'));
CREATE UNIQUE INDEX workflow_effect_wait_work ON workflow_work(task_id) WHERE kind='effect_wait';
