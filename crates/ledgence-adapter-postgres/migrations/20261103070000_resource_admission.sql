-- Admission authority and retained one-shot sources are independent of workflow retention.
ALTER TABLE workflow_runs ADD COLUMN has_admissions boolean NOT NULL DEFAULT false;
ALTER TABLE workflow_runs ADD COLUMN admission_wait_activation_id text COLLATE "C";
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_admission_wait_anchor CHECK(admission_wait_activation_id IS NULL OR (external_wait_key IS NULL AND native_wait_activation_id IS NULL AND provider_wait_activation_id IS NULL AND wait_activation_id=admission_wait_activation_id));

CREATE TABLE admission_resources (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 resource_name text COLLATE "C" NOT NULL, revision bigint NOT NULL CHECK(revision>0),
 record_bytes bytea NOT NULL CHECK(octet_length(record_bytes) BETWEEN 1 AND 16384),
 credit bigint CHECK(credit>=0), high_water_at_ms bigint NOT NULL CHECK(high_water_at_ms>=0),
 offered bigint NOT NULL DEFAULT 0 CHECK(offered>=0),
 started bigint NOT NULL DEFAULT 0 CHECK(started>=0),
 unknown bigint NOT NULL DEFAULT 0 CHECK(unknown>=0 AND unknown<=started),
 next_due_ms bigint CHECK(next_due_ms>=0),
 quarantined_due_ms bigint CHECK(quarantined_due_ms>=0),
 PRIMARY KEY(tenant_id,namespace,resource_name)
);
CREATE INDEX admission_resources_due ON admission_resources(tenant_id,namespace,next_due_ms,resource_name) WHERE next_due_ms IS NOT NULL;
CREATE INDEX admission_resources_quarantined_due ON admission_resources(tenant_id,namespace,quarantined_due_ms,resource_name) WHERE quarantined_due_ms IS NOT NULL;
CREATE TABLE admission_requests (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 workflow_id text COLLATE "C" NOT NULL, origin_activation_id text COLLATE "C" NOT NULL,
 logical_key text COLLATE "C" NOT NULL, resource_name text COLLATE "C" NOT NULL,
 request_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
 state text NOT NULL CHECK(state IN ('waiting','offered','awaiting_rewait','possibly_dispatched','unknown_occupied','finished','closed_before_start','administratively_released')),
 revision bigint NOT NULL CHECK(revision>0), created_at_ms bigint NOT NULL CHECK(created_at_ms>=0),
 updated_at_ms bigint NOT NULL CHECK(updated_at_ms>=created_at_ms), deadline_at_ms bigint NOT NULL CHECK(deadline_at_ms>=created_at_ms),
 parking_activation_id text COLLATE "C", offer_generation bigint NOT NULL DEFAULT 0 CHECK(offer_generation>=0),
 offer_policy_revision bigint, offer_expires_at_ms bigint, check_at_ms bigint,
 snapshot_bytes bytea NOT NULL CHECK(octet_length(snapshot_bytes) BETWEEN 1 AND 262144),
 start_command_bytes bytea, finish_command_bytes bytea,
 PRIMARY KEY(tenant_id,namespace,workflow_id,origin_activation_id,logical_key),
 FOREIGN KEY(tenant_id,namespace,resource_name) REFERENCES admission_resources(tenant_id,namespace,resource_name),
 CHECK(state<>'waiting' OR parking_activation_id IS NOT NULL)
);
CREATE INDEX admission_requests_fifo ON admission_requests(tenant_id,namespace,resource_name,request_seq) WHERE state='waiting';
CREATE INDEX admission_requests_deadline ON admission_requests(tenant_id,namespace,resource_name,deadline_at_ms,request_seq) WHERE state='waiting';
CREATE INDEX admission_requests_offer_expiry ON admission_requests(tenant_id,namespace,resource_name,offer_expires_at_ms,request_seq) WHERE state='offered';
CREATE INDEX admission_requests_offer_epoch ON admission_requests(tenant_id,namespace,resource_name,offer_policy_revision,request_seq) WHERE state='offered';
CREATE INDEX admission_requests_source_due ON admission_requests(tenant_id,namespace,resource_name,check_at_ms,request_seq) WHERE state='possibly_dispatched';
CREATE INDEX admission_requests_workflow_active ON admission_requests(workflow_id,request_seq) WHERE state IN ('waiting','offered','possibly_dispatched');

CREATE TABLE admission_operator_receipts (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 request_id text COLLATE "C" NOT NULL, operation text NOT NULL CHECK(operation IN ('policy','release')),
 command_bytes bytea NOT NULL, receipt_bytes bytea NOT NULL, actor_bytes bytea NOT NULL,
 acceptance_event_id text COLLATE "C" NOT NULL,
 PRIMARY KEY(tenant_id,namespace,request_id)
);
CREATE TABLE workflow_admission_waits (
 workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
 parking_activation_id text COLLATE "C" NOT NULL,
 logical_key text COLLATE "C" NOT NULL, wait_bytes bytea NOT NULL,
 registered_at_ms bigint NOT NULL CHECK(registered_at_ms>=0),
 deadline_at_ms bigint NOT NULL CHECK(deadline_at_ms>=registered_at_ms),
 closed_at_ms bigint CHECK(closed_at_ms>=registered_at_ms), wake_bytes bytea,
 PRIMARY KEY(workflow_id,parking_activation_id),
 CHECK(wake_bytes IS NULL OR closed_at_ms IS NOT NULL)
);
CREATE UNIQUE INDEX workflow_admission_current_wait ON workflow_admission_waits(workflow_id) WHERE closed_at_ms IS NULL;
CREATE INDEX workflow_admission_wait_history ON workflow_admission_waits(workflow_id,registered_at_ms,logical_key,parking_activation_id);
ALTER TABLE workflow_work DROP CONSTRAINT workflow_work_kind_check;
ALTER TABLE workflow_work ADD CONSTRAINT workflow_work_kind_check CHECK(kind IN ('terminal','drain','wait','workflow_terminal','cancel_owned','effect_wait','provider_wait','provider_probe_terminal','admission_wait'));

ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_event_kind;
ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_command;
ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_events_resource_kind_check;
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_event_kind CHECK(event_kind IN ('approval_accepted','sensitive_response','effect_resolved','resource_policy_accepted','admission_released'));
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_resource_kind CHECK(resource_kind IN ('task','workflow','resource'));
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_command CHECK((event_kind IN ('approval_accepted','effect_resolved','admission_released') AND resource_kind='workflow' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='resource_policy_accepted' AND resource_kind='resource' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='sensitive_response' AND operation_key IS NULL AND command_digest IS NULL));
ALTER TABLE workflow_explorer_records DROP CONSTRAINT workflow_explorer_records_kind_check;
ALTER TABLE workflow_explorer_records ADD CONSTRAINT workflow_explorer_records_kind_check CHECK(kind IN ('entrypoint','child','fork','local','child_wait','external_wait','native_effect_wait','provider_wait','admission_wait','admission_operation'));
