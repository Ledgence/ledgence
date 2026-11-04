-- Task admission retains source authority independently of execution history.
-- Ordinary tasks and worker sessions preserve their existing defaults.
ALTER TABLE worker_sessions ADD COLUMN features_bytes bytea;
ALTER TABLE worker_sessions ADD COLUMN admission_worker_bytes bytea;
ALTER TABLE worker_sessions ADD CONSTRAINT worker_session_admission_feature CHECK ((features_bytes IS NULL) = (admission_worker_bytes IS NULL));

ALTER TABLE tasks ADD COLUMN admission_resource text COLLATE "C";
ALTER TABLE tasks ADD COLUMN admission_ready boolean NOT NULL DEFAULT true;
ALTER TABLE tasks ADD COLUMN admission_deferrals bigint NOT NULL DEFAULT 0;
ALTER TABLE tasks DROP CONSTRAINT tasks_attempt_count_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_attempt_count_check CHECK (attempt_count BETWEEN 0 AND 2147483647);
ALTER TABLE tasks ADD CONSTRAINT tasks_admission_deferrals_check CHECK (admission_deferrals BETWEEN 0 AND attempt_count AND (admission_resource IS NOT NULL OR (admission_deferrals=0 AND admission_ready)));
ALTER TABLE tasks ADD CONSTRAINT tasks_admission_root_only CHECK (admission_resource IS NULL OR (workflow_id IS NULL AND workflow_activation_id IS NULL AND provider_probe_bytes IS NULL));
DROP INDEX tasks_due;
-- Disjoint indexes: every ordinary ready row has just one entry. A legacy
-- worker must not scan offered tasks it cannot run on every long poll.
CREATE INDEX tasks_due_ordinary ON tasks (tenant_id,namespace,queue,available_at_ms,submitted_at_ms,task_id)
    WHERE state='queued' AND cancel_requested_at_ms IS NULL AND dispatch_destination IS NULL AND admission_resource IS NULL;
CREATE INDEX tasks_due_admitted ON tasks (tenant_id,namespace,queue,available_at_ms,submitted_at_ms,task_id)
    WHERE state='queued' AND cancel_requested_at_ms IS NULL AND dispatch_destination IS NULL AND admission_resource IS NOT NULL AND admission_ready;

ALTER TABLE attempts ADD COLUMN task_admission_bytes bytea;
ALTER TABLE attempts DROP CONSTRAINT attempts_generation_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_generation_check CHECK (generation BETWEEN 1 AND 2147483647);
ALTER TABLE attempts DROP CONSTRAINT attempts_state_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_state_check CHECK (state IN ('active','succeeded','failed','cancelled','lost','deferred'));
ALTER TABLE attempts ADD CONSTRAINT attempts_deferred_unstarted CHECK (state<>'deferred' OR (NOT execution_may_have_started));
ALTER TABLE dispatch_intents DROP CONSTRAINT dispatch_intents_generation_check;
ALTER TABLE dispatch_intents ADD CONSTRAINT dispatch_intents_generation_check CHECK (generation BETWEEN 1 AND 2147483647);
ALTER TABLE task_history DROP CONSTRAINT task_history_reason_check;
ALTER TABLE task_history ADD CONSTRAINT task_history_reason_check CHECK (reason IN ('submitted','claimed','dispatch_authorized','cancel_requested','cancelled','report_accepted','cleanup_confirmed','succeeded','failed','retry_scheduled','lease_expired','admission_deferred','admission_stopped'));

CREATE TABLE task_admission_requests (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 task_id text COLLATE "C" NOT NULL, generation bigint NOT NULL CHECK (generation BETWEEN 1 AND 2147483647),
 resource_name text COLLATE "C" NOT NULL,
 -- One ordering sequence across workflow and task requests, scoped at selection.
 request_seq bigint NOT NULL DEFAULT nextval('admission_requests_request_seq_seq') UNIQUE,
 state text NOT NULL CHECK (state IN ('waiting','offered','preparing','possibly_dispatched','unknown_occupied','finished','closed_before_start','administratively_released')),
 revision bigint NOT NULL CHECK (revision>0), created_at_ms bigint NOT NULL CHECK (created_at_ms>=0),
 updated_at_ms bigint NOT NULL CHECK (updated_at_ms>=created_at_ms), available_at_ms bigint NOT NULL CHECK (available_at_ms>=0),
 -- FIFO eligibility advances to zero; the actual last handoff time must
 -- remain available to legacy broker receipts without extending their delay.
 last_rewait_at_ms bigint CHECK(last_rewait_at_ms>=0),
 offer_generation bigint NOT NULL DEFAULT 0 CHECK (offer_generation>=0),
 offer_policy_revision bigint, offer_expires_at_ms bigint, check_at_ms bigint,
 owner_attempt_id text COLLATE "C", worker_bytes bytea, source_id text COLLATE "C",
 snapshot_bytes bytea NOT NULL CHECK (octet_length(snapshot_bytes) BETWEEN 1 AND 262144),
 start_command_bytes bytea, close_command_bytes bytea,
 PRIMARY KEY (tenant_id,namespace,task_id,generation),
 FOREIGN KEY (tenant_id,namespace,resource_name) REFERENCES admission_resources(tenant_id,namespace,resource_name),
 UNIQUE (source_id), UNIQUE (owner_attempt_id)
);
CREATE INDEX task_admissions_fifo ON task_admission_requests(tenant_id,namespace,resource_name,available_at_ms,request_seq) WHERE state='waiting';
CREATE INDEX task_admissions_offer_expiry ON task_admission_requests(tenant_id,namespace,resource_name,offer_expires_at_ms,request_seq) WHERE state='offered';
CREATE INDEX task_admissions_offer_epoch ON task_admission_requests(tenant_id,namespace,resource_name,offer_policy_revision,request_seq) WHERE state IN ('offered','preparing');
CREATE INDEX task_admissions_preparation_due ON task_admission_requests(tenant_id,namespace,resource_name,check_at_ms,request_seq) WHERE state='preparing';
CREATE INDEX task_admissions_source_due ON task_admission_requests(tenant_id,namespace,resource_name,check_at_ms,request_seq) WHERE state='possibly_dispatched';
CREATE INDEX task_admissions_active ON task_admission_requests(task_id,generation) WHERE state IN ('waiting','offered','preparing','possibly_dispatched','unknown_occupied');

ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_event_kind;
ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_command;
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_event_kind CHECK(event_kind IN ('approval_accepted','sensitive_response','effect_resolved','resource_policy_accepted','admission_released','task_admission_released'));
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_command CHECK((event_kind IN ('approval_accepted','effect_resolved','admission_released') AND resource_kind='workflow' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='task_admission_released' AND resource_kind='task' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='resource_policy_accepted' AND resource_kind='resource' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='sensitive_response' AND operation_key IS NULL AND command_digest IS NULL));

ALTER TABLE attempts DROP CONSTRAINT attempts_last_renew_intent_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_last_renew_intent_check CHECK (last_renew_intent IN ('keep_alive','dispatch','prepare'));
ALTER TABLE admission_operator_receipts DROP CONSTRAINT admission_operator_receipts_operation_check;
ALTER TABLE admission_operator_receipts ADD CONSTRAINT admission_operator_receipts_operation_check CHECK (operation IN ('policy','release','task_release'));
