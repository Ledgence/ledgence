-- Provider recovery keeps evidence after execution collection. No historical
-- opaque A/B output is reinterpreted as a typed provider fact.
ALTER TABLE tasks ADD COLUMN provider_probe_bytes bytea CHECK(provider_probe_bytes IS NULL OR octet_length(provider_probe_bytes) BETWEEN 1 AND 4096);
ALTER TABLE tasks ADD CONSTRAINT task_provider_probe_identity CHECK(provider_probe_bytes IS NULL OR (workflow_id IS NOT NULL AND workflow_activation_id IS NULL));
ALTER TABLE workflow_approvals ADD COLUMN provider_operation boolean NOT NULL DEFAULT false;
ALTER TABLE workflow_runs ADD COLUMN has_provider_operations boolean NOT NULL DEFAULT false;
ALTER TABLE workflow_runs ADD COLUMN provider_wait_activation_id text COLLATE "C";
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_provider_wait_anchor CHECK(provider_wait_activation_id IS NULL OR (external_wait_key IS NULL AND native_wait_activation_id IS NULL AND wait_activation_id=provider_wait_activation_id));

ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT effect_entry_kind;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT effect_entry_operation;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT effect_entry_bound;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT effect_command_bound;
ALTER TABLE approved_effect_evidence_entries DROP CONSTRAINT effect_entry_relationship;
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_kind CHECK(entry_kind IN ('claim','observation','assessment','provider_dispatch','provider_probe_scheduled','provider_probe_source','provider_report','provider_probe_finished','provider_recovery_stopped'));
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_operation CHECK(operation IN ('claim','confirm','observe','legacy_confirm','resolve','provider'));
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_bound CHECK(octet_length(entry_bytes) BETWEEN 1 AND CASE WHEN entry_kind IN ('claim','observation') THEN 147456 ELSE 262144 END);
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_command_bound CHECK(command_bytes IS NULL OR octet_length(command_bytes) BETWEEN 1 AND CASE WHEN entry_kind IN ('claim','observation') THEN 147456 ELSE 262144 END);
ALTER TABLE approved_effect_evidence_entries ADD CONSTRAINT effect_entry_relationship CHECK(
 (operation='claim' AND entry_kind='claim' AND claim_sequence IS NULL AND claim_kind IS NULL AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL AND acceptance_event_id IS NULL)
 OR (operation IN ('confirm','observe') AND entry_kind='observation' AND claim_sequence IS NOT NULL AND claim_kind='claim' AND claim_sequence<sequence AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL AND acceptance_event_id IS NULL)
 OR (operation='legacy_confirm' AND entry_kind='observation' AND claim_sequence IS NULL AND claim_kind IS NULL AND command_bytes IS NULL AND receipt_bytes IS NULL AND acceptance_event_id IS NULL)
 OR (operation='resolve' AND entry_kind='assessment' AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL AND acceptance_event_id IS NOT NULL AND ((claim_sequence IS NULL AND claim_kind IS NULL) OR (claim_sequence<sequence AND claim_kind='assessment')))
 OR (operation='provider' AND entry_kind IN ('provider_dispatch','provider_probe_scheduled','provider_probe_source','provider_report','provider_probe_finished','provider_recovery_stopped') AND claim_sequence IS NULL AND claim_kind IS NULL AND acceptance_event_id IS NULL));

CREATE TABLE provider_operations (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 workflow_id text COLLATE "C" NOT NULL, activation_id text COLLATE "C" NOT NULL, approval_key text COLLATE "C" NOT NULL,
 instance_id text COLLATE "C" NOT NULL, binding_digest text COLLATE "C" NOT NULL CHECK(octet_length(binding_digest)=71),
 business_namespace text COLLATE "C" NOT NULL CHECK(octet_length(business_namespace) BETWEEN 1 AND 128),
 business_key text COLLATE "C" NOT NULL CHECK(octet_length(business_key) BETWEEN 1 AND 256),
 provider_domain_digest bytea CHECK(provider_domain_digest IS NULL OR octet_length(provider_domain_digest)=32),
 provider_domain_bytes bytea CHECK(provider_domain_bytes IS NULL OR octet_length(provider_domain_bytes) BETWEEN 1 AND 8192),
 provider_key text COLLATE "C" CHECK(provider_key IS NULL OR octet_length(provider_key) BETWEEN 1 AND 256),
 first_due_at_ms bigint NOT NULL CHECK(first_due_at_ms BETWEEN 0 AND 253402300799999),
 deadline_at_ms bigint NOT NULL CHECK(deadline_at_ms BETWEEN first_due_at_ms+1 AND 253402300799999),
 next_due_at_ms bigint CHECK(next_due_at_ms BETWEEN 0 AND deadline_at_ms),
 checks_used integer NOT NULL DEFAULT 0 CHECK(checks_used BETWEEN 0 AND 256),
 generation ldg_u64 NOT NULL DEFAULT 0 CHECK(generation=checks_used),
 active_probe_id text COLLATE "C", active_task_id text COLLATE "C",
 stopped_reason text,
 knowledge text NOT NULL CHECK(knowledge IN ('unknown','pending','requires_action','succeeded','failed','cancelled','disputed')),
 handling text NOT NULL CHECK(handling IN ('unresolved','provider_terminal','operator_closed')),
 needs_review boolean NOT NULL DEFAULT false,
 candidate_revision ldg_u64,
 candidate_bytes bytea CHECK(candidate_bytes IS NULL OR octet_length(candidate_bytes) BETWEEN 1 AND 163840),
 state_bytes bytea NOT NULL CHECK(octet_length(state_bytes) BETWEEN 1 AND 8192),
 current_report_bytes bytea CHECK(current_report_bytes IS NULL OR octet_length(current_report_bytes) BETWEEN 1 AND 262144),
 provider_reference text CHECK(provider_reference IS NULL OR octet_length(provider_reference) BETWEEN 1 AND 1024),
 PRIMARY KEY(tenant_id,namespace,workflow_id,activation_id,approval_key),
 UNIQUE(tenant_id,namespace,business_namespace,business_key),
 UNIQUE(instance_id,provider_domain_digest,provider_key),
 FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key) REFERENCES approved_effect_occurrences,
 CHECK((provider_domain_digest IS NULL)=(provider_domain_bytes IS NULL) AND (provider_domain_digest IS NULL)=(provider_key IS NULL)),
 CHECK((active_probe_id IS NULL)=(active_task_id IS NULL)),
 CHECK(stopped_reason IS NULL OR next_due_at_ms IS NULL),
 CHECK((candidate_revision IS NULL)=(candidate_bytes IS NULL))
);
CREATE INDEX provider_operations_due ON provider_operations(tenant_id,namespace,next_due_at_ms,workflow_id,activation_id,approval_key) WHERE next_due_at_ms IS NOT NULL;
CREATE INDEX provider_operations_workflow ON provider_operations(workflow_id,activation_id,approval_key);
CREATE INDEX provider_operations_active ON provider_operations(workflow_id,active_task_id) WHERE active_task_id IS NOT NULL;
CREATE INDEX provider_operations_handling ON provider_operations(tenant_id,namespace,handling,workflow_id,activation_id,approval_key);
CREATE INDEX provider_operations_knowledge ON provider_operations(tenant_id,namespace,knowledge,workflow_id,activation_id,approval_key);
CREATE INDEX provider_operations_review ON provider_operations(tenant_id,namespace,workflow_id,activation_id,approval_key) WHERE needs_review;

CREATE TABLE provider_probe_tasks (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 workflow_id text COLLATE "C" NOT NULL, activation_id text COLLATE "C" NOT NULL, approval_key text COLLATE "C" NOT NULL,
 probe_id text COLLATE "C" PRIMARY KEY, generation ldg_u64 NOT NULL CHECK(generation>0),
 task_id text COLLATE "C" NOT NULL UNIQUE, live_task_id text COLLATE "C" UNIQUE REFERENCES tasks(task_id),
 assignment_bytes bytea NOT NULL CHECK(octet_length(assignment_bytes) BETWEEN 1 AND 4096),
 descriptor_bytes bytea NOT NULL CHECK(octet_length(descriptor_bytes) BETWEEN 1 AND 16384),
 provider_reference text CHECK(provider_reference IS NULL OR octet_length(provider_reference) BETWEEN 1 AND 1024),
 allocated_at_ms bigint NOT NULL CHECK(allocated_at_ms BETWEEN 0 AND 253402300799999),
 consumed_at_ms bigint CHECK(consumed_at_ms BETWEEN allocated_at_ms AND 253402300799999),
 UNIQUE(tenant_id,namespace,workflow_id,activation_id,approval_key,generation),
 FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key) REFERENCES provider_operations,
 CHECK(live_task_id IS NULL OR live_task_id=task_id), CHECK((consumed_at_ms IS NULL)=(live_task_id IS NOT NULL))
);
CREATE INDEX provider_probe_scope ON provider_probe_tasks(tenant_id,namespace);
CREATE INDEX provider_probe_workflow ON provider_probe_tasks(workflow_id,probe_id) WHERE consumed_at_ms IS NULL;

CREATE TABLE provider_invocations (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 workflow_id text COLLATE "C" NOT NULL, activation_id text COLLATE "C" NOT NULL, approval_key text COLLATE "C" NOT NULL,
 source_id text COLLATE "C" NOT NULL, report_id text COLLATE "C" NOT NULL,
 source_kind text NOT NULL CHECK(source_kind IN ('dispatch','probe')),
 claim_id text COLLATE "C", probe_id text COLLATE "C" REFERENCES provider_probe_tasks(probe_id),
 task_id text COLLATE "C" NOT NULL, attempt_id text COLLATE "C" NOT NULL,
 source_bytes bytea NOT NULL CHECK(octet_length(source_bytes) BETWEEN 1 AND 65536),
 owner_bytes bytea NOT NULL CHECK(octet_length(owner_bytes) BETWEEN 1 AND 4096),
 command_bytes bytea NOT NULL CHECK(octet_length(command_bytes) BETWEEN 1 AND 262144),
 PRIMARY KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,source_id),
 UNIQUE(tenant_id,namespace,workflow_id,activation_id,approval_key,report_id),
 UNIQUE(tenant_id,namespace,workflow_id,activation_id,approval_key,claim_id),
 UNIQUE(probe_id,attempt_id),
 FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key) REFERENCES provider_operations,
 CHECK((source_kind='dispatch' AND claim_id IS NOT NULL AND probe_id IS NULL) OR (source_kind='probe' AND claim_id IS NULL AND probe_id IS NOT NULL))
);
CREATE UNIQUE INDEX provider_one_dispatch ON provider_invocations(tenant_id,namespace,workflow_id,activation_id,approval_key) WHERE source_kind='dispatch';

CREATE TABLE provider_reports (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 workflow_id text COLLATE "C" NOT NULL, activation_id text COLLATE "C" NOT NULL, approval_key text COLLATE "C" NOT NULL,
 source_id text COLLATE "C" NOT NULL, report_id text COLLATE "C" NOT NULL,
 report_bytes bytea NOT NULL CHECK(octet_length(report_bytes) BETWEEN 1 AND 147456),
 receipt_bytes bytea NOT NULL CHECK(octet_length(receipt_bytes) BETWEEN 1 AND 16384),
 entry_sequence ldg_u64 NOT NULL, entry_kind text GENERATED ALWAYS AS ('provider_report'::text) STORED,
 PRIMARY KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,source_id),
 UNIQUE(tenant_id,namespace,workflow_id,activation_id,approval_key,report_id),
 FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,source_id) REFERENCES provider_invocations,
 FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,entry_sequence,entry_kind) REFERENCES approved_effect_evidence_entries(tenant_id,namespace,workflow_id,activation_id,approval_key,sequence,entry_kind)
);
CREATE TABLE provider_terminal_facts (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 workflow_id text COLLATE "C" NOT NULL, activation_id text COLLATE "C" NOT NULL, approval_key text COLLATE "C" NOT NULL,
 fact_kind text NOT NULL CHECK(fact_kind IN ('terminal','reference')),
 fact_digest bytea NOT NULL CHECK(octet_length(fact_digest)=32),
 fact_bytes bytea NOT NULL CHECK(octet_length(fact_bytes) BETWEEN 1 AND 147456),
 first_seen_revision ldg_u64 NOT NULL CHECK(first_seen_revision>0),
 first_report_bytes bytea NOT NULL CHECK(octet_length(first_report_bytes) BETWEEN 1 AND 4096),
 PRIMARY KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,fact_digest),
 FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key) REFERENCES provider_operations
);
CREATE INDEX provider_facts_order ON provider_terminal_facts(tenant_id,namespace,workflow_id,activation_id,approval_key,fact_kind,first_seen_revision);

CREATE TABLE workflow_provider_waits (
 workflow_id text COLLATE "C" NOT NULL REFERENCES workflow_runs(workflow_id),
 parking_activation_id text COLLATE "C" NOT NULL UNIQUE REFERENCES workflow_activations(activation_id),
 logical_key text COLLATE "C" NOT NULL CHECK(octet_length(logical_key) BETWEEN 1 AND 128),
 wait_bytes bytea NOT NULL CHECK(octet_length(wait_bytes) BETWEEN 1 AND 4096),
 registered_at_ms bigint NOT NULL CHECK(registered_at_ms BETWEEN 0 AND 253402300799999),
 closed_at_ms bigint CHECK(closed_at_ms BETWEEN registered_at_ms AND 253402300799999),
 wake_bytes bytea CHECK(wake_bytes IS NULL OR octet_length(wake_bytes) BETWEEN 1 AND 163840),
 PRIMARY KEY(workflow_id,parking_activation_id), UNIQUE(workflow_id,logical_key),
 CHECK(wake_bytes IS NULL OR closed_at_ms IS NOT NULL)
);
ALTER TABLE workflow_runs ADD CONSTRAINT workflow_provider_wait FOREIGN KEY(workflow_id,provider_wait_activation_id) REFERENCES workflow_provider_waits(workflow_id,parking_activation_id);
CREATE INDEX workflow_provider_waits_console_order ON workflow_provider_waits(workflow_id,registered_at_ms,logical_key,parking_activation_id);
ALTER TABLE workflow_work DROP CONSTRAINT workflow_work_kind_check;
ALTER TABLE workflow_work ADD CONSTRAINT workflow_work_kind_check CHECK(kind IN ('terminal','drain','wait','workflow_terminal','cancel_owned','effect_wait','provider_wait','provider_probe_terminal'));
CREATE UNIQUE INDEX workflow_provider_wait_work ON workflow_work(task_id) WHERE kind='provider_wait';
CREATE UNIQUE INDEX workflow_provider_terminal_work ON workflow_work(task_id) WHERE kind='provider_probe_terminal';
ALTER TABLE workflow_explorer_records DROP CONSTRAINT workflow_explorer_records_kind_check;
ALTER TABLE workflow_explorer_records ADD CONSTRAINT workflow_explorer_records_kind_check CHECK(kind IN ('entrypoint','child','fork','local','child_wait','external_wait','native_effect_wait','provider_wait'));
