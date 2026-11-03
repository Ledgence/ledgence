-- Retained proof is independent of execution retention. It never grants execution.
CREATE TABLE approved_effect_occurrences (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    workflow_id text COLLATE "C" NOT NULL,
    activation_id text COLLATE "C" NOT NULL,
    approval_key text COLLATE "C" NOT NULL,
    journal_state text NOT NULL CHECK(journal_state IN ('unconfirmed','confirmed')),
    has_discrepancy boolean NOT NULL,
    effect_revision ldg_u64 NOT NULL CHECK(effect_revision>0),
    entry_count ldg_u64 NOT NULL,
    created_at_ms bigint NOT NULL CHECK(created_at_ms BETWEEN 0 AND 253402300799999),
    updated_at_ms bigint NOT NULL CHECK(updated_at_ms BETWEEN created_at_ms AND 253402300799999),
    binding_bytes bytea NOT NULL CHECK(octet_length(binding_bytes) BETWEEN 1 AND 114688),
    summary_bytes bytea NOT NULL CHECK(octet_length(summary_bytes) BETWEEN 1 AND 8192),
    approval_bytes bytea NOT NULL CHECK(octet_length(approval_bytes) BETWEEN 1 AND 98304),
    program_bytes bytea NOT NULL CHECK(octet_length(program_bytes) BETWEEN 1 AND 16384),
    reference_bytes bytea CHECK(reference_bytes IS NULL OR octet_length(reference_bytes) BETWEEN 1 AND 131072),
    journal_bytes bytea CHECK(journal_bytes IS NULL OR octet_length(journal_bytes) BETWEEN 1 AND 131072),
    PRIMARY KEY(tenant_id,namespace,workflow_id,activation_id,approval_key),
    UNIQUE(workflow_id,approval_key),
    CHECK(entry_count<effect_revision),
    CHECK((journal_state='confirmed')=(journal_bytes IS NOT NULL))
);
CREATE INDEX approved_effect_inbox ON approved_effect_occurrences(tenant_id,namespace,updated_at_ms DESC,workflow_id DESC,activation_id DESC,approval_key DESC);
CREATE INDEX approved_effect_inbox_state ON approved_effect_occurrences(tenant_id,namespace,journal_state,updated_at_ms DESC,workflow_id DESC,activation_id DESC,approval_key DESC);
CREATE INDEX approved_effect_inbox_discrepancy ON approved_effect_occurrences(tenant_id,namespace,updated_at_ms DESC,workflow_id DESC,activation_id DESC,approval_key DESC) WHERE has_discrepancy;
CREATE INDEX approved_effect_inbox_workflow ON approved_effect_occurrences(tenant_id,namespace,workflow_id,updated_at_ms DESC,activation_id DESC,approval_key DESC);

CREATE TABLE approved_effect_evidence_entries (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    workflow_id text COLLATE "C" NOT NULL,
    activation_id text COLLATE "C" NOT NULL,
    approval_key text COLLATE "C" NOT NULL,
    sequence ldg_u64 NOT NULL CHECK(sequence>0),
    entry_kind text NOT NULL CHECK(entry_kind IN ('claim','observation')),
    command_id text COLLATE "C" NOT NULL CHECK(octet_length(command_id) BETWEEN 1 AND 128),
    operation text NOT NULL CHECK(operation IN ('claim','confirm','observe','legacy_confirm')),
    claim_sequence ldg_u64,
    claim_kind text,
    entry_bytes bytea NOT NULL CHECK(octet_length(entry_bytes) BETWEEN 1 AND 147456),
    command_bytes bytea CHECK(command_bytes IS NULL OR octet_length(command_bytes) BETWEEN 1 AND 147456),
    receipt_bytes bytea CHECK(receipt_bytes IS NULL OR octet_length(receipt_bytes) BETWEEN 1 AND 147456),
    PRIMARY KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,sequence),
    UNIQUE(tenant_id,namespace,workflow_id,activation_id,approval_key,entry_kind,command_id),
    UNIQUE(tenant_id,namespace,workflow_id,activation_id,approval_key,sequence,entry_kind),
    FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key) REFERENCES approved_effect_occurrences,
    FOREIGN KEY(tenant_id,namespace,workflow_id,activation_id,approval_key,claim_sequence,claim_kind)
        REFERENCES approved_effect_evidence_entries(tenant_id,namespace,workflow_id,activation_id,approval_key,sequence,entry_kind),
    CHECK((operation='claim' AND entry_kind='claim' AND claim_sequence IS NULL AND claim_kind IS NULL AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL)
       OR (operation IN ('confirm','observe') AND entry_kind='observation' AND claim_sequence IS NOT NULL AND claim_kind='claim' AND claim_sequence<sequence AND command_bytes IS NOT NULL AND receipt_bytes IS NOT NULL)
       OR (operation='legacy_confirm' AND entry_kind='observation' AND claim_sequence IS NULL AND claim_kind IS NULL AND command_bytes IS NULL AND receipt_bytes IS NULL))
);
ALTER TABLE workflow_approvals ADD COLUMN effect_retained_pending boolean NOT NULL DEFAULT false;
UPDATE workflow_approvals SET effect_retained_pending=true WHERE effect_evidence_state IS NOT NULL;
CREATE INDEX workflow_effect_retained_pending ON workflow_approvals(workflow_id,wait_key) WHERE effect_retained_pending;
