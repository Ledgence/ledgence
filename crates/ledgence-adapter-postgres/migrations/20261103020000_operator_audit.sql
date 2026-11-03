-- Metadata only: no action arguments, reviewer labels, reasons or response bodies.
-- Acceptance events survive execution collection; observations have their own lane.
CREATE TABLE operator_audit_events (
    event_id text COLLATE "C" PRIMARY KEY CHECK (octet_length(event_id) BETWEEN 1 AND 128),
    event_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    event_kind text NOT NULL CHECK (event_kind IN ('approval_accepted','sensitive_response')),
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    authority_id text COLLATE "C" NOT NULL,
    instance_id text COLLATE "C" NOT NULL,
    resource_kind text NOT NULL CHECK (resource_kind IN ('task','workflow')),
    resource_id text COLLATE "C" NOT NULL CHECK (octet_length(resource_id) BETWEEN 1 AND 128),
    operation_key text COLLATE "C",
    recorded_at_ms bigint NOT NULL CHECK (recorded_at_ms BETWEEN 0 AND 253402300799999),
    event_bytes bytea NOT NULL CHECK (octet_length(event_bytes) BETWEEN 1 AND 8192),
    command_digest bytea,
    request_id text CHECK (request_id IS NULL OR octet_length(request_id) BETWEEN 1 AND 128),
    CHECK ((event_kind='approval_accepted' AND resource_kind='workflow' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32)
        OR (event_kind='sensitive_response' AND operation_key IS NULL AND command_digest IS NULL))
);
CREATE INDEX operator_audit_scope ON operator_audit_events(tenant_id,namespace,event_seq DESC);
CREATE INDEX operator_audit_resource ON operator_audit_events(tenant_id,namespace,resource_kind,resource_id,event_seq DESC);
CREATE UNIQUE INDEX operator_audit_acceptance ON operator_audit_events(tenant_id,namespace,resource_id,operation_key) WHERE event_kind='approval_accepted';
CREATE INDEX operator_audit_observation_retention ON operator_audit_events(tenant_id,namespace,recorded_at_ms,event_id) WHERE event_kind='sensitive_response';
ALTER TABLE workflow_approvals ADD COLUMN decision_audit_event_id text COLLATE "C" UNIQUE REFERENCES operator_audit_events(event_id);
ALTER TABLE workflow_approvals ADD CONSTRAINT workflow_approvals_audit_decision CHECK (decision_audit_event_id IS NULL OR decision_bytes IS NOT NULL);
ALTER TABLE retention_turn DROP CONSTRAINT retention_turn_lane_check;
ALTER TABLE retention_turn ADD CONSTRAINT retention_turn_lane_check CHECK(lane BETWEEN 0 AND 3);
