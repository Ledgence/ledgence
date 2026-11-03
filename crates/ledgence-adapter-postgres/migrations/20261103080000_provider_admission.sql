-- Preserve the approved occurrence independently of execution retention. A single
-- approval may not create multiple resource reservations by changing wait keys.
CREATE TABLE provider_admission_links (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    workflow_id text COLLATE "C" NOT NULL,
    activation_id text COLLATE "C" NOT NULL,
    approval_key text COLLATE "C" NOT NULL,
    origin_activation_id text COLLATE "C" NOT NULL,
    logical_key text COLLATE "C" NOT NULL,
    PRIMARY KEY (tenant_id, namespace, workflow_id, activation_id, approval_key),
    UNIQUE (tenant_id, namespace, workflow_id, origin_activation_id, logical_key),
    FOREIGN KEY (tenant_id, namespace, workflow_id, origin_activation_id, logical_key)
        REFERENCES admission_requests (tenant_id, namespace, workflow_id, origin_activation_id, logical_key)
        ON DELETE RESTRICT
);
