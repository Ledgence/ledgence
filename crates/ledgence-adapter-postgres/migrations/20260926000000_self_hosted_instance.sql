-- Binding is permanent for this database. Migration does not bind existing data.
CREATE TABLE self_hosted_instance (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    instance_id text COLLATE "C" NOT NULL CHECK (octet_length(instance_id) BETWEEN 1 AND 128),
    tenant_id text COLLATE "C" NOT NULL CHECK (octet_length(tenant_id) BETWEEN 1 AND 128),
    namespace text COLLATE "C" NOT NULL CHECK (octet_length(namespace) BETWEEN 1 AND 128)
);

-- Scope binding checks include all rows, including retiring workflows and
-- terminal completion subscriptions, which existing partial indexes omit.
CREATE INDEX workflows_instance_scope ON workflow_runs(tenant_id,namespace);
CREATE INDEX completion_instance_scope ON completion_subscriptions(tenant_id,namespace);
