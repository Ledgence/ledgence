-- Each immutable policy and ledger belongs to one submitted root. Members
-- survive execution-detail retirement so its operator can still inspect usage.
CREATE TABLE run_budgets (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 root_kind text NOT NULL CHECK(root_kind IN ('task','workflow')), root_id text COLLATE "C" NOT NULL,
 policy_bytes bytea NOT NULL CHECK(octet_length(policy_bytes) BETWEEN 1 AND 4096),
 calls bigint NOT NULL DEFAULT 0 CHECK(calls>=0),
 delegations bigint NOT NULL DEFAULT 0 CHECK(delegations>=0),
 deadline_reached boolean NOT NULL DEFAULT false,
 last_sequence bigint NOT NULL DEFAULT 0 CHECK(last_sequence>=0),
 PRIMARY KEY (tenant_id,namespace,root_kind,root_id)
);
CREATE TABLE run_budget_members (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 kind text NOT NULL CHECK(kind IN ('task','workflow')), member_id text COLLATE "C" NOT NULL,
 root_kind text NOT NULL, root_id text COLLATE "C" NOT NULL,
 ancestor_workflows text[] NOT NULL DEFAULT '{}' CHECK(cardinality(ancestor_workflows)<=17 AND array_position(ancestor_workflows,NULL) IS NULL),
 stopped_at_ms bigint CHECK(stopped_at_ms>=0),
 PRIMARY KEY(tenant_id,namespace,kind,member_id),
 FOREIGN KEY (tenant_id,namespace,root_kind,root_id) REFERENCES run_budgets
);
CREATE TABLE run_budget_entries (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 root_kind text NOT NULL, root_id text COLLATE "C" NOT NULL, sequence bigint NOT NULL CHECK(sequence>0),
 denial_request_digest bytea CHECK (octet_length(denial_request_digest)=32),
 action text NOT NULL CHECK(action IN ('call','delegation')),
 source_id text COLLATE "C" NOT NULL,
 entry_bytes bytea NOT NULL CHECK(octet_length(entry_bytes) BETWEEN 1 AND 16384),
 PRIMARY KEY(tenant_id,namespace,root_kind,root_id,sequence),
 UNIQUE(tenant_id,namespace,root_kind,root_id,action,source_id),
 FOREIGN KEY(tenant_id,namespace,root_kind,root_id) REFERENCES run_budgets
);
ALTER TABLE workflow_runs ADD COLUMN has_run_budget boolean NOT NULL DEFAULT false;
ALTER TABLE attempts ADD COLUMN budget_denial_bytes bytea;

-- A refused interactive fork has its own immutable identity even when no child exists.
CREATE TABLE run_budget_fork_refusals (
 workflow_id text COLLATE "C" NOT NULL, fork_key text COLLATE "C" NOT NULL,
 accepting_attempt text NOT NULL, request_digest bytea NOT NULL CHECK(octet_length(request_digest)=32),
 denial_bytes bytea NOT NULL, PRIMARY KEY(workflow_id,fork_key)
);
