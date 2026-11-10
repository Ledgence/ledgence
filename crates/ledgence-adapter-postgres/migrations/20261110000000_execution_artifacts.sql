-- Execution artifacts are independent of program distribution. No foreign key
-- to an execution/producer may erase immutable provenance or cleanup evidence.
CREATE TABLE artifact_stores (
    singleton boolean PRIMARY KEY CHECK(singleton),
    store_id text COLLATE "C" NOT NULL UNIQUE,
    fingerprint text NOT NULL,
    maintenance_cursor_kind text NOT NULL DEFAULT '',
    maintenance_cursor_id text NOT NULL DEFAULT '',
    maintenance_upload_cursor text COLLATE "C" NOT NULL DEFAULT '',
    maintenance_lane boolean NOT NULL DEFAULT false
);
-- A root retirement changes one row, independently of artifact cardinality.
CREATE TABLE artifact_roots (
    root_kind text NOT NULL CHECK(root_kind IN ('task','workflow')),
    root_id text COLLATE "C" NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    retired_at_ms bigint,
    maintenance_upload_cursor text COLLATE "C" NOT NULL DEFAULT '',
    PRIMARY KEY(root_kind,root_id)
);
CREATE INDEX artifact_roots_scope ON artifact_roots(tenant_id,namespace);
CREATE INDEX artifact_roots_retired ON artifact_roots(root_kind,root_id) WHERE retired_at_ms IS NOT NULL;
CREATE TABLE execution_artifacts (
    artifact_id text COLLATE "C" PRIMARY KEY,
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    root_kind text NOT NULL CHECK(root_kind IN ('task','workflow')),
    root_id text COLLATE "C" NOT NULL,
    producer_bytes bytea NOT NULL CHECK(octet_length(producer_bytes)<=1024),
    artifact_key text COLLATE "C" NOT NULL,
    metadata_bytes bytea NOT NULL CHECK(octet_length(metadata_bytes)<=1024),
    selected_upload_id text COLLATE "C" NOT NULL,
    selection_revision bigint NOT NULL CHECK(selection_revision>0),
    created_at_ms bigint NOT NULL,
    manifest_bytes bytea CHECK(octet_length(manifest_bytes)<=4096),
    UNIQUE(tenant_id,namespace,root_kind,root_id,producer_bytes,artifact_key)
);
CREATE INDEX execution_artifacts_root ON execution_artifacts(tenant_id,namespace,root_kind,root_id,artifact_id);
CREATE TABLE artifact_uploads (
    upload_id text COLLATE "C" PRIMARY KEY,
    artifact_id text COLLATE "C" NOT NULL,
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    root_kind text NOT NULL CHECK(root_kind IN ('task','workflow')),
    root_id text COLLATE "C" NOT NULL,
    revision bigint NOT NULL CHECK(revision>0),
    store_id text COLLATE "C" NOT NULL,
    fingerprint text NOT NULL,
    object_key text COLLATE "C" NOT NULL UNIQUE,
    metadata_bytes bytea NOT NULL CHECK(octet_length(metadata_bytes)<=1024),
    stage text NOT NULL CHECK(stage IN ('prepared','receiving','writing','uncertain','ready','expired_no_write','retired')),
    prepare_expires_at_ms bigint NOT NULL,
    ingress_token text COLLATE "C",
    ingress_expires_at_ms bigint,
    dispatch_expires_at_ms bigint,
    replaced_by text COLLATE "C",
    verified boolean NOT NULL DEFAULT false,
    quiescent boolean NOT NULL DEFAULT true,
    cleanup_token text COLLATE "C",
    cleanup_expires_at_ms bigint,
    deleted boolean NOT NULL DEFAULT false,
    cleanup_eligible boolean NOT NULL DEFAULT false,
    UNIQUE(artifact_id,revision),
    CHECK((ingress_token IS NULL)=(ingress_expires_at_ms IS NULL)),
    CHECK((cleanup_token IS NULL)=(cleanup_expires_at_ms IS NULL)),
    CHECK(NOT verified OR quiescent)
);
CREATE INDEX artifact_uploads_root_pending ON artifact_uploads(tenant_id,namespace,root_kind,root_id,upload_id)
    WHERE stage IN ('prepared','receiving','writing','uncertain') AND NOT deleted;
CREATE INDEX artifact_uploads_cleanup ON artifact_uploads(store_id,upload_id) WHERE NOT deleted;
CREATE INDEX artifact_uploads_binding ON artifact_uploads(artifact_id,upload_id);

CREATE INDEX artifact_uploads_retired_root ON artifact_uploads(root_kind,root_id,upload_id) WHERE NOT deleted;
CREATE INDEX artifact_uploads_replaced ON artifact_uploads(store_id,upload_id) WHERE cleanup_eligible AND NOT deleted;
CREATE INDEX artifact_uploads_purge ON artifact_uploads(upload_id) WHERE deleted;

CREATE INDEX artifact_uploads_root_all ON artifact_uploads(root_kind,root_id,upload_id);
CREATE INDEX execution_artifacts_retention ON execution_artifacts(root_kind,root_id,artifact_id);
