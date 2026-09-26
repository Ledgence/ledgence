-- Explicit immutable references, independent of task/history retention.
CREATE TABLE console_programs (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    program_id text COLLATE "C" NOT NULL,
    metadata_bytes bytea NOT NULL CHECK (octet_length(metadata_bytes) BETWEEN 1 AND 8192),
    registered_versions ldg_u64 NOT NULL DEFAULT 0,
    last_registered_at_ms bigint NOT NULL CHECK (last_registered_at_ms >= 0),
    PRIMARY KEY (tenant_id,namespace,program_id)
);
CREATE TABLE console_program_versions (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    program_id text COLLATE "C" NOT NULL,
    version text COLLATE "C" NOT NULL,
    descriptor_bytes bytea NOT NULL CHECK (octet_length(descriptor_bytes) BETWEEN 1 AND 16384),
    manifest_bytes bytea NOT NULL CHECK (octet_length(manifest_bytes) BETWEEN 1 AND 65536),
    metadata_bytes bytea NOT NULL CHECK (octet_length(metadata_bytes) BETWEEN 1 AND 8192),
    registered_at_ms bigint NOT NULL CHECK (registered_at_ms >= 0),
    PRIMARY KEY (tenant_id,namespace,program_id,version),
    FOREIGN KEY (tenant_id,namespace,program_id) REFERENCES console_programs(tenant_id,namespace,program_id)
);
CREATE INDEX console_program_versions_page ON console_program_versions(tenant_id,namespace,program_id,registered_at_ms DESC,version DESC);
