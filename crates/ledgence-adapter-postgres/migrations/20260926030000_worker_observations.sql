-- One observation per existing session, independent of authority and expiry.
-- Compact list projections avoid reading full snapshots for worker discovery.
CREATE TABLE worker_observations (
    session_id text COLLATE "C" PRIMARY KEY REFERENCES worker_sessions(session_id),
    snapshot_sequence ldg_u64 NOT NULL CHECK (snapshot_sequence > 0),
    received_at_ms bigint NOT NULL CHECK (received_at_ms >= 0),
    display_name text CHECK (octet_length(display_name) BETWEEN 1 AND 128),
    accepting boolean NOT NULL,
    active_consumers bigint NOT NULL CHECK (active_consumers BETWEEN 0 AND 4294967295),
    occupied_process_slots bigint NOT NULL CHECK (occupied_process_slots BETWEEN 0 AND 4294967295),
    detail_state text NOT NULL CHECK (detail_state IN ('available','unsupported_capacity','unavailable')),
    snapshot_bytes bytea NOT NULL CHECK (octet_length(snapshot_bytes) BETWEEN 1 AND 2097152)
);
CREATE INDEX sessions_console ON worker_sessions(tenant_id,namespace,session_id);
CREATE INDEX sessions_console_queue ON worker_sessions(tenant_id,namespace,queue,session_id);
