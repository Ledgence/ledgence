-- Delivery routing is durable and independent of provider SDK configuration.
CREATE TABLE dispatch_routes (
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    queue text COLLATE "C" NOT NULL,
    destination text COLLATE "C" UNIQUE,
    PRIMARY KEY (tenant_id, namespace, queue),
    CHECK (destination IS NULL OR length(destination) BETWEEN 1 AND 128)
);

-- The accepted binding never changes when deployment configuration changes.
ALTER TABLE tasks ADD COLUMN dispatch_destination text COLLATE "C";
DROP INDEX tasks_due;
CREATE INDEX tasks_due ON tasks (tenant_id, namespace, queue, available_at_ms, submitted_at_ms, task_id)
    WHERE state = 'queued' AND cancel_requested_at_ms IS NULL AND dispatch_destination IS NULL;

-- A confirmed publication retains its obligation until claim/cancellation.
CREATE TABLE dispatch_intents (
    task_id text COLLATE "C" PRIMARY KEY REFERENCES tasks(task_id),
    generation bigint NOT NULL CHECK (generation BETWEEN 1 AND 1000),
    destination text COLLATE "C" NOT NULL,
    available_at_ms bigint NOT NULL CHECK (available_at_ms >= 0),
    next_publish_at_ms bigint NOT NULL CHECK (next_publish_at_ms >= 0),
    publication_epoch ldg_u64 NOT NULL DEFAULT 1 CHECK (publication_epoch > 0),
    publication_id text COLLATE "C" NOT NULL UNIQUE,
    lease_token text COLLATE "C",
    lease_until_ms bigint CHECK (lease_until_ms >= 0),
    last_confirmed_at_ms bigint CHECK (last_confirmed_at_ms >= 0),
    CHECK ((lease_token IS NULL) = (lease_until_ms IS NULL))
);
CREATE INDEX dispatch_intents_due ON dispatch_intents (destination, next_publish_at_ms, task_id);

-- Exact operation receipts are independent of the latest consumer cursor.
-- Claimed dispositions store only an attempt reference, never cached authority.
CREATE TABLE dispatch_claim_receipts (
    session_id text COLLATE "C" NOT NULL REFERENCES worker_sessions(session_id),
    consumer_id bigint NOT NULL CHECK (consumer_id BETWEEN 0 AND 4294967295),
    sequence ldg_u64 NOT NULL CHECK (sequence > 0),
    task_id text COLLATE "C" NOT NULL REFERENCES tasks(task_id),
    reply_bytes bytea NOT NULL CHECK (octet_length(reply_bytes) BETWEEN 1 AND 16384),
    PRIMARY KEY (session_id, consumer_id, sequence)
);
