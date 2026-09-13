-- Ledgence-owned schema. All payload bytes use the orchestration JSON contract.
-- Operation counters cover the entire portable u64 range without coercion.
CREATE DOMAIN ldg_u64 AS numeric
    CHECK (VALUE >= 0 AND VALUE <= 18446744073709551615 AND VALUE = trunc(VALUE));

CREATE TABLE worker_sessions (
    session_id text COLLATE "C" PRIMARY KEY,
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    queue text COLLATE "C" NOT NULL,
    concurrency bigint NOT NULL CHECK (concurrency BETWEEN 1 AND 4294967295),
    expires_at_ms bigint NOT NULL CHECK (expires_at_ms >= 0)
);

CREATE TABLE tasks (
    task_id text COLLATE "C" PRIMARY KEY,
    run_id text COLLATE "C" NOT NULL UNIQUE,
    tenant_id text COLLATE "C" NOT NULL,
    namespace text COLLATE "C" NOT NULL,
    queue text COLLATE "C" NOT NULL,
    idempotency_key text COLLATE "C" NOT NULL,
    correlation_key text COLLATE "C",
    input_bytes bytea NOT NULL CHECK (octet_length(input_bytes) BETWEEN 1 AND 2097152),
    descriptor_bytes bytea NOT NULL CHECK (octet_length(descriptor_bytes) BETWEEN 1 AND 2097152),
    origin_trace_bytes bytea CHECK (octet_length(origin_trace_bytes) BETWEEN 1 AND 4096),
    state text NOT NULL CHECK (state IN ('queued', 'active', 'succeeded', 'failed', 'cancelled')),
    submitted_at_ms bigint NOT NULL CHECK (submitted_at_ms >= 0),
    available_at_ms bigint NOT NULL CHECK (available_at_ms >= 0),
    terminal_at_ms bigint CHECK (terminal_at_ms >= 0),
    current_attempt_id text COLLATE "C",
    attempt_count bigint NOT NULL CHECK (attempt_count BETWEEN 0 AND 1000),
    cancel_requested_at_ms bigint CHECK (cancel_requested_at_ms >= 0),
    history_sequence ldg_u64 NOT NULL DEFAULT 0,
    next_expiry_ms bigint CHECK (next_expiry_ms >= 0),
    CONSTRAINT tasks_submission_key UNIQUE (tenant_id, namespace, idempotency_key),
    CHECK ((state = 'active') = (current_attempt_id IS NOT NULL)),
    CHECK ((state = 'active') = (next_expiry_ms IS NOT NULL)),
    CHECK (state <> 'active' OR attempt_count > 0),
    CHECK ((state IN ('succeeded', 'failed', 'cancelled')) = (terminal_at_ms IS NOT NULL))
);

CREATE TABLE attempts (
    attempt_id text COLLATE "C" PRIMARY KEY,
    task_id text COLLATE "C" NOT NULL REFERENCES tasks(task_id),
    generation bigint NOT NULL CHECK (generation BETWEEN 1 AND 1000),
    lease_id text COLLATE "C" NOT NULL UNIQUE,
    -- Historical owner identity is retained independently of cursor lifetime.
    worker_session_id text COLLATE "C" NOT NULL,
    consumer_id bigint NOT NULL CHECK (consumer_id BETWEEN 0 AND 4294967295),
    event_source text COLLATE "C" NOT NULL,
    event_id text COLLATE "C" NOT NULL,
    event_bytes bytea NOT NULL CHECK (octet_length(event_bytes) BETWEEN 1 AND 2097152),
    expires_at_ms bigint NOT NULL CHECK (expires_at_ms >= 0),
    deadline_ms bigint NOT NULL CHECK (deadline_ms >= 0),
    authority_deadline_ms bigint NOT NULL CHECK (authority_deadline_ms >= 0),
    state text NOT NULL CHECK (state IN ('active', 'succeeded', 'failed', 'cancelled', 'lost')),
    execution_may_have_started boolean NOT NULL,
    last_renew_sequence ldg_u64 CHECK (last_renew_sequence > 0),
    last_renew_intent text CHECK (last_renew_intent IN ('keep_alive', 'dispatch')),
    quiescence text NOT NULL CHECK (quiescence IN ('confirmed', 'unconfirmed')),
    finished_at_ms bigint CHECK (finished_at_ms >= 0),
    UNIQUE (task_id, attempt_id),
    UNIQUE (task_id, generation),
    UNIQUE (event_source, event_id),
    CHECK ((last_renew_sequence IS NULL) = (last_renew_intent IS NULL)),
    CHECK ((state = 'active') = (finished_at_ms IS NULL))
);

ALTER TABLE tasks ADD CONSTRAINT tasks_current_attempt
    FOREIGN KEY (task_id, current_attempt_id) REFERENCES attempts(task_id, attempt_id);

CREATE TABLE consumer_cursors (
    session_id text COLLATE "C" NOT NULL REFERENCES worker_sessions(session_id),
    consumer_id bigint NOT NULL CHECK (consumer_id BETWEEN 0 AND 4294967295),
    sequence ldg_u64 CHECK (sequence > 0),
    task_id text COLLATE "C",
    attempt_id text COLLATE "C",
    PRIMARY KEY (session_id, consumer_id),
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts(task_id, attempt_id) MATCH FULL,
    CHECK (sequence IS NOT NULL OR (task_id IS NULL AND attempt_id IS NULL))
);

CREATE TABLE accepted_settlements (
    attempt_id text COLLATE "C" PRIMARY KEY REFERENCES attempts(attempt_id),
    operation_id text COLLATE "C" NOT NULL,
    accepted_command bytea NOT NULL CHECK (octet_length(accepted_command) BETWEEN 1 AND 8388608),
    accepted_at bigint NOT NULL CHECK (accepted_at >= 0)
);

CREATE TABLE task_history (
    task_id text COLLATE "C" NOT NULL REFERENCES tasks(task_id),
    sequence ldg_u64 NOT NULL CHECK (sequence > 0),
    attempt_id text COLLATE "C",
    at_ms bigint NOT NULL CHECK (at_ms >= 0),
    reason text NOT NULL CHECK (reason IN (
        'submitted', 'claimed', 'dispatch_authorized', 'cancel_requested', 'cancelled',
        'report_accepted', 'cleanup_confirmed', 'succeeded', 'failed', 'retry_scheduled', 'lease_expired'
    )),
    PRIMARY KEY (task_id, sequence),
    FOREIGN KEY (task_id, attempt_id) REFERENCES attempts(task_id, attempt_id)
);

CREATE INDEX tasks_due ON tasks (tenant_id, namespace, queue, available_at_ms, submitted_at_ms, task_id)
    WHERE state = 'queued' AND cancel_requested_at_ms IS NULL;
CREATE INDEX tasks_expiry ON tasks (next_expiry_ms, task_id) WHERE state = 'active';
CREATE UNIQUE INDEX attempts_one_active ON attempts (task_id) WHERE state = 'active';
CREATE INDEX consumer_cursors_assignment ON consumer_cursors (task_id, attempt_id);
CREATE INDEX task_history_attempt ON task_history (task_id, attempt_id);
