-- Queue-wide execution capacity, shared by integrated and targeted acquisition.
CREATE TABLE queue_concurrency_policies (
    tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL, queue text COLLATE "C" NOT NULL,
    revision ldg_u64 NOT NULL CHECK (revision > 0), updated_at_ms bigint NOT NULL,
    maximum bigint CHECK (maximum BETWEEN 0 AND 1000000),
    timeout_ms bigint CHECK (timeout_ms BETWEEN 1 AND 2147483647),
    grace_ms bigint CHECK (grace_ms BETWEEN 1 AND 2147483647),
    occupied bigint NOT NULL DEFAULT 0 CHECK (occupied >= 0),
    uncertain bigint NOT NULL DEFAULT 0 CHECK (uncertain BETWEEN 0 AND occupied),
    CHECK (grace_ms IS NULL OR timeout_ms IS NOT NULL),
    PRIMARY KEY (tenant_id,namespace,queue)
);

-- No foreign key to historical tasks, attempts or sessions. Unresolved source
-- authority and replayable closure must outlive their normal retention periods.
CREATE TABLE queue_concurrency_holds (
    tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL, queue text COLLATE "C" NOT NULL,
    task_id text COLLATE "C" NOT NULL, attempt_id text COLLATE "C" PRIMARY KEY, lease_id text NOT NULL,
    worker_session_id text NOT NULL, worker_identity_bytes bytea, consumer_id bigint NOT NULL, generation bigint NOT NULL,
    revision ldg_u64 NOT NULL DEFAULT 1 CHECK (revision > 0), policy_revision ldg_u64 NOT NULL CHECK(policy_revision>0),
    state text NOT NULL CHECK (state IN ('reserved','running','uncertain','released')),
    acquired_at_ms bigint NOT NULL, deadline_ms bigint, grace_ms bigint,
    stop_requested_at_ms bigint, force_at_ms bigint, released_at_ms bigint,
    release_kind text CHECK(release_kind IN ('confirmed','never_started','operator','timeout')),
    CHECK ((state='released')=(release_kind IS NOT NULL) AND (state='released')=(released_at_ms IS NOT NULL)),
    CHECK (deadline_ms IS NULL OR deadline_ms > acquired_at_ms),
    CHECK (grace_ms IS NULL OR (grace_ms BETWEEN 1 AND 2147483647 AND deadline_ms IS NOT NULL)),
    CHECK (stop_requested_at_ms IS NULL OR (deadline_ms IS NOT NULL AND stop_requested_at_ms >= deadline_ms)),
    CHECK (force_at_ms IS NOT DISTINCT FROM (stop_requested_at_ms + grace_ms)),
    CHECK (released_at_ms IS NULL OR released_at_ms >= GREATEST(acquired_at_ms,stop_requested_at_ms)),
    CHECK (release_kind <> 'timeout' OR (force_at_ms IS NOT NULL AND released_at_ms >= force_at_ms)),
    FOREIGN KEY (tenant_id,namespace,queue) REFERENCES queue_concurrency_policies(tenant_id,namespace,queue)
);
CREATE INDEX queue_holds_page ON queue_concurrency_holds(tenant_id,namespace,queue,attempt_id);
CREATE INDEX queue_holds_open_page ON queue_concurrency_holds(tenant_id,namespace,queue,attempt_id) WHERE state <> 'released';
CREATE INDEX queue_holds_due ON queue_concurrency_holds(tenant_id,namespace,deadline_ms,attempt_id)
    WHERE state <> 'released' AND deadline_ms IS NOT NULL AND stop_requested_at_ms IS NULL;
CREATE INDEX queue_holds_force ON queue_concurrency_holds(tenant_id,namespace,force_at_ms,attempt_id)
    WHERE state <> 'released' AND force_at_ms IS NOT NULL;
-- A store without a configured instance can serve multiple scopes. Keep its
-- global recovery head bounded as well as the independently scoped scanner.
CREATE INDEX queue_holds_due_global ON queue_concurrency_holds(deadline_ms,attempt_id)
    WHERE state <> 'released' AND deadline_ms IS NOT NULL AND stop_requested_at_ms IS NULL;
CREATE INDEX queue_holds_force_global ON queue_concurrency_holds(force_at_ms,attempt_id)
    WHERE state <> 'released' AND force_at_ms IS NOT NULL;
CREATE INDEX queue_cutover_attempts ON attempts(task_id,attempt_id)
    WHERE state <> 'active' AND execution_may_have_started AND quiescence='unconfirmed';

ALTER TABLE admission_operator_receipts DROP CONSTRAINT admission_operator_receipts_operation_check;
ALTER TABLE admission_operator_receipts ADD CONSTRAINT admission_operator_receipts_operation_check
    CHECK(operation IN ('policy','release','task_release','queue_policy','queue_release'));
ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_resource_kind;
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_resource_kind
    CHECK(resource_kind IN ('task','workflow','resource','queue'));

-- Absent rows cannot be locked. A shared, per-queue transaction lock for
-- attempt writes serializes the first policy insertion against in-flight work.
-- Empty polls don't take this lock; uncapped attempts never serialize each other.
CREATE FUNCTION ldg_queue_lock_key(t text, n text, q text) RETURNS bigint
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT hashtextextended(jsonb_build_array(t,n,q)::text, 3987450611)
$$;

-- Insert backstop: even binaries already running before this migration cannot
-- bypass an enabled policy. The new adapter sets a transaction-local exact
-- attempt marker after checking capacity and before inserting this attempt.
CREATE FUNCTION ldg_queue_guard_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    src record; p queue_concurrency_policies%ROWTYPE; scoped boolean := false;
    last_t text; last_n text; last_q text; sampled bigint; cap_deadline bigint; persisted boolean;
BEGIN
    -- Normal dispatch inserts one row. A bulk import by an older orchestrator
    -- still acquires the shared cutover lock once per queue, in stable order.
    FOR src IN
        SELECT i.task_id,i.attempt_id,i.lease_id,i.worker_session_id,i.consumer_id,i.generation,
               t.tenant_id,t.namespace,t.queue,s.admission_worker_bytes
        FROM inserted i JOIN tasks t ON t.task_id=i.task_id
        LEFT JOIN worker_sessions s ON s.session_id=i.worker_session_id
        ORDER BY t.tenant_id,t.namespace,t.queue,i.attempt_id
    LOOP
        IF (last_t,last_n,last_q) IS DISTINCT FROM (src.tenant_id,src.namespace,src.queue) THEN
            last_t := src.tenant_id; last_n := src.namespace; last_q := src.queue;
            PERFORM pg_advisory_xact_lock_shared(ldg_queue_lock_key(last_t,last_n,last_q));
            SELECT * INTO p FROM queue_concurrency_policies WHERE tenant_id=last_t AND namespace=last_n AND queue=last_q FOR UPDATE;
            scoped := FOUND;
        END IF;
        IF NOT scoped THEN CONTINUE; END IF;
        IF current_setting('ledgence.queue_claim_attempt', true) IS DISTINCT FROM src.attempt_id THEN
            RAISE EXCEPTION 'queue policy requires a compatible orchestrator' USING ERRCODE='55000';
        END IF;
        IF p.maximum IS NOT NULL AND p.occupied >= p.maximum THEN
            RAISE EXCEPTION 'queue concurrency exhausted' USING ERRCODE='55000';
        END IF;
        sampled := current_setting('ledgence.queue_claim_time')::bigint;
        cap_deadline := sampled + p.timeout_ms;
        INSERT INTO queue_concurrency_holds(tenant_id,namespace,queue,task_id,attempt_id,lease_id,worker_session_id,
            worker_identity_bytes,consumer_id,generation,policy_revision,state,acquired_at_ms,deadline_ms,grace_ms)
        VALUES(last_t,last_n,last_q,src.task_id,src.attempt_id,src.lease_id,src.worker_session_id,
            src.admission_worker_bytes,src.consumer_id,src.generation,p.revision,'reserved',sampled,cap_deadline,p.grace_ms)
        RETURNING task_id=src.task_id AND attempt_id=src.attempt_id AND lease_id=src.lease_id
            AND tenant_id=last_t AND namespace=last_n AND queue=last_q
            AND worker_identity_bytes IS NOT DISTINCT FROM src.admission_worker_bytes
            AND worker_session_id=src.worker_session_id AND consumer_id=src.consumer_id AND generation=src.generation
            AND state='reserved' AND acquired_at_ms=sampled AND policy_revision=p.revision
            AND deadline_ms IS NOT DISTINCT FROM cap_deadline AND grace_ms IS NOT DISTINCT FROM p.grace_ms INTO persisted;
        IF persisted IS DISTINCT FROM true THEN RAISE EXCEPTION 'queue hold insertion suppressed'; END IF;
        UPDATE queue_concurrency_policies SET occupied=occupied+1 WHERE tenant_id=last_t AND namespace=last_n AND queue=last_q
            RETURNING occupied=p.occupied+1 AND uncertain=p.uncertain INTO persisted;
        IF persisted IS DISTINCT FROM true THEN RAISE EXCEPTION 'queue occupancy increment suppressed'; END IF;
        p.occupied := p.occupied + 1;
    END LOOP;
    RETURN NULL;
END $$;
CREATE TRIGGER queue_guard_insert AFTER INSERT ON attempts REFERENCING NEW TABLE AS inserted
    FOR EACH STATEMENT EXECUTE FUNCTION ldg_queue_guard_insert();

-- One transition primitive updates accounting and the durable source together.
-- Callers hold the policy row before locking/changing the source.
CREATE FUNCTION ldg_queue_transition(a text, next_state text, kind text, sampled bigint) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE h queue_concurrency_holds%ROWTYPE; persisted boolean;
BEGIN
    SELECT * INTO h FROM queue_concurrency_holds WHERE attempt_id=a FOR UPDATE;
    IF NOT FOUND OR h.state='released' OR h.state=next_state THEN RETURN false; END IF;
    UPDATE queue_concurrency_holds SET state=next_state, revision=revision+1,
        released_at_ms=CASE WHEN next_state='released' THEN GREATEST(sampled,h.acquired_at_ms,h.stop_requested_at_ms) END, release_kind=kind
        WHERE attempt_id=a RETURNING state=next_state AND revision=h.revision+1 AND release_kind IS NOT DISTINCT FROM kind INTO persisted;
    IF persisted IS DISTINCT FROM true THEN RAISE EXCEPTION 'queue hold transition suppressed'; END IF;
    UPDATE queue_concurrency_policies SET occupied=occupied-(next_state='released')::int,
        uncertain=uncertain-(h.state='uncertain')::int+(next_state='uncertain')::int
        WHERE tenant_id=h.tenant_id AND namespace=h.namespace AND queue=h.queue
        RETURNING new.occupied=old.occupied-(next_state='released')::int
             AND new.uncertain=old.uncertain-(h.state='uncertain')::int+(next_state='uncertain')::int INTO persisted;
    IF persisted IS DISTINCT FROM true THEN RAISE EXCEPTION 'queue occupancy transition suppressed'; END IF;
    RETURN true;
END $$;

CREATE FUNCTION ldg_queue_attempt_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE t tasks%ROWTYPE; h queue_concurrency_holds%ROWTYPE; sampled bigint;
BEGIN
    SELECT tenant_id,namespace,queue INTO t.tenant_id,t.namespace,t.queue FROM tasks WHERE task_id=NEW.task_id;
    PERFORM pg_advisory_xact_lock_shared(ldg_queue_lock_key(t.tenant_id,t.namespace,t.queue));
    PERFORM 1 FROM queue_concurrency_policies WHERE tenant_id=t.tenant_id AND namespace=t.namespace AND queue=t.queue FOR UPDATE;
    IF NOT FOUND THEN RETURN NEW; END IF;
    SELECT * INTO h FROM queue_concurrency_holds WHERE attempt_id=NEW.attempt_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'missing queue occupancy for live attempt'; END IF;
    sampled := trunc(extract(epoch FROM clock_timestamp())*1000)::bigint;
    IF NEW.execution_may_have_started AND NOT OLD.execution_may_have_started AND
        (h.state='released' OR h.stop_requested_at_ms IS NOT NULL OR h.deadline_ms <= sampled) THEN
        RAISE EXCEPTION 'queue attempt cannot start after capacity deadline' USING ERRCODE='55000';
    END IF;
    IF NEW.quiescence='confirmed' OR (NEW.state<>'active' AND NOT NEW.execution_may_have_started) THEN
        PERFORM ldg_queue_transition(NEW.attempt_id, 'released', CASE WHEN NEW.execution_may_have_started THEN 'confirmed' ELSE 'never_started' END, sampled);
    ELSIF NEW.state<>'active' THEN
        PERFORM ldg_queue_transition(NEW.attempt_id, 'uncertain', NULL, sampled);
    ELSIF NEW.execution_may_have_started THEN
        PERFORM ldg_queue_transition(NEW.attempt_id, 'running', NULL, sampled);
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER queue_guard_transition AFTER UPDATE OF execution_may_have_started,state,quiescence ON attempts
    FOR EACH ROW WHEN ((OLD.execution_may_have_started,OLD.state,OLD.quiescence)
                    IS DISTINCT FROM (NEW.execution_may_have_started,NEW.state,NEW.quiescence))
    EXECUTE FUNCTION ldg_queue_attempt_transition();

ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_event_kind;
ALTER TABLE operator_audit_events DROP CONSTRAINT operator_audit_command;
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_event_kind CHECK(event_kind IN ('approval_accepted','sensitive_response','effect_resolved','resource_policy_accepted','admission_released','task_admission_released','queue_policy_accepted','queue_hold_released'));
ALTER TABLE operator_audit_events ADD CONSTRAINT operator_audit_command CHECK((event_kind IN ('approval_accepted','effect_resolved','admission_released') AND resource_kind='workflow' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='task_admission_released' AND resource_kind='task' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='resource_policy_accepted' AND resource_kind='resource' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind IN ('queue_policy_accepted','queue_hold_released') AND resource_kind='queue' AND operation_key IS NOT NULL AND octet_length(operation_key) BETWEEN 1 AND 128 AND command_digest IS NOT NULL AND octet_length(command_digest)=32) OR (event_kind='sensitive_response' AND operation_key IS NULL AND command_digest IS NULL));

CREATE TABLE queue_concurrency_events (
    attempt_id text COLLATE "C" NOT NULL REFERENCES queue_concurrency_holds(attempt_id),
    event_kind text NOT NULL CHECK(event_kind IN ('stop_requested','timeout_released')),
    at_ms bigint NOT NULL, PRIMARY KEY(attempt_id,event_kind)
);
