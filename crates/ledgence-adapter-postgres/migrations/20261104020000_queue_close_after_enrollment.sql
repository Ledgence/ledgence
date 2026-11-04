CREATE OR REPLACE FUNCTION ldg_queue_attempt_transition() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE t tasks%ROWTYPE; h queue_concurrency_holds%ROWTYPE; cleanup attempts%ROWTYPE; sampled bigint;
BEGIN
    SELECT tenant_id,namespace,queue INTO t.tenant_id,t.namespace,t.queue FROM tasks WHERE task_id=NEW.task_id;
    PERFORM pg_advisory_xact_lock_shared(ldg_queue_lock_key(t.tenant_id,t.namespace,t.queue));
    PERFORM 1 FROM queue_concurrency_policies WHERE tenant_id=t.tenant_id AND namespace=t.namespace AND queue=t.queue FOR UPDATE;
    IF NOT FOUND THEN RETURN NEW; END IF;
    SELECT * INTO h FROM queue_concurrency_holds WHERE attempt_id=NEW.attempt_id;
    IF NOT FOUND THEN
        -- A failed budget authorization never started. If the queue was
        -- enrolled later, there was no live attempt for enrollment to adopt.
        IF OLD.state = 'failed' AND NOT OLD.execution_may_have_started
           AND OLD.budget_denial_bytes IS NOT NULL AND OLD.quiescence = 'unconfirmed' THEN
            cleanup := OLD;
            cleanup.quiescence := 'confirmed';
            IF NEW IS NOT DISTINCT FROM cleanup THEN RETURN NEW; END IF;
        END IF;
        RAISE EXCEPTION 'missing queue occupancy for live attempt';
    END IF;
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
