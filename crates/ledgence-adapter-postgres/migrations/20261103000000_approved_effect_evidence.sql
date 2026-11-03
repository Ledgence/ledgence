-- Only real grants carry an attempt. The earlier upgrade also fenced every
-- historical approved occurrence, including ordinary approved local actions.
ALTER TABLE workflow_approvals
    ADD COLUMN effect_evidence_state text COLLATE "C",
    ADD COLUMN effect_step_key text COLLATE "C",
    ADD COLUMN effect_lookup text COLLATE "C",
    ADD COLUMN effect_confirmed_attempt_id text COLLATE "C";

UPDATE workflow_approvals
SET effect_evidence_state='legacy',
    effect_step_key='_approval:' || encode(sha256(convert_to(wait_key,'UTF8')),'hex')
WHERE effect_dispatch_attempt_id IS NOT NULL;

ALTER TABLE workflow_approvals ADD CONSTRAINT approval_effect_evidence_binding CHECK (
    CASE
        WHEN effect_evidence_state IS NULL THEN
            effect_step_key IS NULL AND effect_lookup IS NULL
            AND effect_confirmed_attempt_id IS NULL AND effect_dispatch_attempt_id IS NULL
        ELSE
            effect_dispatch_claimed AND effect_step_key IS NOT NULL
            AND effect_step_key='_approval:' || encode(sha256(convert_to(wait_key,'UTF8')),'hex')
            AND (effect_lookup IS NULL OR octet_length(effect_lookup) BETWEEN 1 AND 512)
            AND CASE effect_evidence_state
                WHEN 'legacy' THEN effect_dispatch_attempt_id IS NOT NULL
                    AND effect_lookup IS NULL AND effect_confirmed_attempt_id IS NULL
                WHEN 'open' THEN effect_confirmed_attempt_id IS NULL
                WHEN 'confirmed' THEN effect_lookup IS NOT NULL AND effect_confirmed_attempt_id IS NOT NULL
                WHEN 'legacy_confirmed' THEN effect_lookup IS NOT NULL AND effect_confirmed_attempt_id IS NOT NULL
                    AND effect_dispatch_attempt_id IS NOT NULL
                ELSE false
            END
    END
);

CREATE UNIQUE INDEX approval_effect_step ON workflow_approvals(workflow_id,effect_step_key)
    WHERE effect_step_key IS NOT NULL;
CREATE INDEX approval_effect_hold ON workflow_approvals(effect_evidence_state,workflow_id,wait_key)
    WHERE effect_evidence_state IN ('legacy','open');
