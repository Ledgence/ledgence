-- An approved occurrence can grant permission to send only once.
ALTER TABLE workflow_approvals
    ADD COLUMN effect_dispatch_claimed boolean NOT NULL DEFAULT false,
    ADD COLUMN effect_dispatch_attempt_id text,
    ADD CONSTRAINT approval_dispatch_attempt_requires_claim
        CHECK (effect_dispatch_attempt_id IS NULL OR effect_dispatch_claimed);

-- Prior workers could have performed the approved action without recording a
-- dispatch grant. Fence all previously approved occurrences, including approvals
-- whose resume has not yet run. An unknown historical attempt stays unknown.
UPDATE workflow_approvals SET effect_dispatch_claimed=true WHERE status='approved';
