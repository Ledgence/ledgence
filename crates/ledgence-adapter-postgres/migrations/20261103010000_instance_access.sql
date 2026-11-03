-- NULL retains explicitly operator-trusted deployments. Startup may set this
-- once, but must never clear or replace the persisted authority. Credential and
-- permission rotation changes verifier configuration, not this binding.
ALTER TABLE self_hosted_instance
    ADD COLUMN access_authority_id text COLLATE "C"
    CHECK (access_authority_id IS NULL OR octet_length(access_authority_id) BETWEEN 1 AND 128);
