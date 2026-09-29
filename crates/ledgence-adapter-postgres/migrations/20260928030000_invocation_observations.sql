-- Optional, bounded metadata alongside the accepted report. No payload backfill:
-- pre-upgrade attempts explicitly have no recorded measurements. Existing
-- settlement retention removes these bytes with the corresponding report.
ALTER TABLE accepted_settlements ADD COLUMN observations_bytes bytea;
ALTER TABLE accepted_settlements ADD CONSTRAINT bounded_invocation_observations
    CHECK (observations_bytes IS NULL OR octet_length(observations_bytes) <= 131072);
