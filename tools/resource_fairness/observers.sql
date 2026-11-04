-- Independent committed observations in the disposable fixture database only.
CREATE TABLE fixture_offers (
  seq bigint GENERATED ALWAYS AS IDENTITY, resource text, task text,
  ticket bigint, requested bigint, offered bigint, generation bigint
);
CREATE FUNCTION fixture_observe_offer() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO fixture_offers(resource, task, ticket, requested, offered, generation)
    VALUES (NEW.resource_name, NEW.task_id, NEW.request_seq, NEW.created_at_ms,
            NEW.updated_at_ms, NEW.offer_generation);
  RETURN NULL;
END $$;
CREATE TRIGGER fixture_offer AFTER UPDATE ON task_admission_requests FOR EACH ROW
  WHEN (OLD.state <> 'offered' AND NEW.state = 'offered')
  EXECUTE FUNCTION fixture_observe_offer();
CREATE TABLE fixture_occupancy(resource text, occupied bigint);
CREATE FUNCTION fixture_observe_occupancy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO fixture_occupancy VALUES (NEW.resource_name, NEW.offered + NEW.started);
  RETURN NULL;
END $$;
CREATE TRIGGER fixture_capacity AFTER UPDATE ON admission_resources FOR EACH ROW
  EXECUTE FUNCTION fixture_observe_occupancy();

-- A Start owns the rate debit. Link the committed source and resource update by
-- transaction rather than estimating Start from when the earlier offer existed.
CREATE TABLE fixture_starts (
  xid xid8, task text, resource text, source text, accepted bigint
);
CREATE FUNCTION fixture_observe_start() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE snapshot jsonb := convert_from(NEW.snapshot_bytes,'UTF8')::jsonb;
BEGIN
  INSERT INTO fixture_starts VALUES (pg_current_xact_id(), NEW.task_id, NEW.resource_name,
    snapshot #>> '{source,source_id}', (snapshot #>> '{source,accepted_at}')::bigint);
  RETURN NULL;
END $$;
CREATE TRIGGER fixture_start AFTER UPDATE ON task_admission_requests FOR EACH ROW
  WHEN (OLD.state <> 'possibly_dispatched' AND NEW.state = 'possibly_dispatched')
  EXECUTE FUNCTION fixture_observe_start();

CREATE TABLE fixture_debits (
  seq bigint GENERATED ALWAYS AS IDENTITY, xid xid8, resource text, at_ms bigint, credit bigint
);
CREATE FUNCTION fixture_observe_debit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO fixture_debits(xid, resource, at_ms, credit) VALUES
    (pg_current_xact_id(), NEW.resource_name, NEW.high_water_at_ms, NEW.credit);
  RETURN NULL;
END $$;
CREATE TRIGGER fixture_debit AFTER UPDATE ON admission_resources FOR EACH ROW
  WHEN (NEW.started > OLD.started) EXECUTE FUNCTION fixture_observe_debit();
