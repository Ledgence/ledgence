-- Console contract 3 calls each logical controller invocation an entrypoint.
-- This is a transactional read-projection conversion, not an authority change.
-- C2 writers use both canonical key order and SQL object construction order.
-- Locate only the top-level kind token, preserving every other byte.
-- PostgreSQL JSON extraction rejects legal escaped U+0000, even for a sibling
-- field, so do not decode or rebuild the remaining metadata as JSON/JSONB.
CREATE FUNCTION pg_temp.ldg_console_entrypoint_metadata(payload bytea) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE STRICT AS $function$
DECLARE payload_length integer := octet_length(payload);
DECLARE cursor_offset integer := 0;
DECLARE token_start integer;
DECLARE token_end integer;
DECLARE value_offset integer;
DECLARE discriminator_offset integer;
DECLARE depth integer := 0;
DECLARE current_byte integer;
BEGIN
    -- Parsing validation alone accepts escaped NUL; extracting JSON fields does
    -- not. The scan uses byte offsets so UTF-8 content is not repeatedly decoded.
    PERFORM convert_from(payload, 'UTF8')::json;
    WHILE cursor_offset < payload_length LOOP
        current_byte := get_byte(payload, cursor_offset);
        IF current_byte = 34 THEN
            token_start := cursor_offset;
            cursor_offset := cursor_offset + 1;
            WHILE cursor_offset < payload_length LOOP
                current_byte := get_byte(payload, cursor_offset);
                EXIT WHEN current_byte = 34;
                cursor_offset := cursor_offset + CASE WHEN current_byte = 92 THEN 2 ELSE 1 END;
            END LOOP;
            token_end := cursor_offset;
            IF depth = 1 AND substring(payload FROM token_start + 1 FOR token_end - token_start + 1) = convert_to('"kind"', 'UTF8') THEN
                value_offset := token_end + 1;
                WHILE get_byte(payload, value_offset) IN (9,10,13,32) LOOP
                    value_offset := value_offset + 1;
                END LOOP;
                IF get_byte(payload, value_offset) = 58 THEN
                    value_offset := value_offset + 1;
                    WHILE get_byte(payload, value_offset) IN (9,10,13,32) LOOP
                        value_offset := value_offset + 1;
                    END LOOP;
                    IF discriminator_offset IS NOT NULL OR substring(payload FROM value_offset + 1 FOR 7) <> convert_to('"phase"', 'UTF8') THEN
                        RAISE EXCEPTION 'Unexpected Console 2 entrypoint metadata discriminator';
                    END IF;
                    discriminator_offset := value_offset;
                END IF;
            END IF;
        ELSIF current_byte IN (123,91) THEN
            depth := depth + 1;
        ELSIF current_byte IN (125,93) THEN
            depth := depth - 1;
        END IF;
        cursor_offset := cursor_offset + 1;
    END LOOP;
    IF discriminator_offset IS NULL THEN
        RAISE EXCEPTION 'Unexpected Console 2 entrypoint metadata encoding';
    END IF;
    RETURN substring(payload FROM 1 FOR discriminator_offset)
        || convert_to('"entrypoint"', 'UTF8')
        || substring(payload FROM discriminator_offset + 8);
END
$function$;

ALTER TABLE workflow_explorer_records
    DROP CONSTRAINT workflow_explorer_records_kind_check;

UPDATE workflow_explorer_records
SET kind = 'entrypoint',
    metadata_bytes = pg_temp.ldg_console_entrypoint_metadata(metadata_bytes)
WHERE kind = 'phase';

ALTER TABLE workflow_explorer_records
    ADD CONSTRAINT workflow_explorer_records_kind_check
    CHECK (kind IN ('entrypoint','child','fork','local','child_wait','external_wait'));

DROP FUNCTION pg_temp.ldg_console_entrypoint_metadata(bytea);
