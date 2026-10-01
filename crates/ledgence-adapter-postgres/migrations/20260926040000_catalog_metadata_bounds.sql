-- Match PROGRAM_DISPLAY_METADATA_MAX_BYTES without narrowing accepted text.
-- 57 framing/kind bytes + 2 * (128 name bytes + 4096 description bytes).
-- Quotes, backslashes, tabs and newlines can double the encoded text length.
ALTER TABLE console_programs
    DROP CONSTRAINT console_programs_metadata_bytes_check,
    ADD CONSTRAINT console_programs_metadata_bytes_check
        CHECK (octet_length(metadata_bytes) BETWEEN 1 AND 8505);
ALTER TABLE console_program_versions
    DROP CONSTRAINT console_program_versions_metadata_bytes_check,
    ADD CONSTRAINT console_program_versions_metadata_bytes_check
        CHECK (octet_length(metadata_bytes) BETWEEN 1 AND 8505);
