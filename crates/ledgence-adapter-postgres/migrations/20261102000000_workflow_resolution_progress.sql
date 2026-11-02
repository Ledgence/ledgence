-- Only an incomplete resolution needs a row. Applying/rejecting work clears
-- it in the same transaction; deleting the work also removes the checkpoint.
CREATE TABLE workflow_resolution_progress (
    work_id text COLLATE "C" PRIMARY KEY REFERENCES workflow_work(id) ON DELETE CASCADE,
    bindings bytea NOT NULL CHECK (octet_length(bindings) <= 262144)
);
