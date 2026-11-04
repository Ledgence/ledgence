-- Both tables draw from the same immutable admission request sequence.
-- The expression keeps a probe for a sparse state on its scoped partial
-- index even when most of the global sequence belongs to unrelated history.
CREATE INDEX admission_requests_open_sources
 ON admission_requests (tenant_id,namespace,resource_name,state,(request_seq + 0::bigint))
 WHERE state IN ('possibly_dispatched','unknown_occupied');
CREATE INDEX task_admissions_open_sources
 ON task_admission_requests (tenant_id,namespace,resource_name,state,(request_seq + 0::bigint))
 WHERE state IN ('possibly_dispatched','unknown_occupied');
