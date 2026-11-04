-- One or two states, each bounded on both retained indexes. Look up
-- provider approval coordinates only for the merged page of occurrences.
WITH page AS MATERIALIZED (
    SELECT candidate.*
    FROM unnest($4::text[]) AS wanted(state)
    CROSS JOIN LATERAL (
        (SELECT (request_seq + 0::bigint) AS seq, r.state, revision, updated_at_ms,
                workflow_id, origin_activation_id, logical_key,
                NULL::text AS task_id, NULL::bigint AS generation
         FROM admission_requests r
         WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
           AND r.state IN ('possibly_dispatched','unknown_occupied')
           AND r.state=wanted.state AND (request_seq + 0::bigint) <= $5
         ORDER BY (request_seq + 0::bigint) DESC LIMIT $6)
        UNION ALL
        (SELECT (request_seq + 0::bigint) AS seq, r.state, revision, updated_at_ms,
                NULL::text, NULL::text, NULL::text, task_id, generation
         FROM task_admission_requests r
         WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
           AND r.state IN ('possibly_dispatched','unknown_occupied')
           AND r.state=wanted.state AND (request_seq + 0::bigint) <= $5
         ORDER BY (request_seq + 0::bigint) DESC LIMIT $6)
    ) AS candidate
    ORDER BY seq DESC LIMIT $6
)
SELECT page.*, approval.activation_id, approval.approval_key
FROM page
LEFT JOIN LATERAL (
    SELECT activation_id, approval_key FROM provider_admission_links p
    WHERE p.tenant_id=$1 AND p.namespace=$2 AND p.workflow_id=page.workflow_id
      AND p.origin_activation_id=page.origin_activation_id AND p.logical_key=page.logical_key
    LIMIT 1
) AS approval ON true
ORDER BY seq DESC
