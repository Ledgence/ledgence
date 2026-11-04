SELECT min(request_seq) FROM (
    (SELECT request_seq FROM admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='waiting'
     ORDER BY request_seq LIMIT 1
    )
    UNION ALL
    (SELECT request_seq FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='waiting' AND available_at_ms=0
     ORDER BY request_seq LIMIT 1
    )
) AS eligible
