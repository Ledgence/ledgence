SELECT (SELECT min(due) FROM (
    (SELECT $5::bigint AS due FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state IN ('offered','preparing') AND offer_policy_revision<$4 LIMIT 1
    )
    UNION ALL
    (SELECT offer_expires_at_ms FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='offered'
     ORDER BY offer_expires_at_ms,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT check_at_ms FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='preparing'
     ORDER BY check_at_ms,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT check_at_ms FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='possibly_dispatched'
     ORDER BY check_at_ms,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT available_at_ms FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='waiting' AND available_at_ms>0
     ORDER BY available_at_ms,request_seq LIMIT 1
    )
) AS dues), EXISTS(SELECT 1 FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='waiting' AND available_at_ms=0)
