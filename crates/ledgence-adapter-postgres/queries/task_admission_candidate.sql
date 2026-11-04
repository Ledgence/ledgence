SELECT task_id,generation,priority,request_seq FROM (
    (SELECT task_id,generation,0 AS priority,request_seq FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state IN ('offered','preparing') AND offer_policy_revision<$4
     ORDER BY offer_policy_revision,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT task_id,generation,1,request_seq FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='offered' AND offer_expires_at_ms<=$5
     ORDER BY offer_expires_at_ms,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT task_id,generation,2,request_seq FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='preparing' AND check_at_ms<=$5
     ORDER BY check_at_ms,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT task_id,generation,2,request_seq FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='possibly_dispatched' AND check_at_ms<=$5
     ORDER BY check_at_ms,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT task_id,generation,3,request_seq FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='waiting' AND available_at_ms>0 AND available_at_ms<=$5
     ORDER BY available_at_ms,request_seq LIMIT 1
    )
    UNION ALL
    (SELECT task_id,generation,4,request_seq FROM task_admission_requests
     WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
       AND state='waiting' AND available_at_ms=0 AND $6
     ORDER BY request_seq LIMIT 1
    )
) AS candidates
     ORDER BY priority,request_seq LIMIT 1
