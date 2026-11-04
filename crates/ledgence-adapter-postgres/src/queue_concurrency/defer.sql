-- A future available_at on a due task denotes an already scheduled capacity
-- handoff. Reuse it without fencing a publication or advancing its deadline.
WITH pending AS MATERIALIZED (
    SELECT available_at_ms FROM dispatch_intents WHERE task_id=$1 AND generation=$2
       AND destination=$3 AND available_at_ms > $4 AND next_publish_at_ms=available_at_ms
       AND lease_token IS NULL AND last_confirmed_at_ms IS NULL FOR UPDATE
), scheduled AS (
    INSERT INTO dispatch_intents(task_id,generation,destination,available_at_ms,next_publish_at_ms,publication_id)
    SELECT $1,$2,$3,$5,$5,'pub_'||gen_random_uuid()::text WHERE NOT EXISTS(SELECT 1 FROM pending)
    ON CONFLICT(task_id) DO UPDATE SET available_at_ms=EXCLUDED.available_at_ms,
        next_publish_at_ms=EXCLUDED.next_publish_at_ms, lease_until_ms=NULL,lease_token=NULL,
        publication_epoch=dispatch_intents.publication_epoch+1,
        publication_id=EXCLUDED.publication_id,last_confirmed_at_ms=NULL
    WHERE dispatch_intents.generation=$2 AND dispatch_intents.destination=$3
    RETURNING available_at_ms, next_publish_at_ms, generation, destination, lease_token,last_confirmed_at_ms
)
SELECT available_at_ms FROM pending UNION ALL SELECT available_at_ms FROM scheduled
 WHERE available_at_ms=$5 AND next_publish_at_ms=$5 AND generation=$2 AND destination=$3
   AND lease_token IS NULL AND last_confirmed_at_ms IS NULL
