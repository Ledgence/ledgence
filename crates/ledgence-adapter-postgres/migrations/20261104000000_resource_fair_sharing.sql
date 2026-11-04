-- Admission authority is already serialized on the scoped resource. The cursor
-- records the last committed weighted offer, not work completed or time occupied.
ALTER TABLE admission_resources
 ADD COLUMN fair_round integer NOT NULL DEFAULT 0 CHECK (fair_round BETWEEN 0 AND 1000),
 ADD COLUMN fair_group_index integer NOT NULL DEFAULT -1 CHECK (fair_group_index BETWEEN -1 AND 15),
 ADD COLUMN fair_sequence bigint NOT NULL DEFAULT 0 CHECK (fair_sequence>=0),
 ADD CHECK ((fair_round=0) = (fair_group_index=-1));

-- Nullable for registrations from earlier binaries. Enabling/reconfiguring
-- a fair policy resolves the queue from still-live owners under its row lock.
ALTER TABLE admission_requests
 ADD COLUMN short_episode_deadline_ms bigint CHECK(short_episode_deadline_ms>=0 AND short_episode_deadline_ms<deadline_at_ms),
 ADD COLUMN origin_queue text COLLATE "C",
 ADD COLUMN fair_group text COLLATE "C";
ALTER TABLE task_admission_requests
 ADD COLUMN origin_queue text COLLATE "C",
 ADD COLUMN fair_group text COLLATE "C";
-- Order the group probe on its own expression. The global unique
-- request_seq index does not constrain group membership: under a skewed
-- backlog the planner otherwise prefers scanning it to find an empty group.
CREATE INDEX admission_requests_group_ready
 ON admission_requests (tenant_id,namespace,resource_name,fair_group,(request_seq + 0::bigint))
 WHERE state='waiting' AND fair_group IS NOT NULL;
CREATE INDEX task_admissions_group_ready
 ON task_admission_requests (tenant_id,namespace,resource_name,fair_group,(request_seq + 0::bigint))
 WHERE state='waiting' AND available_at_ms=0 AND fair_group IS NOT NULL;

-- A rewait can expire before its original request. Keep this rare index
-- separate so rolling FIFO replicas retain their original deadline index.
CREATE INDEX admission_requests_short_deadline
 ON admission_requests (tenant_id,namespace,resource_name,short_episode_deadline_ms,request_seq)
 WHERE state='waiting' AND short_episode_deadline_ms IS NOT NULL;

-- A configuration change may reclassify requeueable rows. Retired or
-- unrelated history is never part of that scan.
CREATE INDEX admission_requests_reclassify
 ON admission_requests (tenant_id,namespace,resource_name)
 WHERE state IN ('waiting','awaiting_rewait','offered');
CREATE INDEX task_admissions_reclassify
 ON task_admission_requests (tenant_id,namespace,resource_name)
 WHERE state IN ('waiting','preparing','offered');

-- Retained separately from the execution and keyed to the resource lifetime.
CREATE TABLE admission_offer_decisions (
 tenant_id text COLLATE "C" NOT NULL, namespace text COLLATE "C" NOT NULL,
 resource_name text COLLATE "C" NOT NULL, sequence bigint NOT NULL CHECK (sequence>0),
 decision_bytes bytea NOT NULL CHECK (octet_length(decision_bytes) BETWEEN 1 AND 16384),
 PRIMARY KEY (tenant_id,namespace,resource_name,sequence),
 FOREIGN KEY (tenant_id,namespace,resource_name) REFERENCES admission_resources
);
