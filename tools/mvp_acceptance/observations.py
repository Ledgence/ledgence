"""Resource observations with bounded output; PostgreSQL remains the execution authority."""

import statistics
import subprocess

CENSUS = '''SELECT json_build_object(
 'tasks',(SELECT count(*) FROM tasks),
 'attempts',(SELECT count(*) FROM attempts),
 'task_states',(SELECT coalesce(json_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM tasks GROUP BY state)s),
 'workflows',(SELECT count(*) FROM workflow_runs),
 'workflow_states',(SELECT coalesce(json_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM workflow_runs GROUP BY state)s),
 'local_results',(SELECT count(*) FROM workflow_local_results),
 'callbacks',(SELECT count(*) FROM completion_subscriptions),
 'callback_states',(SELECT coalesce(json_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM completion_subscriptions GROUP BY state)s),
 'pending_workflow_work',(SELECT count(*) FROM workflow_work WHERE processed_at_ms IS NULL),
 'dispatch_intents',(SELECT count(*) FROM dispatch_intents),
 'active_attempts',(SELECT count(*) FROM attempts WHERE state='active'),
 'database_bytes',pg_database_size(current_database()))'''


def process_tree(rows, root):
    descendants = {root}
    while True:
        added = {pid for pid, (parent, _) in rows.items() if parent in descendants}
        if added <= descendants:
            break
        descendants.update(added)
    return dict(rss_bytes=sum(rows[pid][1] for pid in descendants if pid in rows),
                descendants=len(descendants) - 1, present=root in rows)


def resources(deployment):
    output = subprocess.check_output(['ps', '-axo', 'pid=,ppid=,rss='], text=True, timeout=5)
    rows = {}
    for line in output.splitlines():
        pid, parent, rss = map(int, line.split())
        rows[pid] = (parent, rss * 1024)
    return {process.label: process_tree(rows, process.process.pid)
            for process in deployment.processes if process.process.poll() is None}


def summarize(samples, duration):
    # Compare medians after initial package/process startup. This is an observation
    # of aggregate service RSS, not a proof of absence of memory leaks.
    mature = [row for row in samples if row['elapsed_seconds'] >= min(30, duration / 3)]
    values = [sum(p['rss_bytes'] for p in row['processes'].values()) for row in mature]
    if len(values) < 6:
        raise AssertionError('at least six mature resource samples are required for disjoint growth windows')
    window = max(1, len(values) // 3)
    early, late = statistics.median(values[:window]), statistics.median(values[-window:])
    return dict(samples=len(samples), mature_samples=len(values),
                peak_service_rss_bytes=max(sum(p['rss_bytes'] for p in row['processes'].values()) for row in samples),
                early_median_service_rss_bytes=early, late_median_service_rss_bytes=late,
                median_growth_bytes=late - early,
                peak_worker_descendants=max(p['descendants'] for row in samples for name,p in row['processes'].items() if name.startswith('worker-')))
