#!/usr/bin/env python3
"""Freeze candidate jobs/artifacts and verify promotion proof across reruns (MIT)."""
import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from framework_qualification import FRAMEWORKS, archive_identity, read_json, require, validate_reports, write_json
from verify import checksum

CONSOLE_JOB = 'Console candidate bundle (Linux x86_64)'
CONSOLE_ARTIFACT = 'candidate-console-linux-x86_64'
REQUIRED_JOBS = ('Headless candidate bundle (ubuntu-24.04)', 'Headless candidate bundle (macos-15)',
                 CONSOLE_JOB, *(f'Candidate framework recovery ({sdk})' for sdk in FRAMEWORKS))
CONSOLE_STEPS = (
    'Build and verify candidate artifacts with relocated Console', 'Verify candidate archive checksum',
    'Rehearse migrations and restore with published predecessor bundles',
    'Qualify candidate program profiles against released 0.9',
    'Qualify artifact S3 adapter and persisted objects after restart', 'Retain verified Console candidate',
)
FRAMEWORK_STEPS = ('Verify candidate source and runtime before framework execution',
                   'Qualify extracted candidate SDK and worker recovery', 'Retain candidate framework qualification')
SNAPSHOT_FORMAT = 2
STEP_FIELDS = ('number', 'name', 'status', 'conclusion', 'started_at', 'completed_at')
EXECUTION_FIELDS = ('run_id', 'head_sha', 'name', 'status', 'conclusion', 'runner_id', 'runner_name',
                    'runner_group_id', 'runner_group_name')


def timestamp(value):
    require(isinstance(value, str) and re.fullmatch(
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)', value),
        'invalid UTC execution timestamp')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(parsed.utcoffset() == timedelta(0), 'UTC execution timestamp required')
    return parsed


def execution_fingerprint(job):
    start, end = timestamp(job.get('started_at')), timestamp(job.get('completed_at'))
    require(start <= end, 'reversed successful execution window')
    steps = job.get('steps')
    require(isinstance(steps, list) and 0 < len(steps) <= 1000, 'missing or excessive execution steps')
    numbers, normalized = set(), []
    for step in steps:
        number = step.get('number')
        require(type(number) is int and number > 0 and number not in numbers, 'ambiguous execution steps')
        numbers.add(number)
        first, last = step.get('started_at'), step.get('completed_at')
        if first is None or last is None:
            require(first is None and last is None and step.get('conclusion') == 'skipped', 'missing execution step timestamps')
        else:
            first, last = timestamp(first), timestamp(last)
            require(start <= first <= last <= end, 'execution step outside job window')
        normalized.append((number, step.get('name'), step.get('status'), step.get('conclusion'), first, last))
    identity = [job.get(key) for key in EXECUTION_FIELDS]
    # Hosted copies rewrite the default runner-group ID from 0 to null. Keep
    # its raw value in the frozen audit, but compare those two forms equally.
    group_index = EXECUTION_FIELDS.index('runner_group_id')
    if type(identity[group_index]) is int and identity[group_index] == 0:
        identity[group_index] = None
    return tuple(identity), start, end, sorted(normalized)


def producing_job(latest, jobs):
    require(latest.get('status') == 'completed' and latest.get('conclusion') == 'success',
            'latest required execution did not pass')
    fingerprint = execution_fingerprint(latest)
    # GitHub copies successful records into later attempts with new IDs but
    # unchanged execution times/steps. Only the first identical execution ran.
    matches = [job for job in jobs if job['name'] == latest['name']
               and job.get('status') == 'completed' and job.get('conclusion') == 'success'
               and job['run_attempt'] <= latest['run_attempt'] and execution_fingerprint(job) == fingerprint]
    require(matches, 'missing original producing execution')
    attempt = min(job['run_attempt'] for job in matches)
    originals = [job for job in matches if job['run_attempt'] == attempt]
    require(len(originals) == 1, 'ambiguous original producing execution')
    return originals[0]


def execution_binding(latest, producer):
    require(all(type(value) is int and value > 0 for value in
                (latest.get('id'), producer.get('id'), producer.get('run_attempt'))), 'invalid producing execution identity')
    return dict(latest_job_id=latest['id'], producer_job_id=producer['id'], producing_attempt=producer['run_attempt'])


def execution_artifact(artifacts, prefix, latest, producer, run_id, source):
    start, end = timestamp(latest['started_at']), timestamp(latest['completed_at'])
    pattern = re.compile(re.escape(prefix) + r'([1-9][0-9]*)')
    eligible = [artifact for artifact in artifacts if pattern.fullmatch(artifact.get('name', ''))
                and start <= timestamp(artifact.get('created_at')) <= end]
    require(len(eligible) == 1, f'missing or ambiguous artifact in producing execution: {prefix}')
    expected = prefix + str(producer['run_attempt'])
    require(eligible[0]['name'] == expected, 'artifact attempt does not identify original producing execution')
    return choose_artifact(artifacts, expected, run_id, source, producer)


def gh(endpoint):
    result = subprocess.run(['gh', 'api', endpoint], check=True, capture_output=True, text=True, timeout=60)
    require(len(result.stdout) <= 16_000_000, 'GitHub metadata response exceeds bounds')
    return json.loads(result.stdout)


def pages(endpoint, key, fetch=gh):
    result = []
    total = None
    for page in range(1, 101):
        data = fetch(endpoint + ('&' if '?' in endpoint else '?') + f'per_page=100&page={page}')
        records = data.get(key)
        require(isinstance(records, list) and len(records) <= 100, 'invalid paginated GitHub metadata')
        require(type(data.get('total_count')) is int and 0 <= data['total_count'] <= 10000, 'invalid GitHub metadata count')
        if total is None:
            total = data['total_count']
        require(total == data['total_count'], 'GitHub metadata changed during pagination; retry')
        result.extend(records)
        if len(records) < 100:
            require(len(result) == total, 'incomplete GitHub pagination')
            return result
    raise ValueError('GitHub metadata pagination exceeds bounds')


def artifact_name(framework, source, attempt):
    return f'candidate-framework-{framework}-{source}-{attempt}'


def choose_artifact(artifacts, name, run_id, source, job):
    matches = [a for a in artifacts if a.get('name') == name]
    require(len(matches) == 1, f'missing or ambiguous artifact: {name}')
    artifact = matches[0]
    require(artifact.get('expired') is False, f'expired artifact: {name}')
    run = artifact.get('workflow_run', {})
    require(run.get('id') == run_id and run.get('head_sha') == source, 'artifact belongs to a different run/source')
    require(type(artifact.get('id')) is int and artifact['id'] > 0, 'invalid artifact ID')
    require(timestamp(job['started_at']) <= timestamp(artifact.get('created_at')) <= timestamp(job['completed_at']),
            'artifact outside qualifying job execution')
    return artifact


def snapshot(repository, run_id, fetch=None):
    fetch = fetch or gh
    require(re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository) is not None, 'invalid repository')
    require(type(run_id) is int and run_id > 0, 'invalid candidate run ID')
    prefix = f'repos/{repository}/actions/runs/{run_id}'
    run = fetch(prefix)
    require(run.get('id') == run_id and run.get('repository', {}).get('full_name') == repository
            and run.get('head_repository', {}).get('full_name') == repository, 'candidate repository mismatch')
    require(run.get('path') == '.github/workflows/candidate.yml' and run.get('event') == 'workflow_dispatch', 'wrong candidate workflow')
    require(run.get('status') == 'completed' and run.get('conclusion') == 'success', 'candidate run has not completed successfully')
    source, attempt = run.get('head_sha', ''), run.get('run_attempt')
    require(isinstance(source, str) and re.fullmatch(r'[a-f0-9]{40}', source), 'invalid candidate source')
    require(type(attempt) is int and attempt > 0, 'invalid candidate attempt')
    jobs = pages(prefix + '/jobs?filter=all', 'jobs', fetch)
    artifacts = pages(prefix + '/artifacts', 'artifacts', fetch)
    require(len({a.get('id') for a in artifacts}) == len(artifacts), 'duplicate artifact ID')
    require(len({j.get('id') for j in jobs}) == len(jobs), 'duplicate job ID')
    latest = {}
    executions = set()
    for job in jobs:
        job_attempt = job.get('run_attempt')
        require(type(job_attempt) is int and 0 < job_attempt <= attempt and job.get('run_id') == run_id
                and job.get('head_sha') == source, 'invalid job execution identity')
        identity = (job['name'], job_attempt)
        require(identity not in executions, 'ambiguous job executions')
        executions.add(identity)
        if job['name'] not in latest or latest[job['name']]['run_attempt'] < job_attempt:
            latest[job['name']] = job
    for name in REQUIRED_JOBS:
        job = latest.get(name, {})
        require(job.get('status') == 'completed' and job.get('conclusion') == 'success', f'required candidate job did not pass: {name}')
        require(isinstance(job.get('started_at'), str) and isinstance(job.get('completed_at'), str), 'missing job execution times')
        steps = CONSOLE_STEPS if name == CONSOLE_JOB else FRAMEWORK_STEPS if name.startswith('Candidate framework') else ()
        for step in steps:
            matches = [s for s in job.get('steps', []) if s.get('name') == step]
            require(len(matches) == 1 and matches[0].get('status') == 'completed' and matches[0].get('conclusion') == 'success',
                    f'required candidate step did not pass: {step}')
    selected, bindings = {}, {}
    for role in ('console', *FRAMEWORKS):
        job = latest[CONSOLE_JOB if role == 'console' else f'Candidate framework recovery ({role})']
        producer = producing_job(job, jobs)
        prefix = CONSOLE_ARTIFACT + '-' if role == 'console' else artifact_name(role, source, '')
        selected[role] = execution_artifact(artifacts, prefix, job, producer, run_id, source)
        bindings[role] = execution_binding(job, producer)
    # Preserve earlier failed/cancelled executions as evidence, but qualify only
    # the latest execution of each required job. Freeze only stable API fields.
    job_fields = ('id', 'run_attempt', 'started_at', 'completed_at', *EXECUTION_FIELDS)
    artifact_fields = ('id', 'name', 'expired', 'created_at', 'expires_at', 'size_in_bytes', 'digest', 'workflow_run')
    normalized_jobs = []
    for job in jobs:
        record = {key: job.get(key) for key in job_fields}
        record['steps'] = [{key: s.get(key) for key in STEP_FIELDS} for s in job.get('steps', [])]
        normalized_jobs.append(record)
    normalized_artifacts = [{key: a.get(key) for key in artifact_fields} for a in artifacts]
    return dict(format=SNAPSHOT_FORMAT, repository=repository, run_id=run_id, run_attempt=attempt, source_sha=source,
                workflow=run['path'], status=run['status'], conclusion=run['conclusion'],
                jobs=sorted(normalized_jobs, key=lambda j: j['id']),
                artifacts=sorted(normalized_artifacts, key=lambda a: a['id']),
                selected={key: value['id'] for key, value in selected.items()}, executions=bindings)


def validate_downloads(frozen, downloads, expected_sha256, version):
    require(type(frozen.get('format')) is int and frozen['format'] == SNAPSHOT_FORMAT and isinstance(frozen.get('executions'), dict)
            and set(frozen['executions']) == {'console', *FRAMEWORKS}, 'regenerate candidate snapshot with producing executions')
    require(re.fullmatch(r'[a-f0-9]{64}', expected_sha256) is not None, 'invalid expected archive digest')
    selected = frozen['selected']
    require(isinstance(selected, dict) and set(selected) == {'console', *FRAMEWORKS}
            and all(type(value) is int and value > 0 for value in selected.values()), 'invalid selected artifact roles or IDs')
    producers = {}
    for role in ('console', *FRAMEWORKS):
        name = CONSOLE_JOB if role == 'console' else f'Candidate framework recovery ({role})'
        jobs = [job for job in frozen['jobs'] if job['name'] == name]
        require(jobs, 'missing frozen required execution')
        latest = max(jobs, key=lambda job: job['run_attempt'])
        producer = producing_job(latest, jobs)
        binding = frozen['executions'][role]
        require(isinstance(binding, dict) and all(type(value) is int and value > 0 for value in binding.values())
                and binding == execution_binding(latest, producer), 'frozen producing execution mismatch')
        prefix = CONSOLE_ARTIFACT + '-' if role == 'console' else artifact_name(role, frozen['source_sha'], '')
        artifact = execution_artifact(frozen['artifacts'], prefix, latest, producer, frozen['run_id'], frozen['source_sha'])
        require(selected[role] == artifact['id'], 'selected producing artifact mismatch')
        producers[role] = producer
    by_id = {a['id']: a for a in frozen['artifacts']}
    console = by_id[selected['console']]
    with tempfile.TemporaryDirectory(prefix='ledgence-candidate-proof-') as temporary:
        _, identity = archive_identity(downloads / 'console', Path(temporary) / 'bundle', frozen['source_sha'], 'static')
    require(identity['archive_sha256'] == expected_sha256 and identity['version'] == version, 'selected candidate digest/version mismatch')
    bindings = {}
    for framework in FRAMEWORKS:
        directory = downloads / framework
        require({p.name for p in directory.iterdir()} == {'qualification.json', 'sdk-summary.json', 'worker-summary.json'}, 'unexpected proof artifact contents')
        receipt = read_json(directory / 'qualification.json')
        artifact = by_id[selected[framework]]
        job = producers[framework]
        require(receipt.get('format') == 1 and receipt.get('framework') == framework
                and receipt.get('repository') == frozen['repository'] and receipt.get('run_id') == frozen['run_id']
                and receipt.get('run_attempt') == job['run_attempt'], 'framework receipt execution mismatch')
        require(artifact['name'] == artifact_name(framework, frozen['source_sha'], job['run_attempt']), 'framework artifact substitution')
        candidate = receipt.get('candidate', {})
        require(all(candidate.get(key) == value for key, value in identity.items()), 'framework proof belongs to different candidate bytes')
        qualified_artifact = by_id.get(candidate.get('artifact_id'), {})
        require(qualified_artifact.get('name') == candidate.get('artifact_name') and qualified_artifact.get('expired') is False,
                'missing, expired or substituted qualified archive ID')
        original_jobs = [j for j in frozen['jobs'] if j['name'] == CONSOLE_JOB
                         and qualified_artifact.get('name') == f'{CONSOLE_ARTIFACT}-{j["run_attempt"]}']
        require(len(original_jobs) == 1 and original_jobs[0]['conclusion'] == 'success', 'qualified archive lacks a successful Console build')
        require(producing_job(original_jobs[0], frozen['jobs'])['id'] == original_jobs[0]['id'],
                'qualified archive attempt is only a copied Console execution')
        choose_artifact(frozen['artifacts'], qualified_artifact['name'], frozen['run_id'], frozen['source_sha'], original_jobs[0])
        for label in ('sdk', 'worker'):
            require(receipt.get(label + '_summary_sha256') == checksum(directory / (label + '-summary.json')), 'framework summary hash mismatch')
        sdk, worker = read_json(directory / 'sdk-summary.json'), read_json(directory / 'worker-summary.json')
        validate_reports(sdk, worker, identity, framework)
        bindings[framework] = dict(proof_artifact_id=artifact['id'], qualifying_attempt=job['run_attempt'],
            latest_job_id=frozen['executions'][framework]['latest_job_id'], producer_job_id=job['id'],
            qualified_candidate_artifact_id=candidate['artifact_id'], selected_candidate_artifact_id=console['id'],
            identical_archive_sha256=identity['archive_sha256'], receipt_sha256=checksum(directory / 'qualification.json'))
    return dict(format=1, repository=frozen['repository'], run_id=frozen['run_id'], run_attempt=frozen['run_attempt'],
                candidate=identity, selected_candidate_artifact_id=console['id'], frameworks=bindings,
                s3_scope='source adapter qualification; not selected binary execution')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    select = sub.add_parser('snapshot')
    select.add_argument('--repository', required=True)
    select.add_argument('--run', type=int, required=True)
    select.add_argument('--output', type=Path, required=True)
    verify = sub.add_parser('verify')
    verify.add_argument('--snapshot', type=Path, required=True)
    verify.add_argument('--downloads', type=Path, required=True)
    verify.add_argument('--sha256', required=True)
    verify.add_argument('--version', required=True)
    verify.add_argument('--output', type=Path, required=True)
    check = sub.add_parser('recheck')
    check.add_argument('--snapshot', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'snapshot':
            result = snapshot(args.repository, args.run)
            write_json(args.output, result)
            with Path(os.environ['GITHUB_OUTPUT']).open('a') as stream:
                for name, artifact_id in result['selected'].items():
                    stream.write(f'{name}_artifact_id={artifact_id}\n')
        elif args.command == 'verify':
            write_json(args.output, validate_downloads(read_json(args.snapshot), args.downloads, args.sha256, args.version))
        else:
            frozen = read_json(args.snapshot)
            require(snapshot(frozen['repository'], frozen['run_id']) == frozen,
                    'candidate run, job executions or artifacts changed during preflight; retry')
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f'Candidate promotion preflight failed: {error}\n')


if __name__ == '__main__':
    main()
