#!/usr/bin/env python3
"""Bind offline SDK/worker qualification to one immutable candidate (MIT)."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from verify import checksum, extract_archive, verify_files

FRAMEWORKS = ('pydantic_ai', 'google_adk', 'claude_agent_sdk')
COMMON_CASES = {
    'prepared-sdk-package-published-registered', 'fresh', 'fresh-again',
    'accepted-replay', 'after-failure', 'warm-reuse-and-failed-runtime-retirement',
    'warm-after-handled-sdk-failure', 'crash-after-all-records-accepted',
    'crash-before-tool-or-run-acceptance',
}
OPERATION_CASES = {'caught-nested-control-still-fails', 'accepted-first-model-tool-lost-ack-replacement'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 2_000_000,
            f'missing or oversized JSON: {path.name}')
    result = json.loads(path.read_text())
    require(isinstance(result, dict), f'JSON object required: {path.name}')
    return result


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def files(directory, *, source=False):
    require(directory.is_dir(), 'missing runtime directory')
    require(not any(p.is_symlink() for p in directory.rglob('*')), 'runtime symlinks are forbidden')
    return {str(p.relative_to(directory)): checksum(p) for p in sorted(directory.rglob('*'))
            if p.is_file() and (not source or ('__pycache__' not in p.parts and p.suffix != '.pyc'))}


def archive_identity(download, destination, source, mode):
    archives = list(download.glob('*.tar.gz'))
    require(len(archives) == 1, 'exactly one candidate archive required')
    archive = archives[0]
    require(archive.is_file() and not archive.is_symlink() and archive.stat().st_size <= 1024 ** 3, 'candidate archive exceeds bounds')
    manifest = download / 'SHA256SUMS'
    require(manifest.is_file() and not manifest.is_symlink() and manifest.stat().st_size <= 4096, 'invalid outer checksum manifest')
    digest = checksum(archive)
    require(manifest.read_text() == f'{digest}  {archive.name}\n', 'outer archive checksum mismatch')
    bundle = extract_archive(archive, destination, canonical_modes=True)
    verify_files(bundle)
    provenance = read_json(bundle / 'provenance.json')
    require(provenance.get('source_commit') == source and provenance.get('source_tree_clean') is True,
            'candidate source must match the clean qualification source')
    require(provenance.get('console', {}).get('mode') == mode, 'candidate Console mode mismatch')
    require(provenance.get('target') == 'x86_64-unknown-linux-gnu', 'Linux candidate target required')
    version = provenance.get('package_version', '')
    require(isinstance(version, str) and re.fullmatch(r'\d+\.\d+\.\d+', version), 'candidate package version required')
    runtime = files(bundle / 'runtime')
    helpers = {name: digest for name, digest in runtime.items() if name.startswith('ledgence/worker/') and name.endswith('.py')}
    require('ledgence/worker/bootstrap.py' in helpers, 'worker bootstrap missing')
    return bundle, dict(archive_name=archive.name, archive_sha256=digest,
        provenance_sha256=checksum(bundle / 'provenance.json'), inventory_sha256=checksum(bundle / 'SHA256SUMS'),
        source_sha=source, version=version, mode=mode, binary_sha256=checksum(bundle / 'bin/ledgence'),
        runtime_sha256=runtime, helper_sha256=helpers)


def validate_reports(sdk, worker, identity, framework, helper_origin=None):
    require(framework in FRAMEWORKS, 'unsupported framework')
    for report in (sdk, worker):
        require(report.get('passed') is True and report.get('framework') == framework, 'failed or wrong framework report')
        require(report.get('source_sha') == identity['source_sha'] and report.get('source_dirty') is False, 'report source mismatch or dirty')
        require(report.get('helper_sha256') == report.get('helper_sha256_after') == identity['helper_sha256'], 'helper changed or mismatch')
        require(report.get('provider_calls') == report.get('native_cli') == 'not_exercised', 'unexpected provider or CLI scope')
    require(type(sdk.get('tests')) is int and sdk['tests'] > 0, 'positive SDK test count required')
    require(all(type(sdk.get(k)) is int and sdk[k] == 0 for k in ('skipped', 'errors', 'failures')), 'SDK skipped or failed tests')
    require(sdk.get('forbidden_attempts') == [], 'forbidden SDK IO')
    if helper_origin is not None:
        require(sdk.get('helper_origin') == str(helper_origin), 'SDK imported a different helper')
    inventory = sdk.get('installed_inventory_sha256')
    require(isinstance(inventory, str) and re.fullmatch(r'[a-f0-9]{64}', inventory), 'invalid SDK inventory digest')
    require(sdk.get('installed_inventory_sha256_after') == worker.get('installed_inventory_sha256') == inventory,
            'installed SDK inventory changed')
    require(worker.get('cleanup_complete') is True and worker.get('failure') is None, 'worker failure or incomplete cleanup')
    require(worker.get('binary_sha256') == identity['binary_sha256'], 'worker binary mismatch')
    require(isinstance(sdk.get('python'), str) and sdk['python'].startswith('3.14.') and worker.get('python') == sdk['python'], 'CPython qualification mismatch')
    scenarios = worker.get('scenarios')
    require(isinstance(scenarios, list) and all(isinstance(case, dict) for case in scenarios), 'missing worker cases')
    expected = COMMON_CASES | (OPERATION_CASES if framework != 'claude_agent_sdk' else set())
    require(len(scenarios) == len(expected) and {c.get('case') for c in scenarios} == expected
            and all(c.get('passed') is True for c in scenarios), 'missing, duplicate or failed worker case')
    cases = {c['case']: c for c in scenarios}
    inner = 2 if framework == 'claude_agent_sdk' else 4
    require(type(cases['prepared-sdk-package-published-registered'].get('files')) is int
            and cases['prepared-sdk-package-published-registered']['files'] > 0, 'prepared package missing')
    require(cases['warm-reuse-and-failed-runtime-retirement'].get('invocations') == 5, 'runtime reuse witness missing')
    for name in ('fresh', 'fresh-again', 'accepted-replay', 'after-failure', 'warm-after-handled-sdk-failure', 'crash-after-all-records-accepted'):
        calls = cases[name].get('calls', {})
        require(calls.get('model') == 3 and calls.get('tool') == inner, f'incorrect replay calls: {name}')
    require(cases['crash-after-all-records-accepted'].get('accepted_records', 0) > 0, 'accepted replay witness missing')
    crash = cases['crash-before-tool-or-run-acceptance']
    before, after = crash.get('calls_before_crash', {}), crash.get('calls', {})
    require(type(before.get('tool')) is int and before['tool'] > 0
            and after.get('tool') == before['tool'] + inner
            and after.get('model') == (6 if framework == 'claude_agent_sdk' else 3)
            and crash.get('external_effects_exactly_once') is False, 'unaccepted crash call counts mismatch')
    if framework != 'claude_agent_sdk':
        lost = cases['accepted-first-model-tool-lost-ack-replacement']
        before, after = lost.get('calls_before_crash', {}), lost.get('calls_after', {})
        require(lost.get('replacement_model_calls') == 2 and lost.get('replacement_tool_calls') == 3
                and lost.get('accepted_records', 0) > 0, 'lost acknowledgement witness missing')
        for key, delta in (('model', 2), ('tool', 3)):
            require(type(before.get(key)) is int and after.get(key) == before[key] + delta, 'lost acknowledgement call counts mismatch')


def prepare(args):
    repository = Path(os.environ['GITHUB_WORKSPACE'])
    source = os.environ['GITHUB_SHA']
    require(subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repository, text=True).strip() == source,
            'checkout source mismatch')
    require(not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all'], cwd=repository), 'checkout must be clean')
    bundle, identity = archive_identity(args.download, args.extracted, source, args.mode)
    expected = {'ledgence/' + name: digest for name, digest in files(repository / 'sdk/python/ledgence', source=True).items()}
    require(identity['runtime_sha256'] == expected, 'archive runtime differs from source')
    artifact = int(os.environ['CANDIDATE_ARTIFACT_ID'])
    require(artifact > 0, 'candidate artifact ID required')
    identity.update(artifact_id=artifact, artifact_name=os.environ['CANDIDATE_ARTIFACT_NAME'])
    shutil.move(bundle, args.bundle)
    write_json(args.state, identity)


def receipt(args):
    identity = read_json(args.state)
    paths = [args.bundle, *args.bundle.rglob('*')]
    require(len(paths) <= 10000 and all(not path.is_symlink() and (path.is_file() or path.is_dir())
            for path in paths), 'bundle contains a symlink or unsupported entry')
    require(all(path.stat().st_mode & 0o7777 == (0o755 if path.is_dir() or path.parent.name == 'bin' else 0o644)
            for path in paths), 'bundle modes changed during qualification')
    require(files(args.bundle / 'runtime') == identity['runtime_sha256']
            and checksum(args.bundle / 'bin/ledgence') == identity['binary_sha256'], 'runtime changed during qualification')
    require(checksum(args.bundle / 'provenance.json') == identity['provenance_sha256']
            and checksum(args.bundle / 'SHA256SUMS') == identity['inventory_sha256'],
            'bundle provenance or inventory changed during qualification')
    # The original manifest binds every extracted payload, including Console,
    # documentation and legal material. Resealing altered files cannot qualify.
    verify_files(args.bundle)
    sdk, worker = read_json(args.sdk), read_json(args.worker)
    validate_reports(sdk, worker, identity, args.framework,
        (args.bundle / 'runtime/ledgence/worker/integrations' / (args.framework + '.py')).resolve())
    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    for name, path in (('sdk-summary.json', args.sdk), ('worker-summary.json', args.worker)):
        shutil.copyfile(path, output / name)
    write_json(output / 'qualification.json', dict(format=1, framework=args.framework,
        repository=os.environ['GITHUB_REPOSITORY'], run_id=int(os.environ['GITHUB_RUN_ID']),
        run_attempt=int(os.environ['GITHUB_RUN_ATTEMPT']), candidate=identity,
        sdk_summary_sha256=checksum(args.sdk), worker_summary_sha256=checksum(args.worker)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    build = sub.add_parser('prepare')
    for flag in ('download', 'extracted', 'bundle', 'state'):
        build.add_argument('--' + flag, type=Path, required=True)
    build.add_argument('--mode', choices=('static', 'headless'), required=True)
    proof = sub.add_parser('receipt')
    for flag in ('state', 'bundle', 'sdk', 'worker', 'output'):
        proof.add_argument('--' + flag, type=Path, required=True)
    proof.add_argument('--framework', choices=FRAMEWORKS, required=True)
    args = parser.parse_args()
    try:
        (prepare if args.command == 'prepare' else receipt)(args)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f'Candidate qualification failed: {error}\n')


if __name__ == '__main__':
    main()
