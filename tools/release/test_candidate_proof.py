"""Offline fixtures for exact-artifact promotion, including partial reruns (MIT)."""
import argparse
import copy
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

import candidate_preflight as preflight
import framework_qualification as qualification
from package import archive_tree
from verify import checksum

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release.upgrade_test_fixtures import bundle_fixture, reseal

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = 'Ledgence/ledgence'


def reports(identity, framework, bundle):
    common = dict(passed=True, framework=framework, source_sha=identity['source_sha'], source_dirty=False,
                  python='3.14.0', helper_sha256=identity['helper_sha256'], helper_sha256_after=identity['helper_sha256'],
                  installed_inventory_sha256='d' * 64, provider_calls='not_exercised', native_cli='not_exercised')
    sdk = dict(common, tests=34, skipped=0, errors=0, failures=0, forbidden_attempts=[],
               installed_inventory_sha256_after='d' * 64,
               helper_origin=str((bundle / 'runtime/ledgence/worker/integrations' / (framework + '.py')).resolve()))
    inner = 2 if framework == 'claude_agent_sdk' else 4
    cases = []
    for name in sorted(qualification.COMMON_CASES | (qualification.OPERATION_CASES if framework != 'claude_agent_sdk' else set())):
        case = dict(case=name, passed=True)
        if name == 'prepared-sdk-package-published-registered': case['files'] = 32
        elif name == 'warm-reuse-and-failed-runtime-retirement': case['invocations'] = 5
        elif name == 'caught-nested-control-still-fails': pass
        elif name == 'accepted-first-model-tool-lost-ack-replacement':
            case.update(calls_before_crash={'model': 1, 'tool': 2}, calls_after={'model': 3, 'tool': 5},
                        replacement_model_calls=2, replacement_tool_calls=3, accepted_records=3)
        elif name == 'crash-before-tool-or-run-acceptance':
            case.update(calls_before_crash={'model': 3 if inner == 2 else 1, 'tool': 2},
                        calls={'model': 6 if inner == 2 else 3, 'tool': 2 + inner},
                        external_effects_exactly_once=False, accepted_records=1)
        else:
            case['calls'] = {'model': 3, 'tool': inner}
            if name == 'crash-after-all-records-accepted': case['accepted_records'] = 8
        cases.append(case)
    return sdk, dict(common, cleanup_complete=True, failure=None, binary_sha256=identity['binary_sha256'], scenarios=cases)


class CandidateProofTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix='ledgence-proof-')
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.downloads = self.root / 'downloads'
        (self.downloads / 'console').mkdir(parents=True)
        self.stage = self.root / 'stage'
        bundle_fixture(self.stage, 'current')
        (self.stage / 'README.md').write_text('unchanged documentation\n')
        provenance = qualification.read_json(self.stage / 'provenance.json')
        provenance['target'] = 'x86_64-unknown-linux-gnu'
        qualification.write_json(self.stage / 'provenance.json', provenance)
        self.source = provenance['source_commit']
        self.version = provenance['package_version']
        self.rearchive()
        self.bundle, self.identity = qualification.archive_identity(self.downloads / 'console', self.root / 'extracted', self.source, 'static')
        self.run = dict(id=55, repository={'full_name': REPOSITORY}, head_repository={'full_name': REPOSITORY},
                        path='.github/workflows/candidate.yml', event='workflow_dispatch', status='completed', conclusion='success',
                        head_sha=self.source, run_attempt=1)
        self.jobs = [self.job(name, index + 1) for index, name in enumerate(preflight.REQUIRED_JOBS)]
        self.artifacts = [self.artifact(100, preflight.CONSOLE_ARTIFACT + '-1')]
        for index, framework in enumerate(qualification.FRAMEWORKS):
            self.artifacts.append(self.artifact(101 + index, preflight.artifact_name(framework, self.source, 1)))
            self.proof(framework)

    def rearchive(self):
        reseal(self.stage)
        self.archive = self.downloads / 'console/candidate.tar.gz'
        archive_tree(self.stage, self.archive, 1700000000)
        self.digest = checksum(self.archive)
        (self.downloads / 'console/SHA256SUMS').write_text(f'{self.digest}  candidate.tar.gz\n')

    def job(self, name, job_id, attempt=1, conclusion='success'):
        steps = preflight.CONSOLE_STEPS if name == preflight.CONSOLE_JOB else preflight.FRAMEWORK_STEPS if name.startswith('Candidate framework') else ()
        return dict(id=job_id, name=name, run_id=55, head_sha=self.source, run_attempt=attempt,
                    status='completed', conclusion=conclusion, started_at=f'2026-11-12T0{attempt}:00:00Z',
                    completed_at=f'2026-11-12T0{attempt}:10:00Z',
                    steps=[dict(number=i + 1, name=s, status='completed', conclusion='success') for i, s in enumerate(steps)])

    def artifact(self, artifact_id, name, attempt=1):
        return dict(id=artifact_id, name=name, expired=False, created_at=f'2026-11-12T0{attempt}:05:00Z',
                    size_in_bytes=100, workflow_run={'id': 55, 'head_sha': self.source})

    def fetch(self, endpoint):
        if '/jobs?' in endpoint: return {'total_count': len(self.jobs), 'jobs': copy.deepcopy(self.jobs)}
        if '/artifacts?' in endpoint: return {'total_count': len(self.artifacts), 'artifacts': copy.deepcopy(self.artifacts)}
        return copy.deepcopy(self.run)

    def snapshot(self):
        return preflight.snapshot(REPOSITORY, 55, self.fetch)

    def proof(self, framework, attempt=1, candidate_id=100, candidate_name=None):
        directory = self.downloads / framework
        if directory.exists(): shutil.rmtree(directory)
        sdk, worker = reports(self.identity, framework, self.bundle)
        sdk_file, worker_file, state = (self.root / f'{framework}-{n}.json' for n in ('sdk', 'worker', 'state'))
        for path, value in ((sdk_file, sdk), (worker_file, worker), (state, dict(self.identity,
                artifact_id=candidate_id, artifact_name=candidate_name or preflight.CONSOLE_ARTIFACT + '-1'))):
            qualification.write_json(path, value)
        env = dict(GITHUB_REPOSITORY=REPOSITORY, GITHUB_RUN_ID='55', GITHUB_RUN_ATTEMPT=str(attempt))
        with patch.dict(os.environ, env):
            qualification.receipt(argparse.Namespace(state=state, bundle=self.bundle, sdk=sdk_file,
                worker=worker_file, framework=framework, output=directory))

    def validate(self, frozen=None):
        return preflight.validate_downloads(frozen or self.snapshot(), self.downloads, self.digest, self.version)

    def mutate_report(self, framework, label, update, *, reseal_receipt=True):
        directory = self.downloads / framework
        path = directory / (label + '-summary.json')
        report = qualification.read_json(path)
        update(report)
        qualification.write_json(path, report)
        if reseal_receipt:
            path = directory / 'qualification.json'
            receipt = qualification.read_json(path)
            receipt[label + '_summary_sha256'] = checksum(directory / (label + '-summary.json'))
            qualification.write_json(path, receipt)

    def test_all_three_complete_proofs_bind_exact_archive(self):
        result = self.validate()
        self.assertEqual(result['candidate']['archive_sha256'], self.digest)
        self.assertEqual(set(result['frameworks']), set(qualification.FRAMEWORKS))
        for framework in qualification.FRAMEWORKS:
            self.assertEqual(len(qualification.read_json(self.downloads / framework / 'worker-summary.json')['scenarios']),
                             9 if framework == 'claude_agent_sdk' else 11)

    def test_framework_only_rerun_preserves_other_attempts(self):
        self.run['run_attempt'] = 3
        name = 'Candidate framework recovery (pydantic_ai)'
        self.jobs.extend([self.job(name, 70, 2, 'failure'), self.job(name, 71, 3)])
        self.artifacts.append(self.artifact(201, preflight.artifact_name('pydantic_ai', self.source, 3), 3))
        self.proof('pydantic_ai', attempt=3)
        frozen = self.snapshot()
        result = self.validate(frozen)
        self.assertEqual(result['frameworks']['pydantic_ai']['qualifying_attempt'], 3)
        self.assertEqual(result['frameworks']['google_adk']['qualifying_attempt'], 1)
        self.assertTrue(any(j['conclusion'] == 'failure' for j in frozen['jobs']))

    def rebuild_console(self):
        self.run['run_attempt'] = 2
        self.jobs.append(self.job(preflight.CONSOLE_JOB, 70, 2))
        self.artifacts.append(self.artifact(200, preflight.CONSOLE_ARTIFACT + '-2', 2))

    def test_new_upload_id_is_accepted_only_for_identical_archive_bytes(self):
        self.rebuild_console()
        result = self.validate()
        binding = result['frameworks']['pydantic_ai']
        self.assertEqual(binding['qualified_candidate_artifact_id'], 100)
        self.assertEqual(binding['selected_candidate_artifact_id'], 200)
        # Metadata-only archive changes invalidate all earlier proofs, even
        # when source, binary and helper payload bytes remain unchanged.
        (self.stage / 'README.md').write_text('different archive metadata')
        self.rearchive()
        with self.assertRaisesRegex(ValueError, 'different candidate bytes'): self.validate()

    def test_required_run_jobs_and_console_steps_fail_closed(self):
        for key, value in (('status', 'in_progress'), ('conclusion', 'failure'), ('conclusion', 'cancelled'),
                           ('path', '.github/workflows/ci.yml'), ('event', 'push'), ('head_sha', 'b' * 40),
                           ('repository', {'full_name': 'other/repo'}), ('head_repository', {'full_name': 'other/repo'})):
            with self.subTest(key=key, value=value), patch.dict(self.run, {key: value}):
                with self.assertRaises(ValueError): self.snapshot()
        for job in self.jobs:
            for status in ('failure', 'cancelled', 'skipped', None):
                with self.subTest(job=job['name'], conclusion=status), patch.dict(job, conclusion=status):
                    with self.assertRaises(ValueError): self.snapshot()
        console = next(j for j in self.jobs if j['name'] == preflight.CONSOLE_JOB)
        for step in console['steps']:
            with patch.dict(step, conclusion='skipped'):
                with self.assertRaises(ValueError): self.snapshot()
        removed = self.jobs.pop()
        with self.assertRaises(ValueError): self.snapshot()
        self.jobs.append(removed)

    def test_missing_duplicate_expired_wrong_source_and_substituted_artifacts(self):
        for index in range(len(self.artifacts)):
            artifact = self.artifacts.pop(index)
            with self.assertRaises(ValueError): self.snapshot()
            self.artifacts.insert(index, artifact)
            for values in ({'expired': True}, {'workflow_run': {'id': 56, 'head_sha': self.source}},
                           {'workflow_run': {'id': 55, 'head_sha': 'b' * 40}}, {'created_at': '2026-11-11T01:05:00Z'}):
                with patch.dict(artifact, values):
                    with self.assertRaises(ValueError): self.snapshot()
        self.artifacts.append(dict(self.artifacts[0], id=999))
        with self.assertRaisesRegex(ValueError, 'ambiguous'): self.snapshot()
        self.artifacts.pop()
        path = self.downloads / 'pydantic_ai/qualification.json'
        receipt = qualification.read_json(path)
        for changes in ({'artifact_id': 999}, {'artifact_id': 101}, {'artifact_name': 'substitute'}):
            edited = copy.deepcopy(receipt); edited['candidate'].update(changes)
            qualification.write_json(path, edited)
            with self.assertRaises(ValueError): self.validate()
        qualification.write_json(path, receipt)

    def test_proof_identity_and_hash_substitution_rejected(self):
        path = self.downloads / 'pydantic_ai/qualification.json'
        receipt = qualification.read_json(path)
        for changes in ({'framework': 'google_adk'}, {'run_id': 56}, {'repository': 'other/repo'}, {'run_attempt': 2}):
            qualification.write_json(path, dict(receipt, **changes))
            with self.assertRaises(ValueError): self.validate()
        qualification.write_json(path, receipt)
        self.mutate_report('pydantic_ai', 'sdk', lambda r: r.update(tests=35), reseal_receipt=False)
        with self.assertRaisesRegex(ValueError, 'summary hash mismatch'): self.validate()

    def test_sdk_and_worker_completeness_even_when_summary_hash_resealed(self):
        sdk_updates = [lambda r, k=k, v=v: r.update({k: v}) for k, v in
                       (('tests', 0), ('tests', True), ('skipped', 1), ('errors', 1), ('failures', 1),
                        ('forbidden_attempts', ['network']), ('source_dirty', True), ('source_sha', 'b' * 40),
                        ('installed_inventory_sha256_after', 'e' * 64), ('helper_sha256_after', {}))]
        worker_updates = [lambda r, k=k, v=v: r.update({k: v}) for k, v in
                          (('cleanup_complete', False), ('binary_sha256', 'b' * 64), ('helper_sha256_after', {}),
                           ('installed_inventory_sha256', 'e' * 64), ('passed', False), ('failure', {'type': 'Error'}))]
        worker_updates += [lambda r: r['scenarios'].pop(), lambda r: r['scenarios'].append(r['scenarios'][0]),
                           lambda r: next(c for c in r['scenarios'] if c['case'] == 'fresh')['calls'].update(model=4),
                           lambda r: next(c for c in r['scenarios'] if c['case'] == 'accepted-first-model-tool-lost-ack-replacement').update(replacement_tool_calls=4)]
        for label, updates in (('sdk', sdk_updates), ('worker', worker_updates)):
            for index, update in enumerate(updates):
                with self.subTest(label=label, index=index):
                    self.proof('pydantic_ai')
                    self.mutate_report('pydantic_ai', label, update)
                    with self.assertRaises(ValueError): self.validate()

    def test_headless_source_version_checksum_and_runtime_substitution(self):
        original = qualification.read_json(self.stage / 'provenance.json')
        for mutation in ('headless', 'source', 'dirty', 'version', 'checksum', 'helper'):
            with self.subTest(mutation=mutation):
                provenance = dict(original)
                if mutation == 'headless':
                    provenance['console'] = {'mode': 'headless'}
                    shutil.move(self.stage / 'console', self.root / 'console-saved')
                elif mutation == 'source': provenance['source_commit'] = 'b' * 40
                elif mutation == 'dirty': provenance['source_tree_clean'] = False
                elif mutation == 'version': provenance['package_version'] = '99.0.0'
                elif mutation == 'helper': (self.stage / 'runtime/ledgence/worker/bootstrap.py').write_text('changed')
                qualification.write_json(self.stage / 'provenance.json', provenance)
                self.rearchive()
                if mutation == 'checksum': (self.downloads / 'console/SHA256SUMS').write_text('0' * 64 + '  candidate.tar.gz\n')
                with self.assertRaises(ValueError): self.validate()
                if mutation == 'headless': shutil.move(self.root / 'console-saved', self.stage / 'console')

    def test_runtime_change_prevents_receipt_emission(self):
        (self.bundle / 'bin/ledgence').write_text('changed after worker qualification')
        with self.assertRaisesRegex(ValueError, 'runtime changed'): self.proof('pydantic_ai')

    def test_full_extracted_bundle_must_remain_unchanged_before_receipt(self):
        pristine = self.root / 'pristine-bundle'
        shutil.copytree(self.bundle, pristine)
        for mutation in ('console', 'provenance', 'inventory', 'unrelated', 'resealed', 'symlink-file', 'symlink-directory', 'mode'):
            with self.subTest(mutation=mutation):
                if mutation == 'console': (self.bundle / 'console/index.html').write_text('changed Console')
                elif mutation == 'provenance':
                    with (self.bundle / 'provenance.json').open('a') as stream: stream.write(' ')
                elif mutation == 'inventory':
                    with (self.bundle / 'SHA256SUMS').open('a') as stream: stream.write('\n')
                elif mutation in ('unrelated', 'resealed'):
                    (self.bundle / 'README.md').write_text('changed documentation')
                    if mutation == 'resealed': reseal(self.bundle)
                elif mutation == 'symlink-file':
                    (self.bundle / 'README.md').unlink()
                    (self.bundle / 'README.md').symlink_to(pristine / 'README.md')
                elif mutation == 'symlink-directory':
                    shutil.rmtree(self.bundle / 'console')
                    (self.bundle / 'console').symlink_to(pristine / 'console', target_is_directory=True)
                else: (self.bundle / 'README.md').chmod(0o755)
                with self.assertRaises(ValueError): self.proof('google_adk')
                self.assertFalse((self.downloads / 'google_adk').exists())
                shutil.rmtree(self.bundle)
                shutil.copytree(pristine, self.bundle)
        self.proof('google_adk')
        self.validate()

    def test_paginated_metadata_and_concurrent_changes_are_detected(self):
        all_jobs = [dict(self.jobs[0], id=1000 + n, name=f'extra-{n}') for n in range(102)] + self.jobs
        all_artifacts = [self.artifact(1000 + n, f'extra-{n}') for n in range(102)] + self.artifacts
        requested = []
        def fetch(endpoint):
            requested.append(endpoint)
            if '/jobs?' in endpoint: values, key = all_jobs, 'jobs'
            elif '/artifacts?' in endpoint: values, key = all_artifacts, 'artifacts'
            else: return self.run
            page = int(endpoint.rsplit('page=', 1)[1])
            return {key: values[(page - 1) * 100:page * 100], 'total_count': len(values)}
        frozen = preflight.snapshot(REPOSITORY, 55, fetch)
        self.assertEqual(len(frozen['jobs']), 108)
        self.assertTrue(any('/jobs?' in url and 'page=2' in url for url in requested))
        self.assertTrue(any('/artifacts?' in url and 'page=2' in url for url in requested))
        # A newer rerun must not promote against a stale snapshot.
        snapshot_file = self.root / 'snapshot.json'; qualification.write_json(snapshot_file, self.snapshot())
        self.run['run_attempt'] = 2
        self.jobs.append(self.job('Candidate framework recovery (pydantic_ai)', 90, 2, 'failure'))
        with patch.object(sys, 'argv', ['candidate_preflight.py', 'recheck', '--snapshot', str(snapshot_file)]), patch.object(preflight, 'gh', self.fetch):
            with self.assertRaises(SystemExit) as failure: preflight.main()
            self.assertEqual(failure.exception.code, 1)

    def test_every_worker_scenario_is_required(self):
        for framework in qualification.FRAMEWORKS:
            sdk, worker = reports(self.identity, framework, self.bundle)
            for case in worker['scenarios']:
                with self.subTest(framework=framework, case=case['case']):
                    incomplete = dict(worker, scenarios=[c for c in worker['scenarios'] if c['case'] != case['case']])
                    with self.assertRaisesRegex(ValueError, 'worker case'):
                        qualification.validate_reports(sdk, incomplete, self.identity, framework)

    def test_prepare_rejects_dirty_and_wrong_checkout(self):
        args = argparse.Namespace(download=self.downloads / 'console', extracted=self.root / 'prepared',
            bundle=self.root / 'prepared-bundle', state=self.root / 'prepared.json', mode='static')
        with patch.dict(os.environ, GITHUB_WORKSPACE=str(ROOT), GITHUB_SHA=self.source):
            for responses in ([self.source + '\n', b'?? untracked.py\n'], ['b' * 40 + '\n']):
                with patch.object(qualification.subprocess, 'check_output', side_effect=responses):
                    with self.assertRaises(ValueError): qualification.prepare(args)
                self.assertFalse(args.state.exists())

    def test_metadata_recheck_rejects_a_new_attempt_even_with_unchanged_jobs(self):
        frozen = self.snapshot()
        path = self.root / 'frozen.json'; qualification.write_json(path, frozen)
        with patch.object(sys, 'argv', ['candidate_preflight.py', 'recheck', '--snapshot', str(path)]), patch.object(preflight, 'gh', self.fetch):
            preflight.main()
            self.run['run_attempt'] = 2
            with self.assertRaises(SystemExit) as failure: preflight.main()
            self.assertEqual(failure.exception.code, 1)

    def test_workflows_route_by_id_and_recheck_before_promotion(self):
        candidate = (ROOT / '.github/workflows/candidate.yml').read_text()
        promotion = (ROOT / '.github/workflows/promote-bundle.yml').read_text()
        self.assertIn('needs: [candidate, console]', candidate)
        self.assertIn("inputs.include_console && needs.console.result == 'success'", candidate)
        self.assertIn('artifact-ids: ${{ inputs.include_console && needs.console.outputs.artifact_id', candidate)
        self.assertIn("if: matrix.os == 'ubuntu-24.04'", candidate)
        self.assertIn('name: candidate-console-linux-x86_64-${{ github.run_attempt }}', candidate)
        self.assertEqual(promotion.count('artifact-ids:'), 4)
        self.assertIn('/candidate-proof/pydantic_ai', promotion)
        self.assertLess(promotion.index('candidate_preflight.py recheck'), promotion.index('python tools/release/promote.py'))
        self.assertNotIn('head_sha == github.sha', promotion)  # Source-equivalent release commits remain supported by promote.py.
        upload = candidate.split('      - name: Retain candidate framework qualification', 1)[1].split('\n  console:', 1)[0]
        for forbidden in ('*.log', '*.jsonl', '*.stdout', '*.stderr', 'if: always()'): self.assertNotIn(forbidden, upload)


if __name__ == '__main__': unittest.main()
