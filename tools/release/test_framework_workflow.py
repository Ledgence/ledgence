"""Offline regression for source-bound optional framework qualification (MIT)."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / '.github/workflows/ci.yml'
CANDIDATE = ROOT / '.github/workflows/candidate.yml'
FRAMEWORKS = ('pydantic_ai', 'google_adk', 'claude_agent_sdk')
QUALIFY = 'Qualify optional SDK and worker recovery'
CANDIDATE_QUALIFY = 'Qualify extracted candidate SDK and worker recovery'
VERIFY = 'Verify candidate source and runtime before framework execution'
PROFILES = 'Qualify candidate program profiles against released 0.9'
SHA = 'a' * 40


def step(path, name):
    body = path.read_text().split('      - name: ' + name + '\n', 1)[1]
    return body.split('\n      - name:', 1)[0].split('\n  console:', 1)[0]


def script(path, name):
    body = step(path, name).split('        run: |\n', 1)[1]
    return '\n'.join(line[10:] for line in body.splitlines() if line.startswith('          ') or not line) + '\n'


STUB = '''import hashlib,json,os,sys
from pathlib import Path
if sys.argv[1:] == ['-']: os.execv(sys.executable,[sys.executable,'-'])
args=sys.argv[1:]
label=Path(args[0]).name
if label == 'framework_qualification.py': os.execv(sys.executable,[sys.executable,*args])
with Path(os.environ['CALLS']).open('a') as out: out.write(json.dumps(args)+'\\n')
if os.environ.get('FAIL') == label: raise SystemExit(9)
if os.environ.get('OMIT') == label: raise SystemExit(0)
value=lambda key: args[args.index(key)+1]
out=Path(value('--evidence')); out.mkdir(parents=True)
framework=os.environ['FRAMEWORK'].removeprefix('framework-')
helper=Path(os.environ['FRAMEWORK_HELPER_ROOT']).resolve()
hashes={str(p.relative_to(helper)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (helper/'ledgence/worker').rglob('*.py')}
report=dict(passed=True,framework=framework,source_sha=os.environ['GITHUB_SHA'],source_dirty=False,python='3.14.0',helper_sha256=hashes,helper_sha256_after=hashes,installed_inventory_sha256='d'*64,installed_inventory_sha256_after='d'*64)
if label == 'check-framework-integrations.py':
    report.update(tests=1,skipped=0,errors=0,failures=0,forbidden_attempts=[],helper_origin=str(helper/'ledgence/worker/integrations'/(framework+'.py')))
elif label == 'check-framework-workers.py':
    report.update(cleanup_complete=True,scenarios=[dict(passed=True)],helper_sha256_after=hashes,binary_sha256=hashlib.sha256((Path(value('--binaries'))/'ledgence').read_bytes()).hexdigest())
else:
    report.update(cleanup_complete=True,scenarios=[dict(passed=True)],binary_sha256={key:hashlib.sha256((Path(value(arg))/'ledgence').read_bytes()).hexdigest() for key,arg in [('candidate','--binaries'),('legacy','--legacy-binaries')]})
if os.environ.get('CANDIDATE_QUALIFICATION') and label in ('check-framework-integrations.py','check-framework-workers.py'):
    sys.path.insert(0,str(Path('tools/release').resolve()))
    from test_candidate_proof import reports
    identity=json.loads((Path(os.environ['RUNNER_TEMP'])/'framework-candidate-state.json').read_text())
    report=reports(identity,framework,helper.parent)[label=='check-framework-workers.py']
if os.environ.get('CORRUPT_LABEL') == label: report[os.environ['CORRUPT_KEY']]=json.loads(os.environ['CORRUPT_VALUE'])
(out/'summary.json').write_text(json.dumps(report))
'''


class FrameworkWorkflowTests(unittest.TestCase):
    def exercise(self, path=CI, name=QUALIFY, *, framework='pydantic_ai', fail='', omit='', corrupt=None):
        with tempfile.TemporaryDirectory(prefix='ledgence-framework-routing-') as temporary:
            root=Path(temporary);run=root/'runner with spaces';run.mkdir()
            commands=root/'commands';commands.mkdir()
            shim=commands/'python';shim.write_text('#!'+sys.executable+'\n'+STUB);shim.chmod(0o755)
            helper=run/('ledgence-framework-bundle/runtime' if path==CANDIDATE else 'source-helper')
            package=helper/'ledgence/worker/integrations';package.mkdir(parents=True)
            (helper/'ledgence/worker/bootstrap.py').write_text('# selected helper\n')
            for adapter in FRAMEWORKS: (package/(adapter+'.py')).write_text('# owned adapter\n')
            binaries=run/('ledgence-framework-bundle/bin' if path==CANDIDATE else 'acceptance/all-features')
            binaries.mkdir(parents=True);(binaries/'ledgence').write_text('selected binary')
            for directory in ('candidate','previous-0.9.0'):
                binary=run/'approval-upgrade'/directory/'bin/ledgence';binary.parent.mkdir(parents=True);binary.write_text(directory)
            calls=root/'calls.jsonl'
            env=dict(os.environ,PATH=str(commands)+os.pathsep+os.environ['PATH'],RUNNER_TEMP=str(run),CALLS=str(calls),
                FRAMEWORK=('framework-' if path==CI else '')+framework,FRAMEWORK_HELPER_ROOT=str(helper),
                FRAMEWORK_BINARIES=str(binaries),LEDGENCE_PYTHON=sys.executable,GITHUB_SHA=SHA,FAIL=fail,OMIT=omit)
            if path==CANDIDATE and name==CANDIDATE_QUALIFY:
                runtime={str(p.relative_to(helper)):hashlib.sha256(p.read_bytes()).hexdigest() for p in helper.rglob('*') if p.is_file()}
                bundle=helper.parent
                (binaries/'ledgence').chmod(0o755)
                (bundle/'provenance.json').write_text(json.dumps(dict(source_commit=SHA,source_tree_clean=True,console={'mode':'headless'})))
                (bundle/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+str(p.relative_to(bundle))+'\n'
                    for p in sorted(bundle.rglob('*')) if p.is_file() and p.name!='SHA256SUMS'))
                state=dict(source_sha=SHA,artifact_id=1,artifact_name='candidate-console-linux-x86_64-1',
                    runtime_sha256=runtime,helper_sha256=runtime,binary_sha256=hashlib.sha256((binaries/'ledgence').read_bytes()).hexdigest(),
                    provenance_sha256=hashlib.sha256((bundle/'provenance.json').read_bytes()).hexdigest(),
                    inventory_sha256=hashlib.sha256((bundle/'SHA256SUMS').read_bytes()).hexdigest())
                (run/'framework-candidate-state.json').write_text(json.dumps(state))
                env.update(CANDIDATE_QUALIFICATION='1',GITHUB_REPOSITORY='Ledgence/ledgence',GITHUB_RUN_ID='55',GITHUB_RUN_ATTEMPT='1')
            if corrupt:
                label,key,value=corrupt;env.update(CORRUPT_LABEL=label,CORRUPT_KEY=key,CORRUPT_VALUE=json.dumps(value))
            result=subprocess.run(['bash','-eu','-o','pipefail','-c',script(path,name)],cwd=ROOT,env=env,capture_output=True,text=True)
            recorded=[json.loads(line) for line in calls.read_text().splitlines()] if calls.exists() else []
            return result,recorded

    def test_each_sdk_gets_linux_python314_workers_and_current_binaries(self):
        ci=CI.read_text();suites=re.search(r'suite:\s*\[([^]]+)\]',ci).group(1)
        for framework in FRAMEWORKS: self.assertIn('framework-'+framework,suites)
        acceptance=ci.split('  acceptance:\n',1)[1].split('  dependencies:',1)[0]
        self.assertIn('needs: acceptance-build',acceptance)
        self.assertIn("startsWith(matrix.suite, 'framework-') && '3.14'",acceptance)
        self.assertIn('test "$(cat SOURCE_SHA)" = "$GITHUB_SHA"',acceptance)
        self.assertIn('sha256sum --check SHA256SUMS',acceptance)
        for framework in FRAMEWORKS:
            result,calls=self.exercise(framework=framework);self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual([Path(call[0]).name for call in calls],['check-framework-integrations.py','check-framework-workers.py'])
            for call in calls: self.assertEqual(call[call.index('--framework')+1],framework)
            helper=calls[0][calls[0].index('--helper-root')+1]
            self.assertEqual(calls[1][calls[1].index('--runner')+1],helper+'/ledgence/worker/bootstrap.py')
            self.assertTrue(calls[1][calls[1].index('--binaries')+1].endswith('/acceptance/all-features'))

    def test_missing_failed_or_mismatched_evidence_never_qualifies(self):
        for path,name in ((CI,QUALIFY),(CANDIDATE,CANDIDATE_QUALIFY)):
            for label in ('check-framework-integrations.py','check-framework-workers.py'):
                with self.subTest(path=path.name,label=label):
                    result,calls=self.exercise(path,name,fail=label);self.assertEqual(result.returncode,9)
                    self.assertEqual(len(calls),1 if label=='check-framework-integrations.py' else 2)
                    result,_=self.exercise(path,name,omit=label);self.assertNotEqual(result.returncode,0)
            corruptions=[('check-framework-integrations.py','tests',0),('check-framework-integrations.py','skipped',1),
                ('check-framework-integrations.py','forbidden_attempts',['network']),('check-framework-integrations.py','source_sha','wrong'),
                ('check-framework-integrations.py','helper_origin','/wrong/helper.py'),('check-framework-integrations.py','helper_sha256',{}),
                ('check-framework-workers.py','cleanup_complete',False),('check-framework-workers.py','scenarios',[]),
                ('check-framework-workers.py','binary_sha256','wrong'),('check-framework-workers.py','source_dirty',True),
                ('check-framework-workers.py','helper_sha256_after',{}),('check-framework-workers.py','passed',False),
                ('check-framework-integrations.py','helper_sha256_after',{}),('check-framework-integrations.py','installed_inventory_sha256','short'),
                ('check-framework-integrations.py','installed_inventory_sha256_after','e'*64),('check-framework-workers.py','installed_inventory_sha256','f'*64)]
            for corrupt in corruptions:
                with self.subTest(path=path.name,corrupt=corrupt):
                    result,_=self.exercise(path,name,corrupt=corrupt);self.assertNotEqual(result.returncode,0)

    def test_candidate_only_uses_verified_archive_runtime_and_binary(self):
        job=CANDIDATE.read_text().split('  frameworks:\n',1)[1].split('  console:\n',1)[0]
        for expected in ('needs: [candidate, console]','framework: [pydantic_ai, google_adk, claude_agent_sdk]',"python-version: '3.14'",
            'artifact-ids: ${{ inputs.include_console && needs.console.outputs.artifact_id','FRAMEWORK_HELPER_ROOT: ${{ runner.temp }}/ledgence-framework-bundle/runtime',
            'FRAMEWORK_BINARIES: ${{ runner.temp }}/ledgence-framework-bundle/bin'): self.assertIn(expected,job)
        self.assertNotIn('continue-on-error',job);self.assertNotIn('cargo build',job)
        result,calls=self.exercise(CANDIDATE,CANDIDATE_QUALIFY);self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('/ledgence-framework-bundle/runtime',calls[0][calls[0].index('--helper-root')+1])
        self.assertIn('/ledgence-framework-bundle/bin',calls[1][calls[1].index('--binaries')+1])

    def verify_candidate(self, mutation=None):
        with tempfile.TemporaryDirectory(prefix='ledgence-framework-archive-') as temporary:
            root=Path(temporary);run=root/'run';run.mkdir();stage=root/'stage';(stage/'bin').mkdir(parents=True)
            (stage/'bin/ledgence').write_text('inert candidate executable')
            shutil.copytree(ROOT/'sdk/python/ledgence',stage/'runtime/ledgence',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
            provenance=dict(source_commit=SHA,source_tree_clean=True,console={'mode':'headless'},package_version='0.10.2',target='x86_64-unknown-linux-gnu')
            if mutation=='source': provenance['source_commit']='b'*40
            if mutation=='dirty': provenance['source_tree_clean']=False
            if mutation=='vendor':
                (stage/'runtime/claude_agent_sdk').mkdir();(stage/'runtime/claude_agent_sdk/__init__.py').write_text('vendor')
            if mutation=='helper': (stage/'runtime/ledgence/worker/bootstrap.py').write_text('wrong source')
            if mutation=='missing': (stage/'runtime/ledgence/worker/bootstrap.py').unlink()
            (stage/'provenance.json').write_text(json.dumps(provenance))
            (stage/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+str(p.relative_to(stage))+'\n' for p in sorted(stage.rglob('*')) if p.is_file()))
            download=run/'framework-candidate-download';download.mkdir();archive=download/'candidate.tar.gz'
            def mode(info):
                info.mode=0o755 if info.isdir() or Path(info.name).parent.name=='bin' else 0o644
                return info
            with tarfile.open(archive,'w:gz') as package: package.add(stage,arcname='bundle',filter=mode)
            digest=hashlib.sha256(archive.read_bytes()).hexdigest()
            (download/'SHA256SUMS').write_text((('0'*64) if mutation=='checksum' else digest)+'  candidate.tar.gz\n')
            if mutation=='extra_archive': shutil.copyfile(archive,download/'unexpected.tar.gz')
            commands=root/'commands';commands.mkdir();(commands/'python').symlink_to(sys.executable)
            git=commands/'git'
            git.write_text('#!'+sys.executable+'\nimport sys\nprint('+repr(SHA)+') if sys.argv[1]=="rev-parse" else None\n');git.chmod(0o755)
            checksum=commands/'sha256sum'
            checksum.write_text('#!'+sys.executable+'\nimport hashlib\nfrom pathlib import Path\nfor line in Path("SHA256SUMS").read_text().splitlines():\n    digest,name=line.split("  ",1)\n    assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest\n');checksum.chmod(0o755)
            env=dict(os.environ,PATH=str(commands)+os.pathsep+os.environ['PATH'],RUNNER_TEMP=str(run),GITHUB_WORKSPACE=str(ROOT),
                     GITHUB_SHA=SHA,GITHUB_ENV=str(root/'github-env'),LEDGENCE_POSTGRES_CONTAINER='inert-service',
                     CANDIDATE_ARTIFACT_ID='100',CANDIDATE_ARTIFACT_NAME='candidate-headless-ubuntu-24.04-1',CANDIDATE_MODE='headless')
            result=subprocess.run(['bash','-eu','-o','pipefail','-c',script(CANDIDATE,VERIFY)],cwd=ROOT,env=env,capture_output=True,text=True)
            return result,(run/'ledgence-framework-bundle/runtime/ledgence/worker/bootstrap.py').exists()

    def test_archive_rejects_wrong_source_helper_vendor_or_missing_payload(self):
        result,exists=self.verify_candidate();self.assertEqual(result.returncode,0,result.stderr);self.assertTrue(exists)
        for mutation in ('source','dirty','vendor','helper','missing','checksum','extra_archive'):
            with self.subTest(mutation=mutation):
                result,exists=self.verify_candidate(mutation);self.assertNotEqual(result.returncode,0);self.assertFalse(exists)

    def test_evidence_uploads_exclude_vendor_environments_and_payloads(self):
        for path,name in ((CI,'Retain optional framework summaries and logs'),):
            upload=step(path,name);self.assertIn('if-no-files-found: error',upload)
            paths=upload.split('          path: |\n',1)[1].split('          if-no-files-found:',1)[0]
            for forbidden in ('runtime-env','source-wheel','framework-sdk/\n','framework-workers/\n'): self.assertNotIn(forbidden,paths)
            self.assertIn('framework-workers/summary.json',paths);self.assertIn('framework-sdk/*.json',paths)
        upload=step(CANDIDATE,'Retain candidate framework qualification')
        self.assertIn('path: ${{ runner.temp }}/framework-qualification/',upload)
        for forbidden in ('*.log','*.jsonl','*.stdout','*.stderr','if: always()'): self.assertNotIn(forbidden,upload)

    def test_profile_gate_uses_verified_baseline_and_fails_closed(self):
        self.assertIn('LEDGENCE_POSTGRES_CONTAINER: ${{ job.services.postgres.id }}', step(CANDIDATE, PROFILES))
        result,calls=self.exercise(CANDIDATE,PROFILES);self.assertEqual(result.returncode,0,result.stderr);call=calls[0]
        for flag,end in (('--legacy-binaries','/previous-0.9.0/bin'),('--legacy-runner','/previous-0.9.0/runtime/ledgence/worker/bootstrap.py'),('--runner','/candidate/runtime/ledgence/worker/bootstrap.py')):
            self.assertTrue(call[call.index(flag)+1].endswith(end))
        for changes in ({'fail':'check-program-profiles.py'},{'omit':'check-program-profiles.py'},{'corrupt':('check-program-profiles.py','binary_sha256',{})}):
            result,_=self.exercise(CANDIDATE,PROFILES,**changes);self.assertNotEqual(result.returncode,0)
        text=CANDIDATE.read_text();self.assertLess(text.index('Rehearse migrations and restore'),text.index(PROFILES))
        self.assertIn('0.9.0 c6566558ca60a774b3517d21a96b76a1feb3cc4d4493213c4eefc077347b64ef',text)
        console=text.split('  console:\n',1)[1]
        self.assertIn('if: inputs.include_console',console)
        self.assertIn(PROFILES,console)
        self.assertIn('that skip is not a passed profile check',console)

    def test_shell_blocks_are_valid(self):
        for path,name in ((CI,QUALIFY),(CANDIDATE,CANDIDATE_QUALIFY),(CANDIDATE,VERIFY),(CANDIDATE,PROFILES)):
            subprocess.run(['bash','-n'],input=script(path,name),text=True,check=True)


if __name__=='__main__': unittest.main()
