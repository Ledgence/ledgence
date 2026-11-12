"""Reject altered optional SDK fixture wheels instead of qualifying hidden payloads."""
import tempfile
from pathlib import Path
import unittest
import zipfile

from framework_integrations.source_wheel import validate


class SourceWheelTests(unittest.TestCase):
    def test_unmodified_python_only_source_and_license(self):
        original = {'claude_agent_sdk-1/src/claude_agent_sdk/__init__.py': b'pass\n',
                    'claude_agent_sdk-1/src/claude_agent_sdk/_bundled/.gitignore': b'*\n',
                    'claude_agent_sdk-1/LICENSE': b'MIT license fixture\n'}
        prefix = 'claude_agent_sdk-1.dist-info/'
        contents = {name.split('/src/', 1)[1]: data for name, data in original.items() if '/src/' in name}
        contents.update({prefix+'METADATA': b'Name: claude-agent-sdk\nVersion: 1\n',
                         prefix+'WHEEL': b'Root-Is-Purelib: true\nTag: py3-none-any\n',
                         prefix+'RECORD': b'', prefix+'licenses/LICENSE': original['claude_agent_sdk-1/LICENSE']})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'sdk.whl'
            def write(values):
                with zipfile.ZipFile(path,'w') as archive:
                    for name, value in values.items(): archive.writestr(name, value)
            write(contents); validate(path, original, '1')
            for changes in ({'extra.py': b'pass'}, {'claude_agent_sdk/__init__.py': b'changed'},
                {prefix+'licenses/LICENSE': b'changed'}, {prefix+'WHEEL': b'Root-Is-Purelib: false\nTag: cp314-cp314-linux_x86_64\n'},
                {prefix+'METADATA': b'Name: claude-agent-sdk\nVersion: 2\n'},
                {'claude_agent_sdk/_bundled/claude': b'\x7fELF'}):
                with self.subTest(changes=list(changes)):
                    write(dict(contents, **changes))
                    with self.assertRaises(ValueError): validate(path,original,'1')
            native = dict(original, **{'claude_agent_sdk-1/src/claude_agent_sdk/__init__.py': b'\x7fELF'})
            write(dict(contents, **{'claude_agent_sdk/__init__.py': b'\x7fELF'}))
            with self.assertRaises(ValueError): validate(path,native,'1')




class EvidenceTests(unittest.TestCase):
    def test_successful_sdk_tests_do_not_hide_post_test_inventory_drift(self):
        import importlib.util
        import json
        import sys
        from unittest.mock import patch
        script = Path(__file__).with_name('check-framework-integrations.py')
        spec = importlib.util.spec_from_file_location('framework_gate_test',script)
        gate = importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
        expected = {gate.canonical(item['name']): item['version'] for item in
            json.loads((gate.FIXTURES/'reviewed-dependencies.json').read_text())['profiles']['pydantic_ai']['packages']}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary);helper=root/'runtime/ledgence/worker';helper.mkdir(parents=True)
            (helper/'__init__.py').write_text('# helper')
            evidence=root/'evidence'
            def fake_run(command,cwd,environment,label,**kwargs):
                if label in ('installed-inventory','installed-inventory-after'):
                    files={'module.py': 'before' if label=='installed-inventory' else 'after'}
                    (cwd/(label+'.stdout')).write_text(json.dumps({'packages':expected,'files':files}))
                if label=='sdk-tests':
                    (cwd/'sdk-tests.json').write_text(json.dumps({'passed':True,'tests':1,'failures':0,'errors':0,'skipped':0}))
            def checked(command,**kwargs):
                if command[:2]==['git','status']: return b''
                return 'a'*40 if command[0]=='git' else '3.14.0'
            argv=['gate','--framework','pydantic_ai','--python',sys.executable,'--helper-root',str(root/'runtime'),'--evidence',str(evidence)]
            with patch.object(sys,'argv',argv),patch.object(gate,'run',fake_run),patch.object(gate.subprocess,'check_output',checked),patch('builtins.print'):
                self.assertEqual(gate.main(),1)
            report=json.loads((evidence/'summary.json').read_text())
            self.assertIs(report['passed'],False)
            self.assertEqual(report['failure_type'],'ValueError')
            self.assertNotEqual(report['installed_inventory_sha256'],report['installed_inventory_sha256_after'])

    def test_installed_inventory_hashes_bytes_and_rejects_escape(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        import framework_integrations.inventory as module
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);payload=root/'lib/sdk.py';payload.parent.mkdir();payload.write_bytes(b'original')
            dist=SimpleNamespace(metadata={'Name':'test-sdk'},version='1',files=['lib/sdk.py'],locate_file=lambda name:root/name)
            with patch.object(module.sys,'prefix',str(root)),patch.object(module.importlib.metadata,'distributions',return_value=[dist]):
                first=module.inventory();payload.write_bytes(b'changed');second=module.inventory()
                self.assertNotEqual(first['files'],second['files'])
                dist.files=[Path(__file__).resolve()]
                with self.assertRaises(ValueError):module.inventory()


if __name__=='__main__': unittest.main()
