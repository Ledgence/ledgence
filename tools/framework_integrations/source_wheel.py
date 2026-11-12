"""Verify the optional Claude fixture's unmodified, Python-only source wheel."""
import email
from pathlib import PurePosixPath
import zipfile


def validate(path, original, version):
    root = 'claude_agent_sdk-' + version
    prefix = root + '.dist-info/'
    source_prefix = root + '/src/'
    sdk = {name[len(source_prefix):]: content for name, content in original.items()
           if name.startswith(source_prefix + 'claude_agent_sdk/')}
    metadata_names = {prefix + name for name in ('METADATA', 'WHEEL', 'RECORD', 'licenses/LICENSE')}
    with zipfile.ZipFile(path) as wheel:
        names = wheel.namelist()
        if len(names) != len(set(names)) or set(names) != set(sdk) | metadata_names:
            raise ValueError('source wheel inventory differs')
        for name in names:
            content = wheel.read(name)
            if name in sdk and content != sdk[name]:
                raise ValueError('SDK source bytes changed in wheel')
            if (PurePosixPath(name).suffix in ('.exe', '.dll', '.so', '.dylib', '.pyd', '.node')
                    or content[:4] in (b'\x7fELF', b'\xfe\xed\xfa\xcf', b'\xcf\xfa\xed\xfe')
                    or content[:2] == b'MZ'):
                raise ValueError('source wheel contains a native/runtime payload')
        if [name for name in names if '/_bundled/' in name] != ['claude_agent_sdk/_bundled/.gitignore']:
            raise ValueError('source wheel includes a bundled runtime')
        metadata = email.message_from_bytes(wheel.read(prefix + 'METADATA'))
        if metadata['Name'] != 'claude-agent-sdk' or metadata['Version'] != version:
            raise ValueError('source SDK metadata differs')
        headers = email.message_from_bytes(wheel.read(prefix + 'WHEEL'))
        if headers['Root-Is-Purelib'] != 'true' or headers.get_all('Tag') != ['py3-none-any']:
            raise ValueError('source wheel is not pure Python')
        if wheel.read(prefix + 'licenses/LICENSE') != original[root + '/LICENSE']:
            raise ValueError('source license was not preserved')
