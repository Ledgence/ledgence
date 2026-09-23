"""Record actual image runtime components without replacing upstream notices."""
import hashlib
import json
from pathlib import Path
import platform
import subprocess

legal = Path("/opt/ledgence/legal")
python_license = Path("/usr/local/lib/python3.14/LICENSE.txt")
if not python_license.is_file():
    raise SystemExit("official CPython image did not retain its license")
(legal / "PYTHON-LICENSE.txt").write_bytes(python_license.read_bytes())
(legal / "image-runtime.json").write_text(json.dumps({
    "python": platform.python_version(), "machine": platform.machine(),
    "python_license_sha256": hashlib.sha256(python_license.read_bytes()).hexdigest(),
    "debian_packages": subprocess.check_output(["dpkg-query", "-W", "-f=${Package} ${Version} ${Architecture}\n"], text=True).splitlines(),
    "system_notices": "/usr/share/doc/*/copyright and /usr/share/common-licenses (retained from base image)",
    "distribution": "Local build only. Review OS-component source/redistribution obligations before publishing an OCI image."
}, indent=2) + "\n")
