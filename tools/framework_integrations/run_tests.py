"""Run installed SDK conformance with an explicit, separately delivered helper."""
import importlib
import json
from pathlib import Path
import socket
import sys
import unittest

helper, tests, framework, evidence = map(Path, sys.argv[1:])
framework = str(framework)
# Every SDK test gets the same default-deny boundary, including imports. Local
# asyncio socketpairs remain allowed; network connections/DNS and child processes do not.
attempts = []
def audit(event, args):
    if event in ("socket.connect", "socket.getaddrinfo", "socket.gethostbyname", "socket.gethostbyaddr", "socket.sendto", "socket.sendmsg", "socket.getnameinfo") or event.startswith("subprocess.") or event in ("os.system", "os.fork", "os.forkpty", "os.posix_spawn", "os.exec", "os.spawn"):
        attempts.append(event)
        raise RuntimeError("framework qualification forbids network and child processes")
sys.addaudithook(audit)
sys.path.insert(0, str(helper.resolve()))
module = importlib.import_module("ledgence.worker.integrations." + framework)
expected = helper.resolve() / "ledgence/worker/integrations" / (framework + ".py")
assert Path(module.__file__).resolve() == expected
sys.path.insert(0, str(tests.resolve()))
suffix = {"pydantic_ai": "pydantic", "google_adk": "adk", "claude_agent_sdk": "claude"}[framework]
suite = unittest.defaultTestLoader.discover(str(tests.resolve()), pattern=f"test_integration_{suffix}.py")
# The same installed SDK must also execute its public example and the common
# controller/approval bridge. Other optional SDK examples are not selected.
example = importlib.import_module("test_framework_examples")
example_methods = {
    "pydantic_ai": "test_pydantic_sdk_proposal_and_replay",
    "google_adk": "test_adk_sdk_proposal_and_replay",
    "claude_agent_sdk": "test_claude_sdk_proposal_and_replay",
}
for name in unittest.defaultTestLoader.getTestCaseNames(example.FrameworkControllerTests):
    if "sdk_proposal" not in name or name == example_methods[framework]:
        suite.addTest(example.FrameworkControllerTests(name))
result = unittest.TextTestRunner(verbosity=2).run(suite)
assert Path(module.__file__).resolve() == expected
summary = {"framework": framework, "tests": result.testsRun, "failures": len(result.failures),
           "errors": len(result.errors), "skipped": len(result.skipped), "forbidden_attempts": attempts,
           "helper_origin": str(expected), "provider_calls": "not_exercised", "native_cli": "not_exercised"}
summary["passed"] = result.wasSuccessful() and result.testsRun > 0 and not result.skipped and not attempts
Path(evidence).write_text(json.dumps(summary, indent=2) + "\n")
raise SystemExit(0 if summary["passed"] else 1)
