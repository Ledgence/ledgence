"""Self-contained installed-wheel tests; no checkout or third-party fixtures (MIT).

After installing the wheel, set LEDGENCE_WORKER_INSTALLED_TESTS=1 and run this
suite outside the checkout with `python -I -m unittest discover -s tests -v`.
The repository build gate does this in a fresh virtual environment. Ordinary
source test discovery skips these installation-specific tests.
"""
import asyncio
import importlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


def activation(**changes):
    return {"v": 1, "workflow_id": "workflow-1", "activation_id": "activation-1",
            "revision": 0, "continuation": "start", "state": None,
            "inputs": {}, "local_steps": [], **changes}


@unittest.skipUnless(os.environ.get("LEDGENCE_WORKER_INSTALLED_TESTS") == "1",
                     "run by the isolated worker package gate")
class InstalledPackageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.worker = importlib.import_module("ledgence.worker")
        self.workflow = importlib.import_module("ledgence.worker.workflow")
        self.helper = Path(self.worker.__file__).resolve().parent
        self.assertTrue(self.helper.is_relative_to(Path(sys.prefix).resolve()))
        self.assertNotEqual(sys.prefix, sys.base_prefix, "qualification requires a clean venv")
        self.assertIsNone(importlib.import_module("ledgence").__spec__.origin)

    def test_distribution_and_complete_module_origins(self):
        distribution = importlib.metadata.distribution("ledgence-worker")
        self.assertIsNone(distribution.metadata.get("Requires-Dist"))
        self.assertEqual(distribution.metadata["License-Expression"], "MIT")
        self.assertEqual(distribution.metadata["Requires-Python"], ">=3.11")
        self.assertTrue((self.helper / "py.typed").is_file())
        for name in ("_logging", "_observations", "_protocol", "bootstrap", "otel", "workflow", "providers", "admission", "provider_admission"):
            module = importlib.import_module("ledgence.worker." + name)
            self.assertEqual(Path(module.__file__).resolve().parent, self.helper)
        self.assertIsNone(importlib.util.find_spec("ledgence.client"))
        self.assertNotIn("opentelemetry", sys.modules)
        self.assertNotIn("aiohttp", sys.modules)

    def test_invocation_and_optional_tracing_are_inactive_without_runtime(self):
        with self.assertRaisesRegex(RuntimeError, "no Ledgence invocation"):
            self.worker.current_invocation()
        with self.assertRaises(self.workflow.WorkflowError):
            self.workflow.workflow_context()
        otel = importlib.import_module("ledgence.worker.otel")
        self.assertIsNone(otel._api)
        with self.assertRaises(ModuleNotFoundError):
            otel.enable_context()
        self.assertIsNone(otel._api)

    async def test_checkpoint_freezes_state_and_staged_child_inputs(self):
        async def unexpected_rpc(*args):
            self.fail("checkpoint construction dispatched work")
        context = self.workflow.WorkflowContext(activation(), unexpected_rpc)
        self.addAsyncCleanup(context._finish, cancel=True)
        data, state = {"amount": 42}, {"phase": "waiting"}
        child = context.task("invoice", program="invoice", version="1.0", queue="billing", data=data)
        decision = context.suspend(continuation="collect", state=state, until=[child])
        data["amount"], state["phase"] = 99, "mutated"
        self.assertEqual(decision["kind"], "suspend")
        self.assertEqual(decision["state"], {"phase": "waiting"})
        self.assertEqual(decision["commands"][0]["data"], {"amount": 42})
        self.assertEqual(decision["until"], ["invoice"])
        with self.assertRaises(self.workflow.WorkflowError):
            context.complete(None)

    async def test_local_result_waits_for_ack_and_replays_without_execution(self):
        commits, calls = [], []
        entered, acknowledge = asyncio.Event(), asyncio.Event()

        async def operation(value):
            calls.append(value)
            return {"value": value}

        async def rpc(name, record):
            self.assertEqual(name, "local_step.commit")
            entered.set()
            await acknowledge.wait()
            commits.append(json.loads(json.dumps(record)))
            return {"committed": True}

        context = self.workflow.WorkflowContext(activation(), rpc)
        self.addAsyncCleanup(context._finish, cancel=True)
        pending = asyncio.ensure_future(context.local("read", operation, value=42))
        await asyncio.wait_for(entered.wait(), 2)
        self.assertFalse(pending.done())
        acknowledge.set()
        result = await asyncio.wait_for(pending, 2)
        result["value"] = "mutated"
        resumed = self.workflow.WorkflowContext(activation(local_steps=commits), rpc)
        self.addAsyncCleanup(resumed._finish, cancel=True)
        self.assertEqual(await resumed.local("read", operation, value=42), {"value": 42})
        self.assertEqual(calls, [42])
        self.assertEqual(len(commits), 1)
        with self.assertRaises(self.workflow.WorkflowError):
            resumed.local("read", operation, value=43)

    async def test_failed_ack_cannot_become_successful_completion(self):
        async def rpc(name, record):
            return {"committed": False}
        context = self.workflow.WorkflowContext(activation(), rpc)
        self.addAsyncCleanup(context._finish, cancel=True)
        with self.assertRaises(self.workflow.WorkflowError):
            await context.local("read", lambda: 42)
        with self.assertRaises(self.workflow.WorkflowError):
            context.complete("incorrect-success")

    async def test_negotiated_effect_claim_confirm_and_preloaded_replay(self):
        calls, records, operations = [], [], []
        async def dispatch(amount):
            calls.append(amount)
            return {"receipt": "installed"}
        async def lookup(amount):
            self.fail("fresh grant unexpectedly called lookup")
        action = self.workflow.ApprovalAction.for_callable(
            dispatch, version="1", arguments={"amount": 50}).to_dict()
        approval = {"scope": {"tenant_id": "tenant", "namespace": "tests"},
            "workflow_id": "workflow-1", "key": "refund", "activation_id": "requested",
            "revision": 0, "action": action, "proposed_arguments": {"amount": 50},
            "created_at": 10, "deadline": 20, "status": "approved", "protection": "external_effect",
            "decision": {"decision_id": "decision-1", "decision": "approve", "reviewer": "operator",
                         "reason": None, "decided_at": 11}, "resumed_activation_id": "activation-1"}
        identity = {"workflow_id": "workflow-1", "activation_id": "activation-1", "approval_key": "refund"}
        async def rpc(operation, command):
            operations.append(operation)
            if operation == "approved_effect.claim.v2":
                return {"kind": "dispatch", "claim": dict(command, **identity, claimed_at=12,
                        initial_permission="dispatch", recorded_revision=2, already_accepted=False)}
            self.assertEqual(operation, "approved_effect.confirm.v2")
            self.assertEqual(command["reported_callback"], "dispatch")
            records.append(json.loads(json.dumps(command["record"])))
            return dict(identity, committed=True, claim_id=command["claim_id"],
                        observation_id=command["observation_id"], key=command["record"]["key"],
                        recorded_at=13, recorded_revision=3, already_accepted=False)
        state = activation(revision=1, wake={"kind": "approval", "approval": approval})
        context = self.workflow.WorkflowContext(state, rpc, effect_evidence_v2=True)
        self.addAsyncCleanup(context._finish, cancel=True)
        result = await context.approved_effect(dispatch, lookup=lookup, version="1")
        state["local_steps"] = records
        resumed = self.workflow.WorkflowContext(state, rpc, effect_evidence_v2=True)
        self.addAsyncCleanup(resumed._finish, cancel=True)
        self.assertEqual(await resumed.approved_effect(dispatch, lookup=lookup, version="1"), result)
        self.assertEqual(calls, [50])
        self.assertEqual(operations, ["approved_effect.claim.v2", "approved_effect.confirm.v2"])

    def test_structured_log_snapshots_invocation_identity(self):
        logging = importlib.import_module("ledgence.worker._logging")

        class Sink:
            protocol_version = 3
            log_limit = 16384
            frames = []

            def offer_log(self, encoded):
                self.frames.append(json.loads(encoded))
                return True

            def drop_log(self):
                raise AssertionError("unexpected dropped log")

        previous = logging._sink
        self.addCleanup(setattr, logging, "_sink", previous)
        logging._sink = Sink()
        logger = self.worker.get_logger("installed-package-qualification")
        attributes = {"phase": "running"}
        token = self.worker._invocation.set(self.worker.InvocationContext(
            event_id="evt", attempt_id="attempt", workflow_id="workflow", activation_id="activation"))
        try:
            logger.info("checkpoint %s", "saved", extra={"attributes": attributes})
        finally:
            self.worker._invocation.reset(token)
        attributes["phase"] = "mutated"
        self.assertEqual(len(logging._sink.frames), 1)
        frame = logging._sink.frames[0]
        self.assertEqual(frame["message"], "checkpoint saved")
        self.assertEqual(frame["attributes"], {"phase": "running"})
        self.assertEqual(frame["invocation"], {"event_id": "evt", "attempt_id": "attempt",
                                              "workflow_id": "workflow", "activation_id": "activation"})

    def test_installed_bytes_in_dedicated_isolated_runtime(self):
        # Never use site-packages itself as the runtime helper root. The Rust
        # worker delivers a dedicated helper; reproduce that layout from the
        # installed wheel's bytes, without pulling unrelated venv packages in.
        with tempfile.TemporaryDirectory(prefix="installed-worker-runtime-") as directory:
            root = Path(directory).resolve()
            helper = root / "helper/ledgence/worker"
            shutil.copytree(self.helper, helper, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            for path in helper.iterdir():
                self.assertEqual(path.read_bytes(), (self.helper / path.name).read_bytes())
            program = root / "program"
            program.mkdir()
            (program / "program.py").write_text('''import asyncio, os
from pathlib import Path
import ledgence, ledgence.worker
from ledgence.worker import current_invocation, get_logger, register_shutdown
from ledgence.worker.workflow import workflow_context, WorkflowError
assert ledgence.__spec__.origin is None
assert Path(ledgence.worker.__file__).resolve().is_relative_to(Path(__file__).resolve().parent.parent / "helper")
logger = get_logger("installed-runtime")
register_shutdown(lambda: print("shutdown-callback"))
async def local(value):
    return {"value": value}
async def handle_async(event):
    current = current_invocation()
    assert current.event_id == event["id"]
    assert current.attempt_id == event["ldgattemptid"]
    logger.info("installed runtime invocation")
    result = {"event": event, "pid": os.getpid()}
    if "ldgworkflowid" in event:
        context = workflow_context()
        assert current.workflow_id == context.workflow_id
        result["local"] = await context.local("read", local, value=42)
        return context.complete(result)
    try:workflow_context()
    except WorkflowError:pass
    else:raise AssertionError("workflow context leaked into ordinary invocation")
    return result
def handle(event):
    return asyncio.run(handle_async(event))
''')
            for version in (1, 2, 3):
                with self.subTest(protocol=version):
                    messages = []
                    for number in (1, 2):
                        event = {"specversion": "1.0", "id": f"evt-{number}", "source": "/installed-test",
                                 "type": "test.invoke", "ldgattemptid": f"attempt-{number}", "data": {"value": 42}}
                        message = {"v": version, "type": "invoke", "event_id": event["id"],
                                   "attempt_id": event["ldgattemptid"], "event": event}
                        if version >= 2:
                            message["processing_context"] = None
                        if version == 3 and number == 1:
                            event.update(ldgworkflowid="workflow-1", ldgactivationid="activation-1", ldgtaskid="activation-1")
                            message["extension"] = {"schema": self.workflow.SCHEMA, "payload": activation()}
                        messages.append(message)
                        if version == 3 and number == 1:
                            messages.append({"v": 3, "type": "runtime_reply", "id": 1,
                                             "event_id": "evt-1", "attempt_id": "attempt-1", "result": {"committed": True}})
                    messages.append({"v": version, "type": "shutdown"})
                    result = subprocess.run(
                        [sys.executable, "-I", "-S", "-B", str(helper / "bootstrap.py"),
                         "--package-root", str(program), "--handler", "program:handle_async" if version == 3 else "program:handle",
                         "--python-version", "%d.%d" % sys.version_info[:2], "--protocol-version", str(version)],
                        cwd=root, input=b"".join(json.dumps(m).encode() + b"\n" for m in messages),
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
                    frames = [json.loads(line) for line in result.stdout.splitlines()]
                    controls = [frame for frame in frames if frame["type"] != "log"]
                    expected = ["ready", "runtime_request", "result", "result", "closing"] if version == 3 else ["ready", "result", "result", "closing"]
                    self.assertEqual([frame["type"] for frame in controls], expected)
                    if version == 3:
                        self.assertEqual(controls[1]["operation"], "local_step.commit")
                        self.assertEqual(controls[1]["payload"]["output"], {"value": 42})
                    outputs = []
                    for number, frame in enumerate((frame for frame in controls if frame["type"] == "result"), 1):
                        self.assertEqual(frame["status"], "success", frame)
                        output = frame["output"]
                        if version == 3 and number == 1:
                            self.assertEqual(output["kind"], "complete")
                            output = output["output"]
                            self.assertEqual(output["local"], {"value": 42})
                        self.assertEqual(output["event"]["id"], f"evt-{number}")
                        outputs.append(output)
                    self.assertEqual([output["pid"] for output in outputs], [controls[0]["pid"]] * 2)
                    if version >= 2:
                        self.assertIn(b"shutdown-callback", result.stderr)
                    self.assertFalse(list(root.rglob("__pycache__")))


    def test_installed_provider_probe_negotiation_and_typed_resume(self):
        import hashlib
        from ledgence.worker.providers import PROBE_SCHEMA
        source = """from enum import StrEnum
from ledgence.worker.workflow import Workflow, WorkflowError, workflow_context
from ledgence.worker.providers import ProviderAdapter, ProviderLookup, ProviderReport, ProviderSelectionKind
provider = ProviderAdapter('example.export',version='1',lookup=ProviderLookup.BY_BINDING,completion_criterion='CSV is ready')
@provider.dispatch
async def dispatch(request): raise AssertionError('DISPATCH_ENTERED')
@provider.probe
async def probe(request):
    try: workflow_context()
    except WorkflowError: pass
    else: raise AssertionError('WORKFLOW_CONTEXT_LEAKED')
    return ProviderReport.succeeded(None,target=request.target)
class Entry(StrEnum): DONE='done'
workflow=Workflow(Entry,providers=[provider])
@workflow.entrypoint(Entry.DONE,default=True)
def done(event,ctx):
    selected=ctx.provider_operation_result
    assert selected.kind is ProviderSelectionKind.PROVIDER_SUCCEEDED
    result=selected.result
    result['items'].append('mutated')
    return ctx.complete(selected.result)
handle=workflow.build()
"""
        contract = {"adapter_id":"example.export","version":"1","completion_criterion":"CSV is ready",
            "dispatch_callable":"program:dispatch","reconcile_callable":"program:probe","lookup":"by_binding"}
        contract["digest"]="sha256:"+hashlib.sha256(json.dumps(contract,sort_keys=True,separators=(",",":")).encode()).hexdigest()
        target={"provider":"example","environment":"test","account":"demo","api_variant":"v1","operation":"export","endpoint_identity":"local","dimensions":{}}
        binding={"v":1,"business":{"namespace":"exports","key":"installed"},"contract":contract,"target":target,
            "deduplication":None,"credential_reference":"fixture","reconciliation":{"queue":"probes","initial_delay_ms":1000,"interval_ms":1000,"maximum_interval_ms":5000,"max_checks":3,"max_elapsed_ms":60000,"check_timeout_ms":1000},
            "program":{"program":{"id":"export","version":"1"},"digest":"sha256:"+"a"*64,"size":1234}}
        occurrence={"scope":{"tenant_id":"tenant","namespace":"tests"},"workflow_id":"workflow-1","activation_id":"activation-1","approval_key":"export"}
        ticket={"occurrence":occurrence,"binding_digest":"sha256:"+"b"*64}
        event={"specversion":"1.0","id":"event-1","source":"urn:installed-provider","type":"test.invoke","ldgattemptid":"attempt-1","ldgtaskid":"probe-task","ldgworkflowid":"workflow-1","ldgtenantid":"tenant","ldgnamespace":"tests","data":{}}
        probe={"v":3,"type":"invoke","event_id":"event-1","attempt_id":"attempt-1","event":event,"processing_context":None,
            "runtime_capabilities":{"approved_effect_evidence_v2":True},"runtime_features":{"v":1,"supported":["native_effect_resolution.v1","provider_operation.v1"]},
            "extension":{"schema":PROBE_SCHEMA,"payload":{"ticket":ticket,"binding":binding,"action":{"name":"program:dispatch","version":"1","arguments":{}},
                "probe_id":"probe-1","generation":1,"task_id":"probe-task","attempt_id":"attempt-1","source_id":"source-1","report_id":"report-1","provider_reference":None,"check_timeout_ms":1000}}}
        unsupported=json.loads(json.dumps(probe));unsupported.pop("runtime_features")
        wake={"kind":"provider_operation","key":"wait","occurrence":occurrence,"parking_activation_id":"activation-park","parking_revision":1,"resumed_activation_id":"activation-resumed","resumed_revision":2,"selected_at":200,"selected_effect_revision":12,
            "selection":{"kind":"provider_succeeded","report_ref":{"report_id":"report-1","source_id":"source-1","sequence":4,"recorded_revision":10,"recorded_at":100,"knowledge":"succeeded"},"result":{"items":[1]}}}
        resumed=json.loads(json.dumps(probe));resumed["event"].update(ldgtaskid="activation-resumed",ldgworkflowid="workflow-1",ldgactivationid="activation-resumed")
        resumed["extension"]={"schema":self.workflow.SCHEMA,"payload":activation(activation_id="activation-resumed",revision=2,continuation="done",wake=wake)}
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); helper=root/"helper/ledgence/worker"; package=root/"package";package.mkdir()
            shutil.copytree(self.helper,helper,ignore=shutil.ignore_patterns("__pycache__","*.pyc"))
            (package/"program.py").write_text(source)
            result=subprocess.run([sys.executable,"-I","-S","-B",str(helper/"bootstrap.py"),"--package-root",str(package),"--handler","program:handle","--python-version",f"{sys.version_info.major}.{sys.version_info.minor}","--protocol-version","3"],
                cwd=root,input=b"".join(json.dumps(message).encode()+b"\n" for message in (probe,unsupported,resumed,{"v":3,"type":"shutdown"})),capture_output=True,timeout=20)
        self.assertEqual(result.returncode,0,result.stderr)
        frames=[json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([frame["type"] for frame in frames],["ready","result","result","result","closing"])
        self.assertEqual(frames[1]["output"]["report"]["knowledge"],{"kind":"succeeded","result":None})
        self.assertEqual(frames[2]["status"],"runtime_error")
        self.assertEqual(frames[3]["output"]["output"],{"items":[1]})
        self.assertNotIn(b"DISPATCH_ENTERED",result.stderr)
        self.assertNotIn(b"WORKFLOW_CONTEXT_LEAKED",result.stderr)


    def test_installed_admission_checkpoint_and_preloaded_finish_need_no_callback(self):
        admission = importlib.import_module("ledgence.worker.admission")
        self.assertEqual(Path(admission.__file__).resolve().parent, self.helper)
        source_code = """from enum import StrEnum
from ledgence.worker.workflow import Workflow, OperationKind
from ledgence.worker.admission import AdmissionOutcomeKind
class Entry(StrEnum): START='start'; RESUME='resume'
workflow=Workflow(Entry)
@workflow.operation(kind=OperationKind.TOOL,version='1')
async def operation(value, suffix='!'):
    raise AssertionError('ADMISSION_CALLBACK_ENTERED')
@workflow.entrypoint(Entry.START,default=True)
def start(event,ctx):
    return ctx.request_admission('operation:1',resource='tool',operation=operation.bind(value=42),resume=Entry.RESUME,state=None,timeout_ms=60000)
@workflow.entrypoint(Entry.RESUME)
async def resume(event,ctx):
    result=await ctx.admitted_operation(operation)
    assert result.kind is AdmissionOutcomeKind.SUCCEEDED
    output=result.result
    output['items'].append('changed')
    return ctx.complete(result.result)
handle=workflow.build()
"""
        scope = {"tenant_id": "tenant", "namespace": "tests"}
        digest = "sha256:" + "a" * 64
        occurrence = {"scope": scope, "workflow_id": "workflow-1",
                      "origin_activation_id": "activation-1", "key": "operation:1"}
        binding = {"resource": "tool", "program": {"program": {"id": "test", "version": "1"},
                   "digest": digest, "size": 100}, "operation": {"callable": "program:operation",
                   "kind": "tool", "version": "1", "arguments": {"value": 42, "suffix": "!"}}}
        wake = {"kind": "resource_admission", "key": "operation:1", "occurrence": occurrence,
                "waiting_activation_id": "activation-1", "resumed_activation_id": "resumed",
                "resumed_revision": 1, "binding": binding, "selected_at": 10,
                "selection": {"kind": "offered", "offer": {"occurrence": occurrence,
                    "generation": 1, "policy_revision": 1, "activation_id": "resumed",
                    "expires_at": 2000, "binding_digest": digest}}}
        receipt = {"source": {"occurrence": occurrence, "source_id": "source", "start_id": "start",
                    "finish_id": "finish", "generation": 1, "binding_digest": digest,
                    "owner": {"scope": scope, "task_id": "resumed", "attempt_id": "original-attempt",
                        "lease_id": "lease", "generation": 1, "worker_session_id": "worker", "consumer_id": 0},
                    "worker": {"audience": {"scope": scope, "authority_id": "authority", "instance_id": "instance"},
                        "issuer": "issuer", "subject": "worker", "credential_id": "credential"},
                    "accepted_at": 12, "start_deadline_at": 1012},
                   "output": {"items": [42]}, "accepted_at": 20, "already_accepted": True}
        def message(number, context, negotiated=True):
            event = {"specversion": "1.0", "id": f"event-{number}", "source": "urn:installed-admission",
                "type": "test.invoke", "ldgattemptid": f"attempt-{number}", "data": {},
                "ldgtaskid": context["activation_id"], "ldgactivationid": context["activation_id"],
                "ldgworkflowid": "workflow-1", "ldgtenantid": "tenant", "ldgnamespace": "tests"}
            return {"v": 3, "type": "invoke", "event_id": event["id"], "attempt_id": event["ldgattemptid"],
                "event": event, "processing_context": None,
                "runtime_features": {"v": 1, "supported": ["resource_admission.v1"] if negotiated else []},
                "extension": {"schema": self.workflow.SCHEMA, "payload": context}}
        resumed = activation(activation_id="resumed", revision=1, continuation="resume",
                             wake=wake, admission_result=receipt)
        messages = [message(1, activation()), message(2, resumed, False), message(3, resumed),
                    {"v": 3, "type": "shutdown"}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            helper, package = root / "helper/ledgence/worker", root / "program"
            package.mkdir()
            shutil.copytree(self.helper, helper, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            (package / "program.py").write_text(source_code)
            result = subprocess.run(
                [sys.executable, "-I", "-S", "-B", str(helper / "bootstrap.py"),
                 "--package-root", str(package), "--handler", "program:handle",
                 "--python-version", f"{sys.version_info.major}.{sys.version_info.minor}", "--protocol-version", "3"],
                cwd=root, input=b"".join(json.dumps(m).encode() + b"\n" for m in messages),
                capture_output=True, timeout=20,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        frames = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([f["type"] for f in frames], ["ready", "result", "result", "result", "closing"])
        first = frames[1]
        self.assertEqual(first["status"], "success")
        self.assertEqual(first["output"]["wait"]["kind"], "resource_admission")
        self.assertEqual(first["output"]["wait"]["request"]["operation"]["arguments"], {"value": 42, "suffix": "!"})
        self.assertEqual(frames[2]["status"], "runtime_error")
        self.assertEqual(frames[3]["status"], "success")
        self.assertEqual(frames[3]["output"]["output"], {"items": [42]})
        self.assertNotIn(b"ADMISSION_CALLBACK_ENTERED", result.stderr)


if __name__ == "__main__":
    unittest.main()
