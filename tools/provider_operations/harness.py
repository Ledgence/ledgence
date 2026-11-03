"""Owned native provider-operation deployment and bounded durable observations (MIT)."""

import ast
import re
import importlib.util
import json
from pathlib import Path

from effect_resolution.harness import ResolutionDeployment
from http_acceptance.harness import Process, eventually
from workflow_acceptance.owned_scenarios import quote

CLAIM = "/v1/workflows/provider-operations/claim"
REPORT = "/v1/workflows/provider-operations/report"
CONTEXT = "/v1/provider-probes/context"
PROBE_REPORT = "/v1/provider-probes/report"
OBSERVE = "/v1/workflows/provider-operations/observe"


def provider_class(root):
    spec = importlib.util.spec_from_file_location(
        "ledgence_example_export_provider",
        root / "examples/provider-recovery/provider.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.ExportProvider


def fixture_source(root, *, lookup=True, retained_report=None, release_report=None):
    """Instrument the runnable example's pause/wake boundaries, not provider behavior."""
    source = (root / "examples/provider-recovery/program.py").read_text()
    source = source.replace(
        "from enum import StrEnum",
        "from enum import StrEnum\nimport asyncio\nimport time\nfrom pathlib import Path",
    )
    source = source.replace('"duration_ms": 3_600_000', '"duration_ms": 100')
    source = source.replace("timeout=2", "timeout=10")
    source = source.replace(
        '"account": "demo"', '"account": data.get("provider_account", "demo")'
    )
    source, replacements = re.subn(
        r'"key": business_key,(\s*"horizon")',
        r'"key": data.get("provider_key", business_key),\1',
        source,
    )
    assert replacements == 1
    if retained_report is not None:
        assert release_report is not None
        anchor = '    return classified(request, exchange(request, "POST"))'
        assert source.count(anchor) == 1
        source = source.replace(
            anchor,
            '    report = classified(request, exchange(request, "POST"))\n'
            + f"    Path({str(retained_report)!r}).write_text(json.dumps(report.to_dict()))\n"
            + f"    while not Path({str(release_report)!r}).exists(): time.sleep(0.02)\n"
            + "    return report",
        )
    # Locate the semantic return statement so harmless example formatting does
    # not turn a native failure test into a different program.
    returns = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Return)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "wait_provider_operation"
    ]
    assert len(returns) == 1
    node = returns[0]
    anchor = "\n".join(source.splitlines()[node.lineno - 1 : node.end_lineno])
    assert source.count(anchor) == 1
    source = source.replace(
        anchor,
        """    if event["data"].get("before_park"):
        Path(event["data"]["before_park"]).write_text("ready")
        while not Path(event["data"]["release_park"]).exists():
            await asyncio.sleep(0.02)
"""
        + anchor,
    )
    anchor = "    outcome = ctx.provider_operation_result"
    assert source.count(anchor) == 1
    source = source.replace(
        anchor,
        anchor
        + """
    if event["data"].get("wakes"):
        with Path(event["data"]["wakes"]).open("a") as stream:
            stream.write(json.dumps(outcome.to_dict()) + "\\n")""",
    )
    if not lookup:
        source = source.replace(
            "lookup=ProviderLookup.BY_BINDING", "lookup=ProviderLookup.UNAVAILABLE"
        )
        anchor = '@exports.probe\ndef inspect_export(request):\n    return classified(request, exchange(request, "GET"))\n'
        assert source.count(anchor) == 1
        source = source.replace(anchor, "")
    return source


class ProviderDeployment(ResolutionDeployment):
    probe_queue = "provider-checks"

    def provision(self):
        super().provision()
        self.provider_config = self.directory / "provider.json"
        access = json.loads(self.config.read_text())
        self.provider_config.write_text(
            json.dumps(
                {
                    "v": 1,
                    "audience": {
                        "authority_id": access["authority_id"],
                        "instance_id": access["instance_id"],
                        "scope": access["scope"],
                    },
                    "dispatch": "enabled",
                }
            )
        )

    def server_args(self, config):
        args = super().server_args(config)
        return (
            args + ["--provider-config", str(self.provider_config)]
            if hasattr(self, "provider_config")
            else args
        )

    def set_policy(self, value):
        config = json.loads(self.provider_config.read_text())
        config["dispatch"] = value
        self.provider_config.write_text(json.dumps(config))

    def worker(self, subject, proxy=None, *, queue=None, binaries=None, runner=None):
        credentials = self.workers[subject]
        if proxy is not None:
            payload = json.loads(credentials.read_text())
            payload["server"] = proxy.url
            credentials = self.directory / (subject + "-proxy-credential.json")
            credentials.write_text(json.dumps(payload))
            credentials.chmod(0o600)
            self.secret_files.append(credentials)
        self.counter += 1
        args = [
            str((binaries or self.binaries) / "ledgence"),
            "worker",
            "connect",
            "--server",
            proxy.url if proxy else self.server_url,
            "--credentials-file",
            str(credentials),
            "--tenant",
            self.scope["tenant_id"],
            "--namespace",
            self.scope["namespace"],
            "--queue",
            queue or self.queue,
            "--store",
            self.artifacts.url,
            "--cache",
            str(self.directory / ("cache-" + subject)),
            "--python",
            self.python,
            "--runner",
            str(runner or self.root / "sdk/python/ledgence/worker/bootstrap.py"),
            "--concurrency",
            "1",
        ]
        if hasattr(self, "delivery_configs"):
            args += [
                "--delivery-config",
                str(self.delivery_configs[queue or self.queue]),
            ]
        process = Process(
            args, self.directory, f"worker-{subject}-{self.counter}", self.environment
        )
        self.processes.append(process)
        return process

    def publish_reference(self):
        self.publish(
            "provider-recovery",
            "1.0.0",
            program_source=fixture_source(self.root),
            runtime_protocol=3,
        )
        self.publish(
            "provider-no-lookup",
            "1.0.0",
            program_source=fixture_source(self.root, lookup=False),
            runtime_protocol=3,
        )

    def request_approval(
        self,
        key,
        provider,
        *,
        lookup=True,
        before=False,
        dataset="customers",
        business_key=None,
        program=None,
        attempts=1,
        overrides=None,
    ):
        paths = {
            name: str(self.directory / (key + "-" + name))
            for name in ("before_park", "release_park", "wakes")
        }
        self.gates.add(Path(paths["release_park"]))
        if not before:
            Path(paths["release_park"]).touch()
        command = self.submission(
            key,
            program=program
            or ("provider-recovery" if lookup else "provider-no-lookup"),
            dataset=dataset,
            export_key=business_key or key,
            provider_url=provider.url,
            probe_queue=self.probe_queue,
            **paths,
        )
        command["input"]["data"].update(overrides or {})
        command["input"]["retry_policy"]["max_attempts"] = attempts
        workflow = self.checked("POST", "/v1/workflows", command)["workflow_id"]
        eventually(
            lambda: self.status(workflow)["state"] == "waiting",
            timeout=60,
            description="provider workflow approval",
        )
        approval = self.checked(
            "POST",
            "/v1/workflows/approvals/inspect",
            {"scope": self.scope, "workflow_id": workflow, "key": "export"},
        )
        assert (
            approval["status"] == "pending"
            and approval["protection"] == "provider_operation"
        )
        assert (
            approval["provider_binding"]["target"]["endpoint_identity"] == provider.url
        )
        return approval, paths

    def approve(self, approval, key):
        workflow = approval["workflow_id"]
        decision = {
            name: approval[name]
            for name in (
                "workflow_id",
                "key",
                "activation_id",
                "revision",
                "action",
                "protection",
                "provider_binding",
            )
        }
        decision.update(
            scope=self.scope,
            decision_id="approve-" + key,
            decision="approve",
            reviewer="fixture",
            reason="Reviewed the local export",
        )
        self.checked("POST", "/v1/workflows/approvals/decide", decision)

        def resumed():
            value = self.checked(
                "POST",
                "/v1/workflows/approvals/inspect",
                {"scope": self.scope, "workflow_id": workflow, "key": "export"},
            )
            return value if value["resumed_activation_id"] is not None else None

        accepted = eventually(
            resumed, timeout=60, description="provider approval continuation"
        )
        return {
            "scope": self.scope,
            "workflow_id": workflow,
            "activation_id": accepted["resumed_activation_id"],
            "approval_key": "export",
        }

    def begin(self, key, provider, **options):
        approval, paths = self.request_approval(key, provider, **options)
        return self.approve(approval, key), paths

    def parked(self, occurrence):
        eventually(
            lambda: (
                self.status(occurrence["workflow_id"])["state"] == "waiting"
                and int(
                    self.sql(
                        "SELECT count(*) FROM workflow_provider_waits WHERE workflow_id="
                        + quote(occurrence["workflow_id"])
                        + " AND closed_at_ms IS NULL"
                    )
                )
                == 1
            ),
            timeout=60,
            description="durable provider wait released the workflow worker",
        )

    def wakes(self, paths):
        path = Path(paths["wakes"])
        return (
            [json.loads(line) for line in path.read_text().splitlines()]
            if path.exists()
            else []
        )

    def finished(self, occurrence, paths, selection):
        def completed():
            state = self.status(occurrence["workflow_id"])["state"]
            assert state not in ("failed", "cancelled"), (
                "provider workflow reached an unexpected terminal outcome"
            )
            return state == "succeeded"

        eventually(
            completed,
            timeout=90,
            description="provider workflow continuation completed",
        )
        wakes = self.wakes(paths)
        assert len(wakes) == 1 and wakes[0]["selection"]["kind"] == selection, (
            "provider wait did not select one expected result"
        )
        assert wakes[0]["occurrence"] == occurrence
        assert self.journal(occurrence) == "", (
            "provider report falsely confirmed an A local journal"
        )
        return wakes[0]

    def all_entries(self, occurrence, limit=4):
        entries, cursor = [], None
        for _ in range(512):
            page = self.inspect(occurrence, cursor=cursor, limit=limit)
            entries.extend(page["entries"])
            cursor = page["next_cursor"]
            if cursor is None:
                assert [entry["sequence"] for entry in entries] == list(
                    range(1, page["summary"]["entry_count"] + 1)
                )
                return page, entries
        raise AssertionError("provider evidence cursor did not terminate")

    def witness(self, key, queue):
        command = self.submission(key, program="invoice")
        command["input"]["queue"] = queue
        task = self.submit(command)["task_id"]
        eventually(
            lambda: self.task(task)["state"] == "succeeded",
            timeout=60,
            description="ordinary work proceeds while provider operation is parked",
        )
        return task

    def stop_workers(self):
        for process in self.processes:
            if process.label.startswith("worker-") and process.process.poll() is None:
                process.stop()

    def pair(self):
        flow_proxy, probe_proxy = self.authenticated_proxy(), self.authenticated_proxy()
        flow = self.worker("late-origin", flow_proxy)
        probe = self.worker("crash-replacement", probe_proxy, queue=self.probe_queue)
        return flow, probe, flow_proxy, probe_proxy

    def counts(self, occurrence):
        where = " WHERE workflow_id=" + quote(occurrence["workflow_id"])
        return json.loads(
            self.sql(
                "SELECT json_build_object("
                "'dispatches',(SELECT count(*) FROM approved_effect_evidence_entries"
                + where
                + " AND entry_kind='provider_dispatch'),"
                "'reports',(SELECT count(*) FROM approved_effect_evidence_entries"
                + where
                + " AND entry_kind='provider_report'),"
                "'scheduled',(SELECT count(*) FROM approved_effect_evidence_entries"
                + where
                + " AND entry_kind='provider_probe_scheduled'),"
                "'local_records',(SELECT count(*) FROM workflow_local_results WHERE activation_id="
                + quote(occurrence["activation_id"])
                + "))"
            )
        )
