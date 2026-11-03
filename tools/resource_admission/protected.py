"""Native protected dispatch admission; reuse the provider's independent CSV service (MIT)."""
import ast
import json
from pathlib import Path
import shutil

from http_acceptance.harness import eventually
from provider_operations.harness import ProviderDeployment
from provider_operations.sqs import ProviderSqsDeployment
from .common import ResourceControls

START = "/v1/provider-admissions/start"
REPORT = "/v1/provider-admissions/report"


def fixture_source(root, *, retained_report=None, release_report=None):
    """Observe the example's selected business result without replacing its behavior."""
    source = (root / "examples/provider-admission/program.py").read_text()
    source = source.replace("from enum import StrEnum", "from enum import StrEnum\nfrom pathlib import Path")
    anchor = "    outcome = ctx.provider_operation_result"
    assert source.count(anchor) == 1
    source = source.replace(anchor, anchor + '''
    if event["data"].get("wakes"):
        with Path(event["data"]["wakes"]).open("a") as stream:
            stream.write(json.dumps(outcome.to_dict()) + "\\n")''')
    # Native tests deliberately hold an HTTP response. This changes only the
    # socket bound, not dispatch/recovery policy or provider classification.
    assert source.count("timeout=2") == 1
    source = source.replace("timeout=2", "timeout=40")
    if retained_report is not None:
        assert release_report is not None
        source = source.replace("from enum import StrEnum", "from enum import StrEnum\nimport time")
        tree = ast.parse(source)
        dispatch = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "start_export")
        returned = next(n for n in dispatch.body if isinstance(n, ast.Return))
        expression = ast.unparse(returned.value)
        lines = source.splitlines()
        lines[returned.lineno - 1:returned.end_lineno] = [
            "    outcome = " + expression,
            f"    Path({str(retained_report)!r}).write_text(json.dumps(outcome.to_dict()))",
            f"    while not Path({str(release_report)!r}).exists(): time.sleep(0.02)",
            "    return outcome",
        ]
        source = "\n".join(lines) + "\n"
    ast.parse(source)
    return source


class ProtectedAdmissionDeployment(ResourceControls, ProviderSqsDeployment):
    resource_name = "export-dispatch"

    def provision(self):
        super().provision()
        self.runner = self.directory / "runtime/ledgence/worker/bootstrap.py"
        shutil.copytree(self.root / "sdk/python/ledgence/worker", self.runner.parent,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        config = json.loads(self.config.read_text())
        self.admission_config = self.directory / "admission.json"
        self.admission_config.write_text(json.dumps({"v": 1, "dispatch": "enabled", "audience": {
            key: config[key] for key in ("authority_id", "instance_id", "scope")}}))

    def server_args(self, config):
        arguments = (super().server_args(config) if hasattr(self, "delivery_configs")
                     else ProviderDeployment.server_args(self, config))
        return arguments + ["--admission-config", str(self.admission_config)]

    def start_server(self):
        return (super().start_server() if hasattr(self, "delivery_configs")
                else ProviderDeployment.start_server(self))

    def stop_servers(self):
        for process in self.processes:
            if process.label.startswith(("server-", "probe-publisher-")) and process.process.poll() is None:
                process.stop()

    def restart(self, *, admission="enabled", provider="enabled"):
        self.stop_servers()
        for path, value in ((self.admission_config, admission), (self.provider_config, provider)):
            config = json.loads(path.read_text()); config["dispatch"] = value
            path.write_text(json.dumps(config))
        self.start_server()

    def worker(self, subject="late-origin", proxy=None, *, queue=None, **options):
        return super().worker(subject, proxy, queue=queue, runner=self.runner,
                              cache=self.directory / f"cache-worker-{self.counter + 1}",
                              acquire_wait_ms=1000 if hasattr(self, "delivery_configs") else None,
                              **options)

    def publish_example(self):
        self.publish("provider-admission", "1.0.0", program_source=fixture_source(self.root), runtime_protocol=3)
        self.retained_outcome = self.directory / "protected-retained-dispatch-outcome.json"
        self.release_outcome = self.directory / "protected-release-retained-outcome"
        self.gates.add(self.release_outcome)
        self.publish("provider-admission-held", "1.0.0", runtime_protocol=3,
                     program_source=fixture_source(self.root, retained_report=self.retained_outcome,
                                                   release_report=self.release_outcome))

    def request_approval(self, key, provider, **options):
        options.setdefault("program", "provider-admission")
        return super().request_approval(key, provider, **options)

    def admitted(self, occurrence):
        return eventually(lambda: self.admission(occurrence["workflow_id"]),
                          description="protected admission persisted from original approval")

    def await_ticket(self, occurrence):
        return eventually(lambda: (self.admission(occurrence["workflow_id"]) or {}).get("provider"),
                          description="atomic provider grant and admission source")

    def provider_census(self, provider, key):
        with provider.lock:
            events = [json.loads(line) for line in provider.log_path.read_text().splitlines()] if provider.log_path.exists() else []
        return {"posts": sum(e["kind"] == "request" and e.get("method") == "POST" and e.get("key") == key for e in events),
                "gets": sum(e["kind"] == "request" and e.get("method") == "GET" and e.get("key") == key for e in events),
                "effects": sum(e["kind"] == "created" and e["operation"]["key"] == key for e in events)}

    def delivery_census(self):
        rows = json.loads(self.sql("SELECT coalesce(json_agg(json_build_object('task_id',t.task_id,'queue',t.queue,'destination',t.dispatch_destination,'attempts',t.attempt_count,'claims',(SELECT count(*) FROM dispatch_claim_receipts c WHERE c.task_id=t.task_id))),'[]'::json) FROM tasks t"))
        assert rows and any(row["attempts"] for row in rows)
        for row in rows:
            if hasattr(self, "delivery_configs"):
                assert row["destination"] == self.destinations[row["queue"]]
                if row["attempts"]:
                    assert row["claims"] > 0, "external execution lacks a durable broker claim"
            else:
                assert row["destination"] is None and row["claims"] == 0
        return rows

    def assert_original(self, occurrence):
        snapshot = self.admission(occurrence["workflow_id"])
        subject = snapshot["binding"]["provider"]
        assert subject["ticket"]["occurrence"] == occurrence
        assert subject["binding"]["admission"] == {"resource": self.resource_name}
        assert snapshot["occurrence"]["origin_activation_id"] == occurrence["activation_id"]
        assert snapshot["finish"] is None, "provider result was fabricated as generic callback success"
        record = snapshot.get("provider")
        if record and record["source"]:
            source = record["source"]
            assert source["provider"]["ticket"]["occurrence"] == occurrence
            assert source["admission"]["owner"]["task_id"] != occurrence["activation_id"]
        return snapshot
