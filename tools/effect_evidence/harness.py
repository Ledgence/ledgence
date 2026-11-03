"""Distinct authenticated workers and retained-evidence observations (MIT)."""
import json
from pathlib import Path

from http_acceptance.harness import Deployment, FaultProxy, eventually
from instance_access.harness import AccessDeployment
from workflow_acceptance.owned_scenarios import quote

CLAIM = "/v1/workflows/approved-effects/v2/claim"
CONFIRM = "/v1/workflows/approved-effects/v2/confirm"
OBSERVE = "/v1/workflows/approved-effects/v2/observe"
INBOX = "/v1/workflows/approved-effects/inbox"
INSPECT = "/v1/workflows/approved-effects/inspect"


class EffectDeployment(AccessDeployment):
    def provision(self):
        super().provision()
        self.workers = {}
        for name in ("lost-grant", "crash-original", "crash-replacement", "late-origin", "late-outsider"):
            bundle = self.directory / (name + "-credentials")
            self.bundle_directories.append(bundle)
            self.command("ledgence", ["access", "issue", "--server", self.server_url,
                "--directory", str(bundle), "--subject", name, "--expires-at", self.expiry,
                "--config", str(self.config), "--class", "worker", "--capabilities", "worker_execute"])
            self.config = self.track_bundle(bundle)
            self.workers[name] = bundle / "credential.json"

    def authenticated_proxy(self):
        proxy = FaultProxy(self.server_url, forward_authorization=True)
        self.proxies.append(proxy)
        return proxy

    def worker(self, subject, proxy=None):
        credentials = self.workers[subject]
        if proxy is not None:
            # Credentials are intentionally destination-bound. An explicit local
            # test copy points at this owned proxy and is scrubbed with the originals.
            payload = json.loads(credentials.read_text())
            payload["server"] = proxy.url
            credentials = self.directory / (subject + "-proxy-credential.json")
            credentials.write_text(json.dumps(payload))
            credentials.chmod(0o600)
            self.secret_files.append(credentials)
        return Deployment.start_worker(self, server=proxy.url if proxy else None,
            cache="cache-" + subject, credentials_file=credentials)

    def start_workflow(self, key, provider):
        command = self.submission(key, program="effect-refund", effect_key=key,
                                  provider=provider.url, amount=50)
        command["input"]["data"]["marker"] = str(self.directory / "effect-markers.jsonl")
        command["input"]["retry_policy"]["max_attempts"] = 3
        workflow = self.checked("POST", "/v1/workflows", command)["workflow_id"]
        eventually(lambda: self.status(workflow)["state"] == "waiting", timeout=60,
                   description="effect workflow requests approval")
        approval = self.checked("POST", "/v1/workflows/approvals/inspect",
            {"scope": self.scope, "workflow_id": workflow, "key": "refund"})
        assert approval["protection"] == "external_effect" and approval["status"] == "pending"
        decision = {field: approval[field] for field in (
            "workflow_id", "key", "activation_id", "revision", "action", "protection")}
        decision.update(scope=self.scope, decision_id="approve-" + key,
                        decision="approve", reviewer="fixture-operator", reason=None)
        self.checked("POST", "/v1/workflows/approvals/decide", decision)
        def resumed():
            value = self.checked("POST", "/v1/workflows/approvals/inspect",
                {"scope": self.scope, "workflow_id": workflow, "key": "refund"})
            return value if value["resumed_activation_id"] is not None else None
        accepted = eventually(resumed, timeout=60, description="approval has a resumed effect activation")
        return {"scope": self.scope, "workflow_id": workflow,
                "activation_id": accepted["resumed_activation_id"], "approval_key": "refund"}

    def status(self, workflow):
        return self.checked("GET", self.query("/v1/workflows/status", workflow_id=workflow))

    def cancel_workflow(self, occurrence):
        workflow = occurrence["workflow_id"]
        self.checked("POST", "/v1/workflows/cancel", {"scope": self.scope, "workflow_id": workflow})

    def inspect(self, occurrence, *, cursor=None, limit=4, credentials=None):
        return self.checked("POST", INSPECT, {"occurrence": occurrence, "cursor": cursor, "limit": limit},
                            credentials=credentials)

    def all_entries(self, occurrence, limit=1):
        entries, cursor, pages = [], None, 0
        while True:
            page = self.inspect(occurrence, cursor=cursor, limit=limit)
            assert len(page["entries"]) <= limit
            entries.extend(page["entries"])
            cursor = page["next_cursor"]
            pages += 1
            assert pages <= 20, "effect cursor failed to terminate"
            if cursor is None:
                assert [entry["sequence"] for entry in entries] == list(range(1, page["summary"]["entry_count"] + 1))
                return page, entries

    def counts(self, occurrence):
        where = " WHERE workflow_id=" + quote(occurrence["workflow_id"])
        return json.loads(self.sql("SELECT json_build_object("
            "'claims',(SELECT count(*) FROM approved_effect_evidence_entries" + where + " AND entry_kind='claim'),"
            "'observations',(SELECT count(*) FROM approved_effect_evidence_entries" + where + " AND entry_kind='observation'),"
            "'local_records',(SELECT count(*) FROM workflow_local_results WHERE activation_id=" + quote(occurrence["activation_id"]) + "),"
            "'state',(SELECT journal_state FROM approved_effect_occurrences" + where + "),"
            "'revision',(SELECT effect_revision FROM approved_effect_occurrences" + where + "))"))

    def apply_markers(self, key):
        path = self.directory / "effect-markers.jsonl"
        if not path.exists():
            return []
        # Polls may overlap a single append, but complete lines must all decode.
        return [row for line in path.read_text().splitlines(keepends=True) if line.endswith("\n")
                and (row := json.loads(line))["tag"] == key and row["kind"] == "apply"]

    def journal(self, occurrence):
        return self.sql("SELECT encode(journal_bytes,'hex') FROM approved_effect_occurrences WHERE workflow_id="
                        + quote(occurrence["workflow_id"]))

    def no_private_fields(self, value):
        forbidden = {"owner", "command", "command_bytes", "receipt_bytes", "lease_id", "session_id", "generation", "token"}
        if isinstance(value, dict):
            assert not forbidden.intersection(value), "private execution authority leaked into operator projection"
            for child in value.values(): self.no_private_fields(child)
        elif isinstance(value, list):
            for child in value: self.no_private_fields(child)
