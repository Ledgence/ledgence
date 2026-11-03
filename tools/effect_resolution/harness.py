"""Native assessment fixture using only owned local processes and credentials (MIT)."""
import json
import subprocess
from pathlib import Path

from effect_evidence.harness import EffectDeployment, CLAIM, OBSERVE
from http_acceptance.harness import eventually
from workflow_acceptance.owned_scenarios import quote

RESOLVE = "/v1/workflows/approved-effects/resolve"


class ResolutionDeployment(EffectDeployment):
    def provision(self):
        super().provision()
        self.resolver, self.config, _ = self.issue("resolver", "resolver", ["effect_resolve"])
        self.resolver_rotated, self.config, _ = self.issue("resolver-rotated", "resolver", ["effect_resolve"])
        self.other_resolver, self.config, _ = self.issue("resolver-other", "other-resolver", ["effect_resolve"])
        self.reviewer, self.config, _ = self.issue("reviewer", "reviewer", ["metadata_read", "sensitive_read"])

    def begin(self, key, provider, *, before=False, follow_up=False):
        paths = {name: str(self.directory / (key + "-" + name)) for name in ("before_park", "release_park", "wakes")}
        self.gates.add(Path(paths["release_park"]))
        if not before: Path(paths["release_park"]).touch()
        command = self.submission(key, program="native-effect", provider=provider.url, effect_key=key,
                                  amount=50, follow_up=follow_up, **paths)
        command["input"]["retry_policy"]["max_attempts"] = 1
        workflow = self.checked("POST", "/v1/workflows", command)["workflow_id"]
        eventually(lambda: self.status(workflow)["state"] == "waiting", timeout=60, description="native fixture approval")
        approval = self.checked("POST", "/v1/workflows/approvals/inspect", {"scope": self.scope, "workflow_id": workflow, "key": "refund"})
        decision = {name: approval[name] for name in ("workflow_id", "key", "activation_id", "revision", "action", "protection")}
        decision.update(scope=self.scope, decision_id="approve-" + key, decision="approve", reviewer="fixture", reason=None)
        self.checked("POST", "/v1/workflows/approvals/decide", decision)
        assert provider.accepted.wait(60), "owned provider did not commit its one request"
        provider.drop_reply()
        eventually(lambda: Path(paths["before_park"]).exists(), timeout=40, description="effect returned unknown before park")
        approval = self.checked("POST", "/v1/workflows/approvals/inspect", {"scope": self.scope, "workflow_id": workflow, "key": "refund"})
        occurrence = {"scope": self.scope, "workflow_id": workflow, "activation_id": approval["resumed_activation_id"], "approval_key": "refund"}
        assert self.inspect(occurrence)["summary"]["journal_state"] == "unconfirmed"
        return occurrence, paths

    def parked(self, occurrence, *, count=1):
        workflow = quote(occurrence["workflow_id"])
        eventually(lambda: self.status(occurrence["workflow_id"])["state"] == "waiting" and
            int(self.sql("SELECT count(*) FROM workflow_effect_waits WHERE workflow_id=" + workflow + " AND closed_at_ms IS NULL")) == 1 and
            int(self.sql("SELECT count(*) FROM workflow_effect_waits WHERE workflow_id=" + workflow)) == count,
            timeout=60, description="durable native effect wait")

    def resolution(self, occurrence, identity, assessment, *, reason="Verified local receipt", references=("fixture:receipt",)):
        observed = self.inspect(occurrence)
        previous = observed["summary"].get("operator_resolution")
        return {"occurrence": occurrence, "resolution_id": identity,
            "expected_effect_revision": observed["summary"]["effect_revision"],
            "supersedes_resolution_id": previous["reference"]["resolution_id"] if previous else None,
            "action": observed["binding"]["approval"]["action"], "assessment": assessment,
            "reason": reason, "evidence_references": list(references)}

    def resolve(self, command, credentials=None, expected=200):
        return self.checked("POST", RESOLVE, command, credentials=credentials or self.resolver, expected=expected)

    def wakes(self, paths):
        path = Path(paths["wakes"])
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def finished(self, occurrence, paths, kind):
        eventually(lambda: self.status(occurrence["workflow_id"])["state"] == "succeeded", timeout=60, description="typed native resume completed")
        wakes = self.wakes(paths)
        assert wakes[-1]["outcome"]["kind"] == kind
        assert wakes[-1]["occurrence"] == occurrence
        assert wakes[-1]["resumed_activation_id"] != occurrence["activation_id"]
        assert self.journal(occurrence) == "", "operator assessment falsely confirmed the journal"
        return wakes

    def late_report(self, occurrence, proxy, identity, output):
        matching = [record["json"] for record in proxy.commands(CLAIM) if record["json"]["workflow_id"] == occurrence["workflow_id"]]
        assert len(matching) == 1
        command = {"occurrence": occurrence, "claim_id": matching[0]["claim_id"],
            "observation_id": identity, "reported_callback": "dispatch", "output": output}
        result = self.checked("POST", OBSERVE, command, credentials=self.workers["late-origin"])
        assert result["disposition"] == "evidence_only"
        return result

    def cli_resolve(self, command, *, credentials=None, server=None, expected=0, label="assessment"):
        path = self.directory / (label + ".json")
        path.write_text(json.dumps({"format": "ledgence.effect-resolution.v1", "base_url": server or self.server_url,
                                   "body": json.dumps(command, ensure_ascii=False, separators=(",", ":"))}))
        args = ["effect", "resolve", "--server", server or self.server_url, "--tenant", self.scope["tenant_id"],
                "--namespace", self.scope["namespace"], "--credentials-file", str(credentials or self.resolver), "--file", str(path)]
        return self.command("ledgence", args, expected=expected)

    def assessments(self, occurrence):
        return int(self.sql("SELECT count(*) FROM approved_effect_evidence_entries WHERE entry_kind='assessment' AND workflow_id=" + quote(occurrence["workflow_id"])))
