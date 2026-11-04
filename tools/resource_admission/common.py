"""Shared named-resource controls for native admission acceptance (MIT)."""
import json
import uuid
from workflow_acceptance.owned_scenarios import quote


class ResourceControls:
    resource_name = "support-model"

    def policy(self, *, enabled=True, capacity=1, rate=None, offer_ms=1000):
        query = {"scope": self.scope, "name": self.resource_name}
        status, _, old = self.request("POST", "/v1/resources/query", query, credentials=self.operator)
        assert status in (200, 404)
        command = {**query, "expected_revision": old["resource"]["revision"] if status == 200 else None,
                   "request_id": "policy-" + uuid.uuid4().hex, "reason": "local acceptance scenario",
                   "policy": {"enabled": enabled, "max_occupancy": capacity, "rate": rate,
                              "offer_timeout_ms": offer_ms, "start_timeout_ms": 1000}}
        return self.checked("POST", "/v1/resources/put", command)


    def resource(self):
        return self.checked("POST", "/v1/resources/query", {"scope": self.scope, "name": self.resource_name})


    def admission(self, workflow):
        value = self.sql("SELECT convert_from(snapshot_bytes,'UTF8') FROM admission_requests WHERE workflow_id=" + quote(workflow))
        return json.loads(value) if value else None


    def release(self, workflow):
        value = self.admission(workflow)
        command = {"occurrence": value["occurrence"], "source_id": value["source"]["source_id"],
                   "expected_revision": value["revision"], "request_id": "release-" + uuid.uuid4().hex,
                   "reason": "synthetic provider verified; explicitly release uncertain capacity"}
        first = self.checked("POST", "/v1/admissions/release", command)
        replay = self.checked("POST", "/v1/admissions/release", command)
        assert replay["already_accepted"] and replay["revision"] == first["revision"]
        return command
