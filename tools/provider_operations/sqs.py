"""Focused provider recovery through two explicit owned ElasticMQ routes (MIT)."""

import http.client
import ipaddress
import json
from pathlib import Path
import runpy
import uuid
from urllib.parse import urlsplit

from http_acceptance.harness import Process, eventually, exchange, unused_port
from workflow_acceptance.owned_scenarios import quote
from .harness import ProviderDeployment, PROBE_REPORT, provider_class
from .scenarios import census, public_surfaces


SCENARIOS = (
    "sqs-pending-probe-terminal-wake",
    "sqs-lost-probe-report-acknowledgement",
)


def loopback_endpoint(value):
    parsed = urlsplit(value)
    try:
        loopback = (
            parsed.hostname == "localhost"
            or ipaddress.ip_address(parsed.hostname or "").is_loopback
        )
        parsed.port  # Reject malformed port text before creating any resources.
    except ValueError:
        loopback = False
    if (
        parsed.scheme not in ("http", "https")
        or not loopback
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise ValueError("endpoint must be an owned loopback HTTP(S) origin")
    return value.rstrip("/")


def delivery_config(scope, queue, destination, queue_url, endpoint):
    # The same supported single-route shape used by SqsDeployment. Each
    # publisher process owns one route; workers receive the matching config.
    return {
        "route": {"scope": scope, "queue": queue, "destination": destination},
        "sqs": {
            "region": "us-east-1",
            "queue_url": queue_url,
            "endpoint_url": endpoint,
            "local_credentials": True,
            "operation_timeout_ms": 5000,
            "visibility_timeout_seconds": 60,
        },
    }


class ProviderSqsDeployment(ProviderDeployment):
    def configure_delivery(self, endpoint):
        endpoint = loopback_endpoint(endpoint)
        admin_type = runpy.run_path(str(self.root / "tools/check-sqs.py"))["QueueAdmin"]
        self.queue_admin = admin_type(endpoint, "us-east-1", "aws")
        self.queue_resources = []
        self.delivery_configs = {}
        self.destinations = {}
        for label, queue in (("workflow", self.queue), ("probe", self.probe_queue)):
            resource = {
                "name": "ledgence-test-provider-" + label + "-" + uuid.uuid4().hex,
                "url": None,
                "logical_queue": queue,
                "destination": "provider-sqs-" + label,
            }
            # Track ownership before CreateQueue: a response may be lost after
            # the local broker creates the uniquely named queue.
            self.queue_resources.append(resource)
            created = self.queue_admin.call(
                "CreateQueue",
                {
                    "QueueName": resource["name"],
                    "Attributes": {
                        "DelaySeconds": "0",
                        "VisibilityTimeout": "60",
                        "MessageRetentionPeriod": "3600",
                    },
                },
            )
            resource["url"] = self.queue_admin.queue_url(created)
            config = delivery_config(
                self.scope, queue, resource["destination"], resource["url"], endpoint
            )
            path = self.directory / ("delivery-" + label + ".json")
            path.write_text(json.dumps(config, indent=2) + "\n")
            self.delivery_configs[queue] = path
            self.destinations[queue] = resource["destination"]
        (self.directory / "external-queues.json").write_text(
            json.dumps(self.queue_resources, indent=2) + "\n"
        )

    def server_args(self, config):
        return super().server_args(config) + [
            "--delivery-config",
            str(self.delivery_configs[self.queue]),
        ]

    def start_server(self):
        server = super().start_server()
        port = unused_port()
        arguments = self.server_args(self.config)
        arguments[arguments.index("--bind") + 1] = f"127.0.0.1:{port}"
        arguments[arguments.index("--delivery-config") + 1] = str(
            self.delivery_configs[self.probe_queue]
        )
        self.counter += 1
        publisher = Process(
            arguments,
            self.directory,
            f"probe-publisher-{self.counter}",
            self.environment,
        )
        self.processes.append(publisher)

        def ready():
            assert publisher.process.poll() is None, (
                "probe publisher exited before readiness"
            )
            try:
                return (
                    exchange(f"http://127.0.0.1:{port}", "GET", "/health/ready")[0]
                    == 200
                )
            except (OSError, http.client.HTTPException):
                return False

        eventually(ready, description="second native publisher readiness")
        return server

    def close(self):
        clean = True
        try:
            super().close()
        finally:
            for resource in getattr(self, "queue_resources", ()):
                try:
                    if resource["url"] is None:
                        response = self.queue_admin.call(
                            "GetQueueUrl", {"QueueName": resource["name"]}
                        )
                        resource["url"] = self.queue_admin.queue_url(response)
                    self.queue_admin.call("DeleteQueue", {"QueueUrl": resource["url"]})
                    resource["deleted"] = True
                except Exception:
                    resource["deleted"] = False
                    clean = False
            if hasattr(self, "queue_admin"):
                (self.directory / "external-queue-cleanup.json").write_text(
                    json.dumps(
                        {
                            "queues": self.queue_resources,
                            "calls": self.queue_admin.calls,
                        },
                        indent=2,
                    )
                    + "\n"
                )
        assert clean, "owned external queue cleanup failed"

    def published_probe(self, occurrence):
        statement = (
            "SELECT json_build_object('task_id',t.task_id,'destination',t.dispatch_destination,"
            "'generation',i.generation,'confirmed',i.last_confirmed_at_ms IS NOT NULL) "
            "FROM tasks t JOIN dispatch_intents i USING(task_id) "
            "WHERE t.workflow_id="
            + quote(occurrence["workflow_id"])
            + " AND t.queue="
            + quote(self.probe_queue)
            + " AND i.last_confirmed_at_ms IS NOT NULL"
        )
        value = json.loads(
            eventually(
                lambda: self.sql(statement),
                timeout=60,
                description="real external probe publication before worker startup",
            )
        )
        assert value["destination"] == self.destinations[self.probe_queue]
        assert value["generation"] == 1 and value["confirmed"]
        return value

    def external_census(self, occurrence):
        where = " WHERE workflow_id=" + quote(occurrence["workflow_id"])
        statement = (
            "WITH owned AS (SELECT * FROM tasks" + where + ") "
            "SELECT json_build_object('tasks',coalesce((SELECT json_agg(json_build_object("
            "'queue',t.queue,'destination',t.dispatch_destination,'state',t.state,"
            "'attempts',t.attempt_count,'claims',(SELECT count(*) FROM dispatch_claim_receipts r "
            "WHERE r.task_id=t.task_id))) FROM owned t),'[]'::json),"
            "'pending',(SELECT count(*) FROM dispatch_intents i JOIN owned t USING(task_id)),"
            "'active',(SELECT count(*) FROM attempts a JOIN owned t USING(task_id) WHERE a.state='active'),"
            "'unconfirmed',(SELECT count(*) FROM attempts a JOIN owned t USING(task_id) WHERE a.quiescence<>'confirmed'))"
        )

        def drained():
            value = json.loads(self.sql(statement))
            return (
                value
                if value["pending"] == value["active"] == value["unconfirmed"] == 0
                else None
            )

        value = eventually(
            drained, timeout=60, description="external delivery obligations drained"
        )
        assert len(value["tasks"]) == 4, (
            "expected three entrypoints and one read-only check"
        )
        assert sum(row["queue"] == self.queue for row in value["tasks"]) == 3
        assert sum(row["queue"] == self.probe_queue for row in value["tasks"]) == 1
        for row in value["tasks"]:
            assert row["destination"] == self.destinations[row["queue"]]
            assert (
                row["state"] == "succeeded"
                and row["attempts"] == 1
                and row["claims"] >= 1
            )
        return value


def run_case(deployment, probe_proxy, *, lose_report, interpreter):
    key = "provider-sqs-loss" if lose_report else "provider-sqs-pending"
    Provider = provider_class(deployment.root)
    lost = probe_proxy.lose_once(PROBE_REPORT) if lose_report else None
    with Provider(
        deployment.directory / (key + "-service"), completion_delay=0.1
    ) as provider:
        occurrence, paths = deployment.begin(key, provider)
        published = None
        if not lose_report:
            deployment.parked(occurrence)
            published = deployment.published_probe(occurrence)
            deployment.worker(
                "crash-replacement", probe_proxy, queue=deployment.probe_queue
            )
        wake = deployment.finished(occurrence, paths, "provider_succeeded")
        # A terminal report can wake the workflow before its lost HTTP reply is
        # retried. Wait for that probe's real settlement before counting calls.
        external = deployment.external_census(occurrence)
        page, entries = deployment.all_entries(occurrence)
        sources = [
            entry for entry in entries if entry["kind"] == "provider_probe_source"
        ]
        reports = [
            entry
            for entry in entries
            if entry["kind"] == "provider_report" and entry["source"]["kind"] == "probe"
        ]
        commands = probe_proxy.commands(
            PROBE_REPORT, lambda command: command["ticket"]["occurrence"] == occurrence
        )
        assert len(sources) == len(reports) == 1
        assert page["summary"]["provider"]["reconciliation"]["generation"] == 1
        assert page["summary"]["provider"]["reconciliation"]["checks_used"] == 1
        counts = census(provider)
        assert counts["status_gets"] == 1
        assert len(commands) == (2 if lose_report else 1)
        if lose_report:
            assert lost.is_set(), "probe acknowledgement loss was not exercised"
            assert commands[0]["body"] == commands[1]["body"]
            assert commands[0]["status"] == commands[1]["status"] == 200
        assert wake["selection"]["result"]["artifact"]
        assert all(
            not proxy.commands("/v1/acquisitions") for proxy in deployment.proxies
        ), "external workers silently used integrated acquisition"
        result = {
            "occurrence": occurrence,
            "provider": counts,
            "external_delivery": external,
            "database": deployment.counts(occurrence),
            "published_probe": published,
            "probe_report_requests": len(commands),
            "accepted_probe_reports": len(reports),
            "integrated_acquisition_requests": 0,
            "wake_count": 1,
        }
        if not lose_report:
            result["public_surfaces"] = public_surfaces(
                deployment, occurrence, interpreter
            )
        return result
