"""Exercise independent route publication, aggregate readiness and recovery."""

import json

from http_acceptance.harness import eventually
from .fixture import PAY, REPORT
from .scenarios import completed


def publisher_outage(d):
    path = d.directory / "publisher-plan.json"
    d.set_plan(path, 1, {PAY: 1, REPORT: 1})
    worker = d.start_consumer("publisher-outage", 2, plan=path)
    completed(d, [d.submit_to(q, "warm-outage-" + q) for q in (PAY, REPORT)])
    d.meter.fail_publication(PAY)
    try:
        waiting = d.submit_to(PAY, "await-payment-publisher")

        def degraded():
            status, _, body = d.request("GET", "/health/ready")
            return status == 503 and "publication_unavailable" in body["reasons"]

        eventually(
            degraded,
            timeout=40,
            description="one unavailable publisher degrades readiness",
        )
        witness = d.submit_to(REPORT, "independent-report-publisher")
        completed(d, [witness])
        assert d.request("GET", "/health/live")[0] == 200
        assert degraded() and not d.invocations(waiting)

        def route_reported():
            lines = d.server.stderr_path.read_bytes().splitlines(keepends=True)
            for line in lines:
                if line.endswith(b"\n") and line.startswith(b"{"):
                    fields = json.loads(line).get("fields", {})
                    if (
                        fields.get("queue") == PAY
                        and fields.get("destination")
                        == d.routes[PAY]["route"]["destination"]
                    ):
                        return True
            return False

        eventually(
            route_reported, description="publisher identifies the unavailable queue"
        )
    finally:
        d.meter.fail_publication(None)
    completed(d, [waiting])
    eventually(
        lambda: d.request("GET", "/health/ready")[0] == 200,
        timeout=30,
        description="readiness recovers following confirmed publication",
    )
    assert len(d.invocations(waiting)) == 1
    worker.stop()
    return {
        "healthy_sibling_completed": True,
        "unavailable_readiness": 503,
        "recovered_readiness": 200,
        "original_task_completed": True,
    }
