"""Real worker/orchestrator startup must reject multiple URLs for one physical queue (MIT)."""

import json
import subprocess
from urllib.parse import urlsplit

from http_acceptance.harness import unused_port


def aliased_routes(d):
    # Use the same broker, path and queue under two different URL hosts. Both
    # GetQueueAttributes calls return the same ARN on actual ElasticMQ.
    first, second = "alias-first", "alias-second"
    original = next(iter(d.routes.values()))
    url = urlsplit(original["sqs"]["queue_url"])
    assert url.hostname == "127.0.0.1" and url.port
    second_url = url._replace(netloc=f"localhost:{url.port}").geturl()
    entries = [
        {
            "route": {"scope": d.scope, "queue": name, "destination": name},
            "sqs": dict(original["sqs"], queue_url=queue_url),
        }
        for name, queue_url in [(first, url.geturl()), (second, second_url)]
    ]
    delivery = d.directory / "aliased-delivery.json"
    delivery.write_text(json.dumps({"routes": entries}))
    plan = d.directory / "aliased-plan.json"
    d.set_plan(plan, 1, {first: 1, second: 1})

    server = d.server_args(d.config)
    server[server.index("--delivery-config") + 1] = str(delivery)
    server[server.index("--bind") + 1] = f"127.0.0.1:{unused_port()}"
    worker = d.worker_args("aliased-worker", 2, plan=plan, config=delivery)
    before = d.sql("SELECT count(*) FROM worker_sessions")
    for args in (server, worker):
        result = subprocess.run(args, env=d.environment, capture_output=True, timeout=20)
        assert result.returncode != 0
        assert b"same queue ARN" in result.stderr
    assert before == d.sql("SELECT count(*) FROM worker_sessions")
    assert d.sql(
        "SELECT count(*) FROM dispatch_routes WHERE queue IN ('alias-first','alias-second')"
    ) == "0"
    return {"duplicate_physical_queue_rejected_on_both_peers": True}
