"""Bounded task/workflow/callback smoke using only the Python standard library."""
import json
import os
import time
import urllib.parse
import urllib.request
import uuid

SERVER = os.environ.get("LEDGENCE_SERVER", "http://orchestrator:8080")
SCOPE = {"tenant_id": "acme", "namespace": "demo"}
PREFIX = "demo-" + uuid.uuid4().hex


def request(path, body=None, server=SERVER):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(server + path, data=data,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def eventually(operation, predicate, description):
    deadline = time.monotonic() + 90
    while True:
        value = operation()
        if predicate(value):
            return value
        if time.monotonic() >= deadline:
            raise RuntimeError(f"timed out waiting for {description}: {value!r}")
        time.sleep(0.2)


def result(kind, identity):
    query = urllib.parse.urlencode(dict(SCOPE, **{kind + "_id": identity}))
    value = eventually(lambda: request(f"/v1/{kind}s/result?{query}"),
                       lambda value: value["outcome"] is not None, kind + " result")
    assert value["outcome"]["kind"] == "succeeded", value
    return value["outcome"]["output"]


def submit(kind, name, data, key):
    command = {"idempotency_key": PREFIX + key,
               "input": dict(SCOPE, queue="demo", program={"id": name, "version": "1.0.0"},
                             data=data, correlation_key=PREFIX)}
    value = request(f"/v1/{kind}s", command)
    return value[kind + "_id"]


def subscribe(kind, identity):
    subscription = request("/v1/completion-subscriptions", {
        "scope": SCOPE, "target": {"kind": kind, "id": identity},
        "destination": "demo-callback", "idempotency_key": PREFIX + kind})
    query = urllib.parse.urlencode(dict(SCOPE, subscription_id=subscription["subscription_id"]))
    delivered = eventually(lambda: request("/v1/completion-subscriptions/status?" + query),
                          lambda value: value["state"] == "delivered", "callback delivery")
    events = request("/events", server="http://receiver:8091")
    event = next(event for event in events if event["id"] == delivered["event"]["id"])
    assert event["ldgcorrelationkey"] == PREFIX and "data" not in event, event
    return subscription["subscription_id"]


def main():
    first = submit("task", "invoice-issuer", {"invoice_id": PREFIX + "1"}, "-task-1")
    one = result("task", first)
    second = submit("task", "invoice-issuer", {"invoice_id": PREFIX + "2"}, "-task-2")
    two = result("task", second)
    # The canonical demo uses exactly one consumer/process slot.
    assert one["pid"] == two["pid"] and two["invocation"] == one["invocation"] + 1, (one, two)
    task_subscription = subscribe("task", second)  # Deliberately after completion.
    workflow = submit("workflow", "workflow-example",
                      {"urls": ["http://receiver:8091/page.txt"] * 4, "queue": "demo"}, "-workflow")
    output = result("workflow", workflow)
    assert output["page_count"] == 4 and output["summary"]["pages"] == 4, output
    workflow_subscription = subscribe("workflow", workflow)
    print(json.dumps({"passed": True, "task_ids": [first, second], "workflow_id": workflow,
                      "subscriptions": [task_subscription, workflow_subscription],
                      "reused_process_id": one["pid"], "workflow_output": output}, indent=2))


if __name__ == "__main__":
    main()
