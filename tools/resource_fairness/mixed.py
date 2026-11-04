"""Native workflow and task callers contend in one weighted resource domain (MIT)."""

from http_acceptance.harness import eventually
from resource_admission.harness import classifier

from .scenario import await_entries, configure, history, occupancy_peak, wait_terminal


def mixed_callers(d):
    # Run the public example unchanged. Its operation calls a loopback-only
    # fixture that records every underlying request independently of Ledgence.
    d.resource_name = "support-model"
    configure(d, "weighted", capacity=1)
    gate = d.directory / "mixed-holder"
    provider = classifier(d.root)
    text = "weighted invoice"
    provider.hold(text)
    try:
        holder = d.submit_to(
            "batch", "mixed-holder", gate=gate, resource=d.resource_name
        )
        await_entries(d, {holder}, 1)
        batch = [
            d.submit_to("batch", f"mixed-batch-{i}", resource=d.resource_name)
            for i in range(4)
        ]
        command = d.submission(
            "mixed-workflow",
            program="resource-admission",
            endpoint=provider.url,
            text=text,
        )
        command["input"].update(queue="support", attempt_timeout_ms=60_000)
        workflow = d.checked("POST", "/v1/workflows", command)["workflow_id"]
        eventually(
            lambda: (value := d.admission(workflow)) and value["state"] == "waiting",
            description="workflow operation registered while batch occupies the slot",
        )
        gate.touch()
        eventually(
            lambda: provider.census()["active"] == 1,
            description="admitted workflow entered the local provider",
        )
        assert (
            d.resource()["occupied"] == 1
            and d.admission(workflow)["state"] == "possibly_dispatched"
        )
        provider.release(text)
        wait_terminal(d, [holder, *batch])

        def succeeded():
            result = d.checked(
                "GET", d.query("/v1/workflows/result", workflow_id=workflow)
            )
            if result["outcome"] is None:
                return False
            assert result["outcome"] == {
                "kind": "succeeded",
                "output": {"category": "billing"},
            }
            return True

        eventually(
            succeeded,
            description="workflow returned the actual local provider response",
        )
        snapshot = d.admission(workflow)
        assert snapshot["state"] == "finished" and provider.census()["requests"] == {
            text: 1
        }
        assert d.resource()["occupied"] == 0 and occupancy_peak(d) == 1
        decisions = history(d)
        assert len(decisions) == 6
        for i, decision in enumerate(decisions):
            s = decision["subject"]
            if s["kind"] == "workflow":
                assert s["workflow_id"] == workflow and s["key"] == "classify:0"
                assert (
                    s["origin_activation_id"]
                    == snapshot["occurrence"]["origin_activation_id"]
                )
                assert i <= 2 and decision["queue"] == decision["group"] == "support"
                assert (
                    decision["reason"] == "weighted"
                    and decision["eligible_groups"] == 2
                )
            else:
                assert (
                    s["kind"] == "task"
                    and decision["group"] == decision["queue"] == "batch"
                )
        assert sum(d["subject"]["kind"] == "workflow" for d in decisions) == 1
        assert [
            r["subject"]["task_id"] for r in decisions if r["subject"]["kind"] == "task"
        ] == [holder, *batch]
        for task in [holder, *batch]:
            d.output_for(task, "batch")
        return {
            "workflow_id": workflow,
            "provider_invocations": 1,
            "admission_peak": 1,
            "first_workflow_opportunity": next(
                i for i, r in enumerate(decisions) if r["subject"]["kind"] == "workflow"
            ),
            "decisions": decisions,
        }
    finally:
        gate.touch()
        provider.close()
