"""A real predecessor cannot obtain an unfenced grant in a budgeted workflow (MIT)."""

from http_acceptance.harness import eventually
from provider_operations.harness import ProviderDeployment


def prior_worker(d, provider):
    bundle = d.previous_bundle
    key = "legacy-budget-claim"
    d.stop_workers()
    proxy = d.authenticated_proxy()
    worker = ProviderDeployment.worker(
        d,
        "late-origin",
        proxy,
        binaries=bundle / "bin",
        runner=bundle / "runtime/ledgence/worker/bootstrap.py",
        cache=d.directory / "legacy-budget-worker-cache",
    )
    try:
        occurrence, _ = d.begin(
            key, provider, program="provider-recovery", budget={"max_calls": 1}
        )
        root = occurrence["workflow_id"]
        path = "/v1/workflows/provider-operations/claim"
        eventually(
            lambda: proxy.commands(path),
            timeout=60,
            description="predecessor attempts protected provider dispatch",
        )
        eventually(
            lambda: d.status(root)["state"] in ("failed", "cancelled"),
            timeout=60,
            description="legacy worker settles the refused action",
        )
        query = {"scope": d.scope, "subject": {"kind": "workflow", "workflow_id": root}}
        snapshot = d.checked("POST", "/v1/budgets/inspect", query)
        assert snapshot["calls"] == "0" and d.provider_census(provider, key)["posts"] == 0
        return {
            "root": root,
            "prior_dispatch_refused": True,
            "calls": 0,
            "provider_posts": 0,
            "claim_requests": len(proxy.commands(path)),
        }
    finally:
        if worker.process.poll() is None:
            worker.stop()
