# Durable action approval

This example proposes a simulated refund of 100, normalizes its amount to 50
**before** requesting approval, and saves both the original proposal and the
effective action. The reviewer sees `amount: 50` and `currency: USD`. Approval
executes those saved arguments through a durable local step. Rejection and expiry
resume without executing the operation; workflow cancellation closes the pending
request. No payment provider or external account is used.

Run the local example check from the repository root with Python 3.11 or newer:

```sh
python3.13 examples/durable-approval/check.py
```

The check supplies simulated activation records and verifies normalization,
approval, restart replay, rejection, and expiry. It does not exercise PostgreSQL
or claim production delivery guarantees.

For a real orchestrator run, build the worker, orchestrator, and Python client
from this checkout, apply all database migrations, and use the setup in the
[checkpoint workflow example](../checkpoint-workflow/README.md). The worker must
use the same CPython major/minor used to prepare this package. Prepare a new
directory and publish it to the configured program store:

```sh
"$LEDGENCE_PYTHON" examples/durable-approval/prepare.py "$workflow_demo/approval/program"
./target/debug/ledgence program publish --source "$workflow_demo/approval/program" --store "$workflow_demo/store"
```

With an open `ledgence.client.AsyncClient` named `client`, submit the workflow:

```python
run = await client.workflows.submit(
    program="durable-approval", version="1.0.0", queue="workflows",
    data={"amount": 100}, idempotency_key="refund-demo:1",
)
print(run.id)
```

Inspect `await run.approvals()` or use the workflow's **General → Approvals** view in
Console after its first activation. A key does not exist until the workflow has
committed its request. Reconnect at any time with
`client.workflows.handle(workflow_id)` and inspect the stored effective action:

```python
import json
from pathlib import Path
from ledgence.client import ApprovalStatus

approval = await run.approval("refund")
assert approval.status is ApprovalStatus.PENDING
print(approval.proposed_arguments)  # {"amount": 100}
print(approval.action.arguments)    # {"amount": 50, "currency": "USD"}

# The reviewer chooses the decision after inspecting the effective action.
command = run.prepare_approval_decision(
    approval, decision_id="refund-demo:review-1", decision="approve",
    reviewer="local-demo-reviewer", reason="Reviewed the effective amount",
)
Path("refund-decision.json").write_text(json.dumps(command.to_dict()))
receipt = await run.decide_approval(command)
print(await run.result(timeout=60))
```

The output reports a simulated refund of 50. To demonstrate rejection, submit a
new workflow with a new idempotency key and choose `decision="reject"`. To
demonstrate immediate expiry, submit `data={"amount": 100, "timeout_ms": 0}`.
The default approval deadline is one day; waiting releases the worker slot.

If decision delivery is uncertain, retain `ApprovalDecisionUncertain.command`
and explicitly resend it. After restarting the client, restore the saved command
with `run.restore_approval_decision(json.loads(Path("refund-decision.json").read_text()))`
and pass it to `run.decide_approval(...)`. Identical retries return the original
decision; changed decisions or action bindings conflict. Restore commands only
from application-owned storage.

`reviewer` is claimed attribution. Deployment authentication and authorization
must establish who can inspect and decide a request; a name in a JSON command
does not prove identity. `approved_local` checks the callable's module, qualified
name, and supplied version and reads a private copy of the approved arguments.
Operator-trusted Python code still controls its own behavior. Code that calls an
external service must use an external idempotency key or reconcile uncertainty:
an effect can succeed before the local result is durably acknowledged. This
example does not promise exactly-once external effects.
