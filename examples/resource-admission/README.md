# Share model-call capacity

**Requires the current development checkout.** Published Ledgence 0.8.0 does not
include this capability. This example uses a deterministic local HTTP classifier;
it needs no model subscription, provider key or external service.

Open [`program.py`](program.py) in your editor. Its flow has two entrypoints:

1. `request` freezes a registered operation and parks while `support-model` has
   no capacity. The worker process can run another task.
2. `call` uses the offered capacity, makes one HTTP request and returns its
   accepted result. An expired offer causes an explicit rewait with the original
   operation identity. A replay reads the retained result without another call.

`@workflow.operation` registers `classify`; `.bind()` freezes its effective
arguments. The network client is created when the function runs. No credentials
or client object are serialized. The transport uses one bounded request without
automatic retries or redirects. [`provider.py`](provider.py) records its HTTP
activity independently, so repeated calls cannot hide behind a successful workflow.

## Run the complete local check

Use a source build, Python 3.11+, `psql` and an explicitly disposable PostgreSQL 18
server. The check creates and later deletes its own randomly named database on
that server, along with temporary credentials. It leaves your other databases
and running Ledgence installations alone. Run from the repository root:

```sh
cargo build -p ledgence-cli --bin ledgence --all-features --locked
python3 -m venv /tmp/ledgence-admission-client
/tmp/ledgence-admission-client/bin/python -m pip install ./sdk/python-client
LEDGENCE_POSTGRES_URL='postgres://postgres:local-password@127.0.0.1:5432/postgres' python3 tools/check-resource-admission.py --binaries target/debug --python-client /tmp/ledgence-admission-client/bin/python --evidence /tmp/ledgence-admission-check
```

The temporary virtual environment installs the client from your checkout so the
check can exercise its public API. Replace the connection URL with your disposable
test server's URL. That variable
selects the test server; it is not sent to the classifier. The evidence directory
must be new. `summary.json` records each scenario, delivery mode, cleanup and
credential scan. `provider-census.json` records calls and concurrent HTTP activity.

The check configures credentials and admission, publishes this program, starts
real workers and exercises parking/restart, competing capacity, lost responses,
offer expiry, worker loss after result acceptance and restore quarantine.
To repeat against your own local ElasticMQ instance, add
`--endpoint http://127.0.0.1:9324` and choose a new evidence directory. The harness
creates and removes its own queue and checks durable broker claims. It uses a
one-second idle receive wait to keep shutdowns short; lease durations and
recovery assertions are unchanged. This validates ElasticMQ, not real AWS SQS.

## Use the pattern in your installation

Follow [durable resource admission](../../docs/resource-admission.md) to enable
`--admission-config` and create the `support-model` resource. Set
`max_occupancy=1` to serialize these calls, optionally with a token-bucket rate.
Use an operator credential with `resource_control` to configure policies;
workers use their separate `worker_execute` credential.

For local authoring, `uv sync --project examples/resource-admission` installs
this checkout's client and helper into a normal development environment.
The source package contains only `program.py`; use `ledgence.toml` to choose your
actual worker platform, CPython version and pinned build image before publishing.

You can start the classifier manually with:

```sh
python3 examples/resource-admission/provider.py --port 8099
```

Submit `resource-admission` version `1.0.0` as a workflow with
`data={"endpoint": "http://127.0.0.1:8099", "text": "Please explain my invoice"}`.
The loopback endpoint must be reachable from the worker's host; the synthetic
provider intentionally supports native local testing, not cross-container routing.
A successful result is `{"category": "billing"}`.

## What this demonstrates

The limit controls explicit callback authorizations. It does not discover hidden
requests inside arbitrary SDKs. An uncertain Start can consume capacity without
any HTTP call; an uncertain external call can keep capacity occupied after the
workflow fails. Inspect the original occurrence before explicitly releasing it.
Release is an audited operator decision, not proof of completion or permission
to repeat an external effect. Protected provider composition and standalone task
admission are later work.
