# Two ticket reviews, one model call

This development example submits two tickets to independently owned classifier workflows. The root allows two delegations and one governed model call. Whichever child obtains the first Start can call the classifier. The other child leaves an inspectable refusal. The example reuses the [resource-admission classifier](../resource-admission/README.md).

Read [program.py](program.py) for the workflow and [submit.py](submit.py) for its budget. The limit belongs to the root submission. Neither child estimates a balance or passes a budget ID to its sibling.

## Run the complete example locally

Use the matching development checkout, a disposable PostgreSQL 18 instance, Python 3.11+ and `psql`. From the repository root:

```sh
cargo build --locked --all-features -p ledgence-cli --bin ledgence
python3 -m venv /tmp/ledgence-budget-client
/tmp/ledgence-budget-client/bin/python -m pip install ./sdk/python-client
```

Supply the URL of your disposable database server to the following command. The runner creates its own database, starts real workers and a local classifier, runs both programs, then removes the database and temporary credentials. Choose a fresh evidence directory each time.

```sh
LEDGENCE_POSTGRES_URL='postgres://postgres:local-password@127.0.0.1:5432/postgres' \
  python3 tools/check-resource-admission.py --run-budget \
    --binaries target/debug --python-client /tmp/ledgence-budget-client/bin/python \
    --evidence /tmp/ledgence-budget-check
```

Open `results.json` in the evidence directory. The first case parks both children, restarts the orchestrator and grants one call. `provider-census.json` independently records which ticket reached the classifier. The remaining cases exercise delegation exhaustion and an elapsed deadline.

## Use an existing installation

Enable instance credentials and resource admission, configure the `support-model` resource with capacity of at least two, and publish the `resource-admission` and `shared-run-budget` workflows. In each program’s `ledgence.toml`, set the target to the workers’ platform and CPython version, and replace the image placeholder with the digest-pinned worker image. Use the same worker queue for both programs. Start the local classifier on the worker host with `python3 examples/resource-admission/provider.py`. The sample endpoint accepts loopback connections, so run this example with workers that can reach it.

Run [submit.py](submit.py) from an environment with the matching Ledgence SDK. Supply an operator credential scoped to the chosen tenant and namespace with `execution_submit` to launch the root, `sensitive_read` to read its result, and `metadata_read` to inspect its budget. The operator who configures the `support-model` policy also needs `resource_control`.

```sh
python examples/shared-run-budget/submit.py --classifier http://127.0.0.1:8099 \
  --credentials ./operator.json --request-id ticket-review-001
```

Open the root execution to inspect its consumed allowances. You can also look up either child's budget to see the same root. Use a new request ID for a separate investigation.
