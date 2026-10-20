# Hello Ledgence

Two small, real programs for the introductory learning path:

1. [Run your first task](https://docs.ledgence.com/tutorials/first-task): publish
   `hello-ledgence@0.1.0` and submit a greeting.
2. [Run your first workflow](https://docs.ledgence.com/tutorials/first-workflow):
   publish `hello-workflow@0.1.0`, which calls the same greeting task and resumes
   when it finishes.
3. [Explore Console](https://docs.ledgence.com/tutorials/use-console): inspect
   those executions, including the workflow's actual child task.

The Python files here match the named files in those lessons. Start the local
Ledgence **0.6.1** distribution first, with its API at `http://127.0.0.1:8080`,
tenant `acme`, namespace `demo`, and queue `demo`. You need the Ledgence CLI,
Python 3.11 or newer, uv, and a local Docker engine using Linux containers.
No model provider, API key, or external service is needed.

## Run the checked-in programs

These directories already contain the scaffolded configuration, so do not run
`program new` inside them. In each `ledgence.toml`, choose the worker target
before building. `docker info --format '{{.Architecture}}'` reports the local
Docker engine's architecture: use `linux/arm64` for `aarch64` and `linux/amd64`
for `x86_64`. The Python version and pinned image are for the published 0.6.1
local distribution. A different worker requires its own matching target.

From this directory, run the task first:

```sh
cd task
uv sync
uv run python -m unittest discover -s tests -v
ledgence program build
ledgence program publish --server http://127.0.0.1:8080 --register
uv run python submit.py
```

Then run the workflow:

```sh
cd ../workflow
uv sync
uv run python -m unittest discover -s tests -v
ledgence program build
ledgence program publish --server http://127.0.0.1:8080 --register
uv run python submit.py
```

Both print an execution ID and `Hello, Ledgence!`. Keep the IDs to find those
executions in Console. The workflow creates a new greeting task; it does not
reuse the earlier standalone task's execution.

Repeated submissions with the unchanged idempotency key and input reconnect
to the same execution. Change the key when intentionally requesting new work,
including when changing the name. A result timeout only stops the client wait;
it neither cancels nor resubmits the execution.

## Development and publication

`uv sync` installs both `ledgence-client==0.6.1` and
`ledgence-worker==0.6.1` into the project environment. The former submits work;
the latter supplies workflow authoring imports and local editor/test support.
The server's Rust worker supplies the runtime helper. Only `program.py` is
selected as application source; `submit.py` and tests remain local.

The task tests cover input handling. Workflow unit tests cover registration and
successful, failed, and cancelled child outcome handling. They do not prove
checkpoint persistence or dispatch; the tutorial exercises those against a
running stack.

Each program version is immutable. To change deployed code, increment its
version in both `ledgence.toml` and `pyproject.toml`, update references in the
client or workflow as needed, then use a fresh build output such as
`ledgence program build --output .ledgence/prepared-0.1.1` and publish it with
`--source .ledgence/prepared-0.1.1`. The builder does not overwrite its previous
output. Re-submitting an unchanged program needs no rebuild or republish.

A fresh build can produce different archive bytes after a toolchain or runtime
change, even when application source is unchanged. If the same program ID and
version already exists with different bytes, choose a new application version;
never replace the published artifact.
