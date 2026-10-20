---
title: Develop Python programs
description: Scaffold a task or workflow with the CLI, install the worker helper, and test business logic before packaging it.
---

Install `ledgence-worker` in your application's development environment to use
`ledgence.worker` imports, editor completion and inline type information while
writing programs. The helper uses only the Python standard library. The separate
[`ledgence-client`](/reference/python-client) submits and observes executions.

**Available in Ledgence 0.6.1.** Install a helper version matching the deployed
worker. The package does not install a CLI, service or local workflow execution
harness; invocation context still requires execution by Ledgence.

## Create a project with the CLI

**Available in Ledgence 0.6.1.** [Install the CLI](/how-to/install-native) and
ensure `ledgence --version` reports 0.6.1. Source builds remain available through
the [CLI reference](/reference/cli#build-and-inspect).

For the development and test commands below, install Python 3.11 or newer and
uv. In the directory where you keep application projects, create a task:

```sh
ledgence program new invoice-issuer
cd invoice-issuer
uv sync
uv run python -m unittest discover -s tests -v
```

The parent directory must already exist; `invoice-issuer` must not exist, even
as an empty directory or a symlink. The first command creates these files:

```text
invoice-issuer/
├── program.py
├── tests/test_program.py
├── pyproject.toml
├── ledgence.toml
├── requirements.txt
├── .gitignore
└── README.md
```

Scaffolding runs offline and invokes no Python, uv, Git, Docker or services.
The separate `uv sync` step installs the development environment and may access
the package index. `pyproject.toml` uses `package = false` and pins both
`ledgence-worker` and `ledgence-client` as development dependencies to the CLI
version that created it. The helper supports authoring programs; the client
provides `from ledgence.client import AsyncClient` to submit and inspect
executions against a running Ledgence service. Both are available after
`uv sync`, without additional dependency commands. They are not automatically
included in the program artifact: runtime dependencies belong in the build's
`requirements.txt`, and the worker supplies its own helper at execution time.
The initial program version is `0.1.0`. The directory basename becomes its ID;
use `--name ID` to choose a different ID without changing the directory name.
Names must begin and end with a lowercase letter or digit; dots, underscores and
hyphens are also accepted inside the name.

For a workflow, run this from your application projects directory instead:

```sh
ledgence program new invoice-flow --kind workflow
cd invoice-flow
uv sync
uv run python -m unittest discover -s tests -v
```

The workflow starter registers a typed default entrypoint and exposes
`program:handle`. The generated tests check business logic and handler imports;
they do not simulate durable workflow execution. Extend the handler and tests
for your application, then exercise orchestration through a real worker.
Both starters contain a functional greeting program: input
`data={"name": "Ada"}` produces `{"message": "Hello, Ada!"}` after build,
publication and execution.

The generated `ledgence.toml` has explicit target placeholders. Before building,
replace them with the worker's Linux platform, exact Python major/minor and
digest-pinned runtime image. You can also supply `--platform`, `--python` and
`--image` together to `program new`; see the [CLI reference](/reference/cli#create-a-python-program).
The command never selects a target from your laptop. `requirements.txt` starts
without production dependencies; add exact version pins and hashes when your
application needs packages. See [Build and publish Python programs](/how-to/build-and-publish-programs).

To add the helper to a project you already have, follow the next section.

## Add the helper to your project

Use Python 3.11 or newer and your application's existing environment and package
manager. From an existing uv project:

```sh
uv add --dev "ledgence-worker==0.6.1"
uv run python -c "from ledgence.worker.workflow import Workflow; from ledgence.worker import current_invocation; print('worker imports ready')"
```

Select the project's `.venv` interpreter in your editor; no `PYTHONPATH` is needed.
For pip, activate the application's existing virtual environment and run:

```sh
python -m pip install "ledgence-worker==0.6.1"
python -c "from ledgence.worker.workflow import Workflow; print('worker imports ready')"
```

To use a source checkout instead, run
`uv add --dev /absolute/path/to/ledgence/sdk/python` or install that same path with
pip. Keep the checkout available for dependency synchronization and retain its
exact commit: a local source entry does not freeze a mutable directory. Use
`uv add --dev --editable /absolute/path/to/ledgence/sdk/python` only when changing
the helper itself. See [uv path dependencies](https://docs.astral.sh/uv/concepts/projects/dependencies/#path).

The distribution owns `ledgence.worker` and its `py.typed` marker. It can coexist
with `ledgence-client` in the same environment: both use the native `ledgence`
namespace. Do not add a root `ledgence/__init__.py` to your application.

## Write a program with testable business logic

Save this as `program.py` in your application project:

```python
from enum import StrEnum

from ledgence.worker.workflow import Workflow


def count_words(text: str) -> int:
    return len(text.split())


class Entry(StrEnum):
    START = "start"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    text = event["data"]["text"]
    if not isinstance(text, str):
        raise ValueError("text must be a string")
    return ctx.complete({"words": count_words(text)})


handle = workflow.build()
```

Importing this module registers and validates its entrypoints. It does not run
the workflow. You can check the independent business function locally:

```sh
uv run python -c "from program import count_words; assert count_words('hello Ledgence') == 2; assert count_words('') == 0"
```

With pip, run the same check using `python` in your activated environment in
place of `uv run python`. Add ordinary unit tests for your own domain logic.

`current_invocation()` and `workflow_context()` require an active Ledgence
invocation and fail outside one. The installed helper does not provide an
offline workflow runner; importing `handle` or testing `count_words` does not
exercise checkpoint persistence, delivery, retries or recovery. Exercise the
workflow through a worker and orchestrator as shown in
[Your first workflow](/tutorials/first-workflow), using the
[workflow context reference](/reference/workflow-context) for the API contract.

## Prepare execution separately

Choose an installed helper version matching the worker release you deploy;
installation does not check the deployed worker's compatibility. This
package installs no CLI, server, Python interpreter or HTTP client. The Rust
worker continues to launch its own matching helper; its bootstrap loads that
copy before any helper vendored in your program artifact. The local package is
for authoring and tests, not a replacement for the worker's `--runner` setting.

Keep `ledgence-worker` as a development dependency. Prepare your actual
application dependencies for the worker's operating system, architecture and
exact Python major/minor. A development environment on your laptop is not
automatically a deployable program artifact. Publish code and prepared runtime
dependencies as an immutable program with handler `program:handle` and runtime
protocol **3**. The worker does not install dependencies during execution.

Ledgence 0.6.1 offers an explicit Docker preparation adapter and
HTTP publication with saved retry receipts. Follow [Build and publish Python
programs](/how-to/build-and-publish-programs) when both CLI and server support it.
It requires a target and digest-pinned worker runtime image and never infers a
Linux package from the laptop environment.

Follow [Register an agent](/how-to/register-agent) for existing filesystem publication,
[the program package contract](https://github.com/Ledgence/ledgence/blob/develop/docs/program-packages.md)
for manifests and artifact contents, and
[the worker helper guide](https://github.com/Ledgence/ledgence/blob/develop/sdk/python/README.md)
for local installation and runtime behavior.
