---
title: Develop Python programs
description: Install the worker helper in your application environment, import workflow code, and test business logic before packaging it.
---

Install `ledgence-worker` in your application's development environment to use
`ledgence.worker` imports, editor completion and inline type information while
writing programs. The helper uses only the Python standard library. The separate
[`ledgence-client`](/reference/python-client) submits and observes executions.

**Development source only:** local packaging is available in updated `develop`
source as version **0.4.1**. `ledgence-worker` has not been published to PyPI, and
the existing `v0.4.0` source tag has no `sdk/python/pyproject.toml`. Use a checkout
containing that file. Version 0.4.1 is being prepared for release; 0.4.0 remains
the current published platform release. Do not install this package by name
from an index yet.

## Add the helper to your project

Use Python 3.11 or newer and your application's existing environment and package
manager. From an existing uv project, replace the path below with the absolute
path to your updated Ledgence checkout:

```sh
uv add --dev /absolute/path/to/ledgence/sdk/python
uv run python -c "from ledgence.worker.workflow import Workflow; from ledgence.worker import current_invocation; print('worker imports ready')"
```

This records a local development dependency and installs the versioned helper.
Select the project's `.venv` interpreter in your editor. You do not need to set
`PYTHONPATH`. Keep the checkout available for future dependency synchronization.
A local source entry or lockfile does not freeze that mutable directory's
contents; record and retain the exact checkout commit when sharing or reproducing
your setup. See
[uv's path dependency documentation](https://docs.astral.sh/uv/concepts/projects/dependencies/#path)
for how local sources are recorded.

If your project uses pip, activate its existing virtual environment and use:

```sh
python -m pip install /absolute/path/to/ledgence/sdk/python
python -c "from ledgence.worker.workflow import Workflow; print('worker imports ready')"
```

Only when changing the helper itself, choose an editable installation with
`uv add --dev --editable /absolute/path/to/ledgence/sdk/python`; helper source
edits then appear directly in that environment. A normal local installation is
the default for program authors.

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

Updated development source offers an explicit Docker preparation adapter and
HTTP publication with saved retry receipts. Follow [Build and publish Python
programs](/how-to/build-and-publish-programs) when both CLI and server support it.
It requires a target and digest-pinned worker runtime image and never infers a
Linux package from the laptop environment.

Follow [Register an agent](/how-to/register-agent) for existing filesystem publication,
[the program package contract](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/program-packages.md)
for manifests and artifact contents, and
[the worker helper guide](https://github.com/Ledgence/ledgence/blob/develop/sdk/python/README.md)
for local installation and runtime behavior.
