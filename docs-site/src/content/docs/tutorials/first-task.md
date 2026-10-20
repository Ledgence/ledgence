---
title: Run your first task
description: Create a small Python program, publish it, and get a greeting back from a real Ledgence worker.
---

Let's make Ledgence say **Hello, Ledgence!** You will create a Python project,
read its code in your editor, publish it to your local installation, and submit
a task from a short Python script.

A **program** is the code you publish. A **task** is one execution of that
program with a particular input. Publishing makes code available; submitting
asks a worker to run it.

## Before you start

- Complete one of the [local installation paths](/getting-started/installation)
  and leave the Ledgence **0.6.1** stack running at `http://127.0.0.1:8080`.
  The three paths lead to the same starting point for this lesson.
- Have Python **3.11 or newer** and
  [uv](https://docs.astral.sh/uv/getting-started/installation/) installed on your
  computer. We use uv to create each project's Python environment and install
  its declared dependencies.
- Keep Docker running with Linux containers. Building the program uses the
  worker's Linux runtime, even when your editor runs on macOS.

You do not need a model provider, an API key, Rust, or a clone of the Ledgence
repository. If you have an older saved local stack, follow the
[upgrade guide](/how-to/upgrade-to-0-6-1) first; installing a newer CLI alone does
not upgrade its running services.

## 1. Create a project

In your terminal, choose a folder where you keep projects and run:

```sh
ledgence program new hello-ledgence
cd hello-ledgence
uv sync
```

Open the new `hello-ledgence` folder in your editor. The files you will use are:

| File | Purpose |
| --- | --- |
| `program.py` | The Python code the worker will run. |
| `tests/test_program.py` | Local tests for that code. |
| `pyproject.toml` | The project and its development dependencies. |
| `ledgence.toml` | The program's name, version, source files, and worker target. |
| `requirements.txt` | Dependencies to package with the program; this example needs none. |

`uv sync` creates `.venv` and installs both Ledgence Python packages. The client
package lets `submit.py` talk to the API; the worker package provides workflow
authoring imports for your editor and tests. You do not need a separate `pip`
installation. Choose `.venv` as your editor's Python environment if it does not
automatically detect it.

## 2. Read and test the task

The CLI has already written this functional `program.py`. Keep it as generated:

```python title="program.py"
"""A task performs work and returns a JSON-serializable result."""


def greet(name: str) -> dict[str, str]:
    """Keep business logic independent of the execution envelope."""
    if not isinstance(name, str) or not name.strip() or len(name) > 200:
        raise ValueError("name must contain 1 to 200 characters and not be blank")
    return {"message": f"Hello, {name}!"}


def handle(event: dict) -> dict[str, str]:
    """CloudEvent data belongs to the application; platform IDs stay outside it."""
    return greet(event["data"]["name"])
```

`handle` is the entry function Ledgence calls. The input you submit arrives in
`event["data"]`; the rest of the CloudEvent envelope carries execution and trace
identifiers. `greet` keeps the application logic separate and returns a JSON
object.

Run the generated tests from the `hello-ledgence` directory:

```sh
uv run python -m unittest discover -s tests -v
```

All three tests should pass. They check the greeting, rejected names, and how
`handle` reads the input. This tests the Python code locally; the next steps run
it through Ledgence.

## 3. Choose the worker runtime once

Your development computer and your worker can have different runtimes. The
local installation's worker runs **Linux with Python 3.14** inside Docker.
Check the architecture of that local Docker engine:

```sh
docker info --format '{{.Architecture}}'
```

| Command output | `platform` value in `ledgence.toml` |
| --- | --- |
| `x86_64` | `linux/amd64` |
| `aarch64` | `linux/arm64` |

In your editor, replace the whole generated `[target]` section in
`ledgence.toml` with the following section. **If the command printed
`aarch64`, change `platform` to `"linux/arm64"`.** Leave the rest of the file
unchanged.

```toml title="ledgence.toml — replace [target]"
[target]
# Use linux/arm64 instead if the local Docker worker is aarch64.
platform = "linux/amd64"
python = "3.14"
protocol = 3
image = "docker.io/ledgence/ledgence@sha256:baffb9d9a84bd811e14ffdd7c3e02a36c5c6294eba0bc39cb50aa652ddb3216a"
```

The image digest selects the exact published **0.6.1** runtime used by the local
installation. Python `3.14` here describes the worker, not a requirement to
change the Python version in your editor. This one-time configuration lets the
builder prepare files for the right worker. For a different deployment, use
[that worker's actual target](/how-to/build-and-publish-programs#before-you-start).

## 4. Build and publish

From the `hello-ledgence` directory:

```sh
ledgence program build
ledgence program publish --server http://127.0.0.1:8080 --register
```

The first command prepares a package in `.ledgence/prepared`. The second uploads
that package to your local instance and registers it in Console. Both commands
print JSON reports when they succeed. Neither command runs the task.

Your program is now **`hello-ledgence` version `0.1.0`**. That is your
application's version; it is independent of Ledgence's `0.6.1` version. Leave
this terminal in the project directory for the next step.

## 5. Submit the task from a Python file

Create a file named **`submit.py`**, next to `program.py`, and paste:

```python title="submit.py"
"""Submit a greeting to a running local Ledgence instance."""
import asyncio

from ledgence.client import AsyncClient


async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        task = await client.tasks.submit(
            program="hello-ledgence",
            version="0.1.0",
            queue="demo",
            data={"name": "Ledgence"},  # This becomes event["data"] in program.py.
            idempotency_key="hello-task-1",  # Reuse this key to find the same task.
        )
        print("Task:", task.id)
        output = await task.result(timeout=60)  # Wait here for up to 60 seconds.
        print(output["message"])


if __name__ == "__main__":
    asyncio.run(main())
```

Here, `program` and `version` select the package you published. `queue="demo"`
selects the queue your local worker consumes. The local installation uses
`tenant="acme"` and `namespace="demo"` to group its executions.

Run the file:

```sh
uv run python submit.py
```

You should see a task ID followed by:

```text
Hello, Ledgence!
```

The Python script sends a request to Ledgence. A worker retrieves your published
program, runs it with `{"name": "Ledgence"}`, and saves the result. The client
then reads that result. `submit.py` runs on your computer; it is not packaged
with `program.py`.

Keep the printed task ID. You will use it in the
[Console walkthrough](/tutorials/use-console).

## 6. Try another input

Run `submit.py` again unchanged. The same input and `idempotency_key` return the
same task, so an accidental rerun does not request another execution.

To run new work, edit **both** fields in `submit.py`:

```python title="submit.py — change these two arguments"
data={"name": "Ada"},
idempotency_key="hello-task-2",
```

Run the file again. You should get a new task ID and **Hello, Ada!**. You have
changed an execution's input, so there is no need to rebuild or republish the
program. Reusing an old key with different input is a conflict, not an update.

`result(timeout=60)` limits how long this client waits. A timeout **does not
cancel the task or submit it again**. Keep its ID and use
`client.tasks.handle(task_id)` to reconnect later; see
[Receive results](/how-to/receive-results) for complete examples.

:::note[If you edit the program later]
Published versions are immutable. Before publishing changed `program.py` bytes,
use a new application version in `ledgence.toml` and `pyproject.toml`, and build
to a fresh output directory. The
[build and publication guide](/how-to/build-and-publish-programs#build-once)
explains that next step. You do not need it to finish this lesson.
:::

**Next:** [Run your first workflow](/tutorials/first-workflow) to compose this
same greeting task. Keep your stack running and the published task available.

**Example files:** [Hello Ledgence](https://github.com/Ledgence/ledgence/tree/develop/examples/hello-ledgence).
