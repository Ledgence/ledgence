---
title: Choose your local installation
description: Choose one of three ways to install Ledgence, then write and run your first task.
---

Start by choosing **one** installation method. All three give you the same local
Ledgence 0.6.1 services: a database, an orchestrator with Console, and a worker
that runs your programs.

| Method | Choose this when… | What you install |
| --- | --- | --- |
| **[1. CLI installer and Docker](/how-to/install-native)** — recommended | You want the shortest path to a working local installation. | The released CLI; `ledgence local up` manages the supplied Docker Compose kit. |
| **[2. Docker Compose directly](/how-to/install-compose)** | You already use Compose and want to manage the services yourself. | The released Compose kit and its published container images; the CLI is used later to create and publish programs. |
| **[3. Build the CLI from source](/how-to/build-cli-from-source)** | You want to compile the command-line tool yourself. | The Rust CLI from the 0.6.1 source tag, plus the same released Docker Compose kit. |

You do not need to follow all three. They are alternative ways to set up your
machine, not successive stages. The second and third methods do not require you
to build the runtime container image.

## Before you choose

Install and start [Docker Engine](https://docs.docker.com/engine/install/) or
[Docker Desktop](https://docs.docker.com/desktop/), using Linux containers and
**Docker Compose 2.23.1 or newer**. The published runtime images support
`linux/amd64` and `linux/arm64`.

The native CLI installer supports **Linux x86_64 with glibc** and **macOS Apple
silicon**. Linux release binaries are qualified on Ubuntu 24.04. There are no
published native Windows, Linux ARM64, macOS Intel, or Alpine/musl bundles in
0.6.1. Docker image support does not imply native CLI support on those systems.
[Building the CLI yourself](/how-to/build-cli-from-source) is a separate path,
not a guarantee that an unqualified host platform is supported.

The local stack supplies Python inside its worker container. You do **not** need
host Python just to start it. To write the programs in the tutorials, you will
also need Python 3.11+ and [uv](https://docs.astral.sh/uv/getting-started/installation/),
a Python project and dependency manager. The first task tutorial explains that
project setup separately.

## What you will have when installation is finished

- An API at `http://127.0.0.1:8080`.
- [Ledgence Console](http://127.0.0.1:8080/console/) to view programs and executions.
- A worker listening on queue `demo`, in tenant `acme` and namespace `demo`.
- An empty program registry, ready for your first program.

The supplied credentials and loopback address are for local development with
trusted code. This is not a production deployment recipe.

## Next: write a small task

Once your chosen installation is running, follow **[Your first task: Hello
Ledgence](/tutorials/first-task)**. You will create a Python file in your editor,
publish it, and submit a task. The following tutorial builds a workflow around
that same task.

## Looking for a different setup?

- **Already have a Ledgence server?** Install just the CLI with method 1 and skip
  `local up`. For a Python application that only calls the API, use the
  [Python client](/reference/python-client); its package does not install the CLI
  or start a server.
- **Need manual archive installation or native services?** See the
  [advanced native installation guide](/how-to/manual-native-installation).
- **Developing Ledgence itself?** The [full source Compose walkthrough](/tutorials/run-locally)
  builds the platform image from a checkout. It is separate from the three
  application-development paths above.
- **Already have local data?** Read [Upgrade to 0.6.1](/how-to/upgrade-to-0-6-1)
  before changing that installation. Updating the CLI does not upgrade saved
  services or their database.
