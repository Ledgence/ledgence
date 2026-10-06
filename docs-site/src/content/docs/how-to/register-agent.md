---
title: Register an agent
description: Add a published immutable program to the self-hosted Console catalog and manage its descriptive metadata.
---

Register a program so operators can discover its exact versions, inspect runtime requirements, and start it from **Programs** in Console. Registration is independent of execution: submitting a task does not automatically add its package to the catalog.

This guide applies to Ledgence 0.4.0. Start a matching instance using [Explore Ledgence Console](/tutorials/use-console).

## Publish the package first

Prepare the application, its dependencies, and `ledgence-program.json` for the worker's target Python version and platform. Publish with the matching `ledgence` executable from the native bundle or a source build:

```sh
ledgence program publish \
  --source /absolute/path/to/prepared-package \
  --store /absolute/path/to/program-store
```

Replace the paths with your prepared package and the store configured on the orchestrator. These paths must refer to the same published bytes from the server's point of view; writing to an arbitrary host directory does not populate a Docker volume.

Publication stores immutable package bytes. Registration then asks the orchestrator to fetch and verify that package's archive, descriptor, and manifest without executing it. For package preparation and store layout, see the [program package contract](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/program-packages.md).

If you are following the Compose tutorial, its `publish` command already publishes and registers all three example programs. You do not need to register those examples again.

## Register with the CLI

Assuming the published manifest identifies `invoice-issuer` version `1.0.0`:

```sh
ledgence program register \
  --server http://127.0.0.1:8080 \
  --program invoice-issuer --version 1.0.0 --kind task
```

If you are working from source without installing the executable, replace `ledgence` with `cargo run --locked -p ledgence-cli --` in these commands and run them at the repository root.

Choose the intended use explicitly:

| Value | Meaning |
| --- | --- |
| `task` | An ordinary program invocation. |
| `workflow` | A controller implementing the workflow protocol. |
| `unspecified` | Leave the operator to choose when submitting. |

The catalog does not infer business intent from a runtime protocol. Versions are exact opaque strings: `release-september` is valid if it matches the manifest, and there is no automatic “latest” version selection.

Open **Programs**, select the program, and choose its version. Confirm that its digest and runtime requirements match the intended package. Registration can accept a package for another supported platform; an available worker must still satisfy that package's requirements before it can run.

## Register from Console

As an alternative to the CLI:

1. Open **Programs → Register program**.
2. Enter the **Program ID** and **Exact version** from the published manifest.
3. Set **Declared use** to **Task**, **Workflow controller**, or **Unspecified**.
4. Optionally add a display name and description, then select **Register reference**.
5. Use **Inspect registered version** after verification succeeds.

This form registers an existing reference in the configured store. It does not upload source files, install dependencies, build a package, or deploy workers.

## Update descriptive metadata

Registering the same reference, bytes, and metadata again is idempotent. To deliberately replace an existing registration's descriptive fields:

```sh
ledgence program register \
  --server http://127.0.0.1:8080 \
  --program invoice-issuer --version 1.0.0 --kind task \
  --display-name "Invoice issuer" \
  --description "Create an invoice from an approved request." \
  --update-metadata true
```

In Console, open the registered version, choose **Register / update metadata**, and enable **Replace descriptive metadata for an existing registration**. Supply every descriptive value you intend to retain: replacement can clear omitted optional fields.

Metadata changes never replace the artifact digest. Publish changed code or dependencies under a new version and register that new exact reference.

## Recover from an interrupted registration

Publication and registration are separate operations. If publication succeeds but registration fails, retain the valid immutable artifact and retry registration. Do not delete the package as a rollback.

A conflict can mean that the same reference already identifies different bytes, or that descriptive metadata differs without explicit replacement. Check the reference and store; use a new version for different bytes and metadata replacement only for descriptive changes.

Console does not automatically repeat a write after a transport error. Its manual retry retains the same in-memory command. Review [command and retry behavior](/reference/console#commands-and-retries) before intentionally starting a separate operation.
