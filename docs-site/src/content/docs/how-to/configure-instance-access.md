---
title: Configure instance access
description: Provision operator and worker credentials and connect Ledgence clients.
---

This capability is available on `develop` with matching CLI, server, worker,
Python client and Console components. The published 0.7.0 distributions use the
operator-trusted deployment model.

An installation can authenticate individual operators, service clients and
workers using locally provisioned credentials. The optional access adapter
verifies credentials in memory and checks the route's capabilities before
reading a command or starting an upload. Portable contracts remain independent
of the credential format and identity provider.

## Set up a local instance

Run these commands from the repository root using an existing PostgreSQL
connection. Build matching Rust and Console components with the repository's
Rust toolchain, Node.js 24.21.0 and pnpm 11.27.1.

```sh
cargo build -p ledgence-cli --bin ledgence --locked
pnpm --dir console install --frozen-lockfile --ignore-scripts
pnpm --dir console build
```

The commands below select the development executable explicitly. The
[HTTP deployment guide](https://github.com/Ledgence/ledgence/blob/develop/docs/http-orchestration.md) describes the database and program
store. Save this instance configuration as `instance.json` in your working
folder.

```json
{
  "instance_id": "local-access",
  "name": "Local Ledgence",
  "scope": {"tenant_id": "acme", "namespace": "demo"},
  "allowed_origins": ["http://127.0.0.1:8080"]
}
```

Provision a new private directory. Choose an expiry that fits your credential
rotation schedule. This example expires on December 1, 2026.

```sh
./target/debug/ledgence access init \
  --instance-config instance.json \
  --server http://127.0.0.1:8080 \
  --directory access \
  --subject alice \
  --expires-at 2026-12-01T00:00:00Z
```

The directory contains three files.

- `access.json` holds the installation authority, credential verifiers,
  capabilities and expirations. Mount it on each orchestrator.
- `operator.json` holds Alice's reusable credential and its intended server.
  It grants all operator capabilities.
- `worker.json` holds a separate credential for the trusted worker plane.

Keep the two client files private. The directory has Unix mode `0700` and its
files have mode `0600`. Provisioning reports paths and credential identifiers.
Treat client credential files as secrets when choosing backup or transfer tools.
Mount access and client configuration as regular files. Unix loaders reject a
symlink in the final path component and reject pipes or devices.

Apply the current migrations, then start the API with access enabled. Configure
`DATABASE_URL` for your own PostgreSQL installation before these commands.

```sh
mkdir -p release-store
./target/debug/ledgence orchestrator migrate
./target/debug/ledgence orchestrator serve \
  --store ./release-store \
  --console-dir ./console/dist \
  --instance-config instance.json \
  --access-config access/access.json \
  --allow-program-publication \
  --bind 127.0.0.1:8080 \
  --allow-insecure-console-access
```

The publication flag enables uploads to this instance's store. Each upload also
requires a credential with `program_publish`. Omit that flag if programs are
published directly into your configured store.

The last flag enables HTTP cookies for an explicitly loopback-only development
listener and origin. For a remote deployment, configure a dedicated HTTPS
hostname in `allowed_origins`, provision client files for that HTTPS URL, and
terminate TLS at your proxy. Omit the local HTTP flag. Serve Console and the API
from the same origin.

Once access is enabled, the database binds the installation to its authority.
Every later startup requires matching access configuration. Enabling access on
an existing installation requires draining all API replicas and starting each
with the same authority configuration. A running older process retains its
previous configuration until stopped.

## Connect clients and workers

Add the credential file to API-facing CLI commands.

```sh
./target/debug/ledgence task list \
  --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo \
  --credentials-file access/operator.json
```

The same option works with approval commands, program registration, HTTP
publication and `ledgence mcp serve`. Supply it again with a saved publication
receipt's `--resume` command. Publication receipts retain operation identity and
artifact data. Client credentials are supplied separately for each invocation.

For a connected worker, add `--credentials-file access/worker.json` to your
existing `ledgence worker connect` command. Both execution traffic and the
separate observation reporter use it. The artifact store uses its own transport.

In Python, save client code in a file such as `inspect_tasks.py` and use the
normal client constructor.

```python
from ledgence.client import AsyncClient

client = AsyncClient(
    "http://127.0.0.1:8080",
    tenant="acme",
    namespace="demo",
    credentials_file="access/operator.json",
)
```

Use the client through its asynchronous context manager as shown in the
[client guide](/reference/python-client). Applications with their own secret
loader can pass `token=` instead. The two options are mutually exclusive.
Credential files are bound to their normalized server URL. Credential-bearing
clients require HTTPS or loopback HTTP and reject redirects.

For MCP, give the configured service its own narrowly scoped credential. A
shared MCP process authenticates as that service. Its downstream human users
remain identities managed by the MCP host.

## Console identity

Open `/console/` and paste the `token` value from your private operator file into
the access form. Console exchanges it for a host-only HttpOnly cookie. The API
validates the credential and current configured capabilities on each request.
The sidebar identifies the authenticated subject.

Console binds resource requests and prepared commands to the verified actor
that produced the screen. Identity changes clear cached payloads and prepared
commands. Authentication loss hides protected views. An uncertain approval can
be retried deliberately with identical bytes after reauthentication as the same
actor, with the same instance and capabilities, within that page's lifetime. A full browser reload retrieves the persisted
server state. Frozen browser commands live only in memory.

**Sign out of this browser** clears the browser credential. Administrative
revocation requires removing its verifier from the server configuration and
restarting every replica. Cookies are shared across ports on a hostname. Use a
dedicated hostname for production and treat local HTTP as trusted development.

## Capabilities

| Capability | Permitted operations |
| --- | --- |
| `metadata_read` | Status, history summaries, approval inbox, catalog, Console metadata and the scoped payload-free operator audit feed |
| `sensitive_read` | Inputs, outputs, approval arguments and accepted local-step details |
| `execution_submit` | Task and workflow submissions |
| `execution_control` | Cancellation, external events and completion subscription commands |
| `program_publish` | Publication to the configured store and catalog registration |
| `approval_decide` | Approval decisions, together with `sensitive_read` for their returned snapshot |
| `worker_execute` | Acquisition, runtime context, claims, renewals, settlements, observations and raw attempt inspection |

Operator and service credentials receive explicit operator capabilities. Worker
credentials receive `worker_execute`. Workers share the installation's trusted
worker plane. Raw attempt inspection requires worker authority because its
response contains live lease details. Console's attempt projection provides
operator-facing diagnostics.

Metadata includes names and diagnostic messages supplied by trusted programs.
Application code controls values it places in those fields. Artifact publication
authorizes the instance's entire configured store. Use a dedicated store for
each access-isolated instance.

Authentication errors return `401 unauthenticated`. Insufficient authority
returns `403 forbidden`. These are definitive rejections in the CLI, Python
client and MCP. Ordinary transport failures retain their existing uncertain
outcome semantics.

## Add and rotate credentials

Issue a narrower credential into another new directory.

```sh
./target/debug/ledgence access issue \
  --config access/access.json \
  --server http://127.0.0.1:8080 \
  --directory support-access \
  --subject support-service \
  --class service \
  --capabilities metadata_read,sensitive_read \
  --expires-at 2026-12-01T00:00:00Z
```

The new directory contains `credential.json` and a complete replacement
`access.json` with the additional verifier. Deploy that replacement to every API
replica and restart them. Keep the authority and subject stable when rotating a
credential for the same actor. During rotation, keep both verifiers until clients
use the new credential, then remove the old verifier and restart all replicas.
Permissions, expiry and revocation follow the loaded configuration and API host
clock. Synchronize host clocks.

Access configuration is opt-in for the native service. The packaged local kits
continue their operator-trusted deployment contract and checksum validation.
Use explicit native service configuration for this development capability.

Native credential mode requires durable approval attribution and sensitive-response
audit. Approval `reviewer` remains caller supplied. Recorded acceptance identifies
the verified actor separately. Read the operating and retention contract before
enabling access on an existing deployment. [Operator audit](/how-to/audit-operators).
