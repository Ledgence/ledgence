---
title: Upgrade to 0.6.1
description: Adopt Curvature and the updated learning path while preserving existing programs, service data and local installations.
---

Ledgence 0.6.1 updates the visual identity and installation and first-program
learning path, and updates TLS certificate validation and bundled trust data.
It adds no PostgreSQL migrations, public API changes or execution
semantics relative to 0.6.0. Console contract **5** and workflow runtime protocol
**3** remain unchanged. For older installations, also review the historical
[0.6.0 upgrade guide](/how-to/upgrade-to-0-6).

The TLS updates reject distrusted emSign intermediary public keys in native
connections and remove an affected root certificate from the WebAssembly
dependency graph. See the [TLS maintenance notes](https://github.com/Ledgence/ledgence/blob/develop/docs/releases/0.6.1.md#dependency-maintenance)
for the affected advisories and exact dependency versions.

## Install the matching CLI

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/download/v0.6.1/install.sh | sh -s -- --version 0.6.1
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

The final command should print `ledgence 0.6.1`. New Bash/Zsh terminals load the
configured PATH automatically. Native bundles support Linux x86_64/glibc and
macOS arm64; the matching container runtime supports Linux amd64 and arm64.
Manual downloads are covered by [native installation](/how-to/manual-native-installation).

## Preserve existing installations

Updating the CLI does not replace an existing local installation's kit, pinned
image, Docker context, port, volumes or project identity. Do not edit checksummed
kit files or delete volumes as an upgrade. To evaluate 0.6.1 with separate data,
choose a new directory and a free port:

```sh
ledgence local up --directory "$HOME/.local/share/ledgence/local-0.6.1" --port 8087
ledgence local status --directory "$HOME/.local/share/ledgence/local-0.6.1"
ledgence local down --directory "$HOME/.local/share/ledgence/local-0.6.1"
```

Use that same directory for later lifecycle commands. `local down` preserves
data. This separate stack does not import an earlier installation; moving
retained data requires an explicit backup, restore and compatibility plan.

For a retained service deployment, keep tested backups of the database, program
store and instance configuration, plus the previous bundle and service options.
Plan the restart and replace orchestrator, workers, runtime helper and Console
assets with matching 0.6.1 components. Preserve immutable instance identity and
existing publication settings. The existing schema is checked at startup; there
is no new migration relative to 0.6.0. Verify readiness, a known task or workflow,
its retained history and Console after the restart.

## Match application dependencies

For an existing application, use its environment manager to update the client
and development helper together:

```sh
uv add "ledgence-client==0.6.1"
uv add --dev "ledgence-worker==0.6.1"
```

A project created by the 0.6.1 CLI already declares both packages in its
development group; run `uv sync` there. The running worker supplies its own
helper. Runtime application dependencies remain separately prepared for the
worker's platform and exact Python major/minor.

Custom adapters can use the 0.6.1 Rust API crates; their contracts are unchanged
from 0.6.0. Run your application checks before adopting the new deployment.
Keep existing immutable program artifacts and receipts. A platform upgrade
does not change application program versions or replace published archives.

**Source:** [0.6.1 release notes](https://github.com/Ledgence/ledgence/blob/develop/docs/releases/0.6.1.md) · [Release artifacts](https://github.com/Ledgence/ledgence/releases/tag/v0.6.1)
