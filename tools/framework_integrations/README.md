# Optional framework qualification

These fixtures test the optional `ledgence.worker.integrations` adapters against
installed, pinned SDKs. They are development dependencies of the qualification
process; they are **not** included in Ledgence's helper wheel, native bundles or
container image. Application programs choose their own framework dependencies.

Run from the repository root with an explicitly selected Python interpreter:

```sh
python3.13 tools/check-framework-integrations.py \
  --framework pydantic_ai --python python3.13 \
  --evidence /tmp/ledgence-pydantic-qualification
```

Use a new evidence directory for each run. Select `google_adk` or
`claude_agent_sdk` to test the other profiles. The setup fetches the reviewed,
hash-pinned packages from PyPI. Tests prohibit provider/network connections and
child processes. Zero executed tests or skipped SDK tests fail qualification.
The helper can come from a release bundle with `--helper-root BUNDLE/runtime`;
its actual module origins are checked.

On macOS, use CPython 3.13 for the ADK fixture: its declared `watchdog>=6,<7`
dependency has no CPython 3.14 macOS wheel. The Linux qualification uses CPython
3.14 and the dependency's published Linux wheel. The gate neither overrides
upstream dependency constraints nor silently enables source builds.

Claude's official SDK wheel may include a separate runtime. This fixture instead
builds the hash-pinned official Python source through its unchanged Hatchling
backend. The build has no network/process access. The verifier checks the exact
source files, pure-Python wheel metadata, retained license and absence of bundled
native payloads. It exercises the actual public `ClaudeSDKClient` with an explicit
`CustomTransport`; it does not qualify a live Claude CLI, provider, account or
subscription. No native CLI is downloaded or executed.

For production-path acceptance, supply the successful evidence directory, a
current Ledgence binary/helper, and a disposable PostgreSQL server:

```sh
python3.13 tools/check-framework-workers.py \
  --framework pydantic_ai \
  --sdk-evidence /tmp/ledgence-pydantic-qualification \
  --binaries target/debug --runner sdk/python/ledgence/worker/bootstrap.py \
  --psql psql --evidence /tmp/ledgence-pydantic-workers
```

`LEDGENCE_POSTGRES_URL` must point to a local test server. The gate creates and
removes its own uniquely named database, publishes a complete host-specific SDK
fixture package, and starts real orchestrator/worker processes. It tests warm
reuse, handled SDK failures, failed-runtime retirement, process death before and
after durable acceptance, and operation-level recovery where the adapter supports
it. Lost acknowledgements are injected after the real server commits. Raw marker
counts distinguish accepted replay from repeated unaccepted work. The test does
not establish exactly-once external effects or qualify another platform's wheels.

`requirements/*.txt` and `reviewed-dependencies.json` record the selected fixture
graphs and license metadata. Third-party code retains its original terms. The
Claude source reference records the original archive hash. Setup and test logs,
source/helper identities, outcomes, skipped cases and cleanup results are retained
separately; a local pass is not a GitHub or publication result.
