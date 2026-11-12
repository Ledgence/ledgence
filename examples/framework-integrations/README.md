# Framework proposals with controller-owned writes

These three peer examples use real optional SDKs and deterministic offline model
or transport fixtures. Each reads a static policy and proposes a notice. The
shared Ledgence controller then requests approval, waits for shared capacity,
dispatches the saved notice through a simulated provider, and waits for its result.
SDK callbacks have no approval, admission or dispatch authority.

This is **current development functionality for 0.10**, not functionality in the
published 0.9 worker helper. Use matching components from this source checkout.
No model account, credentials, live provider, Claude CLI or CLI download is needed
for the checks below. Live provider and proprietary CLI behavior is not exercised.

| Example | Exact optional SDK | Accepted replay boundary |
| --- | --- | --- |
| [PydanticAI](pydantic_program.py) | `pydantic-ai-slim==2.66.0` | Individual model and read-tool results |
| [Google ADK](google_adk_program.py) | `google-adk==2.14.0`, `google-genai==2.34.0` | Individual model and read-tool results |
| [Claude Agent SDK](claude_program.py) | Source-only `claude-agent-sdk==0.2.180` | Complete finished run; replay constructs no client |

PydanticAI uses the public `FunctionModel`; ADK uses a public `BaseLlm` subclass;
Claude uses a public custom transport that simulates an external runtime's read.
None is a real AI response. The Claude fixture does not claim per-tool replay or
prove that an external process exited. A crash before a result is accepted can
repeat the corresponding model/read operation or the entire Claude run.

## Run the offline checks

From the repository root, run the controller check with the base SDK alone:

```sh
PYTHONPATH=sdk/python:sdk/python/tests python3.14 -m unittest test_framework_examples -v
```

The three optional SDK cases skip when their exact dependencies are absent. The
controller cases still exercise real `WorkflowContext` approval, admission,
dispatch and result-wait behavior, including refusal and unchanged saved intent.
They use an in-memory RPC fixture, not an orchestrator or database.

For one framework, use its own virtual environment and corresponding requirements
file. These files pin the SDK versions; retain and review the complete resolved
transitive dependency inventory and legal notices when preparing an application.
For example, after installing the PydanticAI requirements in that environment:

```sh
python -m pip install -r examples/framework-integrations/requirements-pydantic.txt
PYTHONPATH=sdk/python:sdk/python/tests python -m unittest \
  test_framework_examples.FrameworkControllerTests.test_pydantic_sdk_proposal_and_replay -v
```

For ADK use `requirements-adk.txt` and
`test_framework_examples.FrameworkControllerTests.test_adk_sdk_proposal_and_replay`.
For Claude, install **only an independently reviewed source-only wheel and its
reviewed dependencies** from a local wheelhouse; the requirements file disables
package indexes so it cannot fall back to an upstream wheel with a bundled CLI:

```sh
python -m pip install --find-links /path/to/reviewed-source-only-wheelhouse \
  -r examples/framework-integrations/requirements-claude.txt
PYTHONPATH=sdk/python:sdk/python/tests python -m unittest \
  test_framework_examples.FrameworkControllerTests.test_claude_sdk_proposal_and_replay -v
```

Each actual-SDK check blocks network and subprocess calls, runs the proposal twice
with accepted records, and checks that the read runs only once. A skipped case is
not a framework qualification pass. These examples include no vendor SDK, wheel,
CLI or runtime binary in the repository.

## Follow the controller bridge

[framework_common.py](framework_common.py) shares one controller across all three
examples:

1. `propose` validates input and receives bounded JSON from the selected adapter.
   It freezes the proposal, provider contract, target and `notice-write` resource
   into one approval request.
2. `reserve` reads the original approval wake. A refusal completes without a
   dispatch. Approval requests capacity for that exact saved intent.
3. `dispatch` consumes the existing admission/provider grant with
   `ctx.admitted_provider_operation()`. Fresh event data supplies no replacement
   arguments. If capacity or recovery needs another wait, the workflow parks.
4. `result` reads the selected provider outcome. Only provider success supplies a
   receipt.

The provider is intentionally a **simulated write** returning a receipt. It has
no real notice board or external lookup. An uncertain real dispatch must remain
under review; `ProviderLookup.UNAVAILABLE` does not invent evidence or authorize
another write. For a complete local HTTP provider with recovery probes and a
real orchestrator/database check, see [approved provider admission](../provider-admission/README.md).

A workflow submission uses ordinary user-owned data:

```json
{"topic":"maintenance","notice_key":"notice-1","queue":"workflows"}
```

When preparing a protocol-3 workflow program, include `framework_common.py` and
one selected entry module, with handler `pydantic_program:handle`,
`google_adk_program:handle`, or `claude_program:handle`. Package that application's
reviewed optional dependencies, including distribution metadata and notices.
Use the [program build/publication guide](../../docs/cli.md) and configure the
`notice-write` resource as in the provider-admission example. Large ADK packages
require the new package profile and upgraded workers; a published 0.9 worker is
not sufficient.

## Replace a fixture deliberately

Keep the selected model, settings, read-tool versions and configuration explicit.
Construct a fresh native agent/client in the factory for each invocation. ADK
requires `close=` for each bound model: this resource-free fixture uses `None`;
a real provider client needs an async cleanup callable. Never capture a global
async client across worker event loops.

Native tool permissions, pure transform hooks and model suggestions do not
replace Ledgence approval. Put external mutations in the controller's provider
adapter and preserve the approved arguments through admission and recovery.
Keep provider credentials outside journaled semantic configuration. Provider
stop acknowledgement, transport closure and external effect completion remain
separate facts.
