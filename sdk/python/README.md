# Ledgence Python helper

MIT-licensed, standard-library-only program support. The Rust worker starts the
configured CPython executable with `-I -S` and this directory's bootstrap. Python
is a separately installed runtime; Ledgence does not embed or download CPython.
Programs declare an exact supported Python major/minor (at least 3.11), with code
and vendored dependencies in their immutable artifact directory.

The worker supplies the `ledgence_worker` context helper alongside the runner;
applications do not need to vendor that helper. Application dependencies belong
in the program artifact. The bootstrap loads its own helper first, then adds the
absolute artifact directory to Python's import path.

The worker sets a separate temporary working directory for each session. Relative
writes go there and persist across that session's invocations. Confirmed session
cleanup removes this directory. Read packaged resources relative to the program
module's `__file__`, not the working directory. Directory separation is file
lifecycle management and does not change OS access permissions.

Expose a synchronous callable as `module:function`. It receives the complete
CloudEvent dictionary and returns a JSON-compatible value. `current_invocation()`
provides transport execution identifiers when needed. The helper starts a fresh
`contextvars.Context` for every invocation; process/module globals intentionally
persist. Programs must clean up other invocation state and background work.

One process handles one invocation at a time. Business exceptions produce typed
failure results. Coroutines and non-JSON/non-finite output are rejected. A protocol
fault terminates the process. Python and native stdout writes are redirected to
stderr; an isolated descriptor carries bounded JSON-lines responses. Parent-side
log capture must continue draining stderr even after its retention limit.

This process boundary executes trusted code with the worker's OS permissions.
The helper is not a sandbox and cannot undo external effects on retries.
