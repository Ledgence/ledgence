# Ledgence Python helper

MIT-licensed, standard-library-only program support. The Rust worker starts the
configured CPython executable with `-I -S -B` and this directory's bootstrap. Python
is a separately installed runtime; Ledgence does not embed or download CPython.
Programs declare an exact supported Python major/minor (at least 3.11), with code
and vendored dependencies in their immutable artifact directory.

The worker supplies the `ledgence_worker` context helper alongside the runner;
applications do not need to vendor that helper. Application dependencies belong
in the program artifact. The bootstrap loads its own helper first, then adds the
absolute artifact directory to Python's import path. Bytecode generation is
explicitly disabled, so ordinary imports do not add `__pycache__` directories to
an artifact even when filesystem permissions would allow it.

The worker sets a separate temporary working directory for each session. Relative
writes go there and persist across that session's invocations. Confirmed session
cleanup removes this directory. Read packaged resources relative to the program
module's `__file__`, not the working directory. Directory separation is file
lifecycle management and does not change OS access permissions.

Expose a synchronous callable as `module:function`. Its module and package
parents must originate inside the artifact. Names already loaded by the bootstrap
(such as `json`, `os`, and `ledgence_worker`) cannot be handler modules; conflicts
are rejected before readiness. Use an application-specific module name. Regular
and namespace packages are supported, and application code may still import the
standard library normally.

The callable receives the complete
CloudEvent dictionary and returns a JSON-compatible value. `current_invocation()`
provides transport execution identifiers when needed. The helper starts a fresh
`contextvars.Context` for every invocation; process/module globals intentionally
persist. Programs must clean up other invocation state and background work.

One process handles one invocation at a time. Results support null, booleans,
Unicode scalar strings, integers from `-2**63` through `2**64 - 1`, finite binary64
floating-point values, arrays, and objects with string keys. Python tuples are
encoded as JSON arrays. At most 64 nested containers are permitted; a scalar has
depth zero. Cycles, non-string keys, lone surrogate characters, out-of-range
integers, non-finite numbers, coroutines, and oversized results produce typed
`invalid_output` failures and leave the process reusable. Rust checks the shared
wire profile when accepting the response.

The default protocol frame limit is 2 MiB, including result/input envelopes and
the newline. The worker supplies the configured limits explicitly when starting
the helper. The delivery submission limit remains 1 MiB of application data;
the additional frame capacity carries CloudEvent metadata and protocol fields.

Business exceptions produce typed failure results. Error text is shortened by the
complete frame's encoded UTF-8 byte budget, including event and attempt IDs, JSON
escaping, and the newline. The handler is not called if those IDs cannot fit even
an empty failure response. A protocol
fault terminates the process. Python and native stdout writes are redirected to
stderr; an isolated descriptor carries bounded JSON-lines responses. Parent-side
log capture must continue draining stderr even after its retention limit.

This process boundary executes trusted code with the worker's OS permissions.
The helper is not a sandbox and cannot undo external effects on retries.
