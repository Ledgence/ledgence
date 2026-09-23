# Python program example

`program.py` demonstrates a synchronous `program:handle` entry point. It receives
the complete CloudEvent, logs the current event and attempt identity, and
returns JSON containing the unchanged user input and subprocess PID.

Package this file together with any vendored dependencies. The manifest must
declare its immutable program ID/version, the exact host Python major/minor,
protocol version 2, and target operating system and architecture. Generate these
values for the worker host rather than copying a manifest from another machine.

Programs use `from ledgence.worker import current_invocation` for the invocation
context supplied by the platform SDK. `get_logger(__name__)` emits structured
records with snapshotted invocation identity and an optional active trace context. Python is a separately installed host
dependency; it is not included in this example package.
