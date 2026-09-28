"""Small stdio MCP server exposing only the bundled documentation tools."""

import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile

# Codex runs this script with Python -I. Load the trusted sibling explicitly;
# neither the working directory nor caller-controlled Python paths are needed.
_SPEC = importlib.util.spec_from_file_location("ledgence_support_documentation", Path(__file__).with_name("program.py"))
program = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(program)

MAX_MESSAGE_BYTES = 64 * 1024
PROTOCOL_VERSIONS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
TOOLS = [
    {
        "name": "search_docs", "description": "Search the bundled Ledgence documentation. Read matching documents before citing them.",
        "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}},
                        "required": ["query"], "additionalProperties": False},
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
    {
        "name": "read_doc", "description": "Read one bundled document by its exact ID after searching the documentation.",
        "inputSchema": {"type": "object", "properties": {"document_id": {"type": "string", "enum": sorted(program.DOCUMENTS)}},
                        "required": ["document_id"], "additionalProperties": False},
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False},
    },
]


def rpc_error(identifier, code, message):
    return {"jsonrpc": "2.0", "id": identifier, "error": {"code": code, "message": message}}


def rpc_result(identifier, result):
    return {"jsonrpc": "2.0", "id": identifier, "result": result}


class Server:
    def __init__(self, audit_path):
        self.audit_path = audit_path
        self.docs = program.Documentation()
        self.initialized = False
        self.ready = False
        if audit_path.exists():
            # A client reconnect must not reset the invocation's tool budget.
            audit = program.read_audit(audit_path, complete=False)
            self.docs.tool_calls = audit["tool_calls"]
            self.docs.searched = audit["searched"]
            self.docs.read_ids = set(audit["read_ids"])
            self.docs.exhausted = audit["exhausted"]
        self.persist()

    def persist(self):
        body = json.dumps(self.docs.audit(), separators=(",", ":")).encode("utf-8")
        if len(body) > program.MAX_AUDIT_BYTES:
            raise program.AgentError("Documentation tool evidence exceeds its size limit")
        descriptor, temporary = tempfile.mkstemp(prefix=".documentation-audit-", dir=self.audit_path.parent)
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(body)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.audit_path)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def call_tool(self, params):
        try:
            self.docs._tool_call()
            # Reserve the call before executing it, even for invalid arguments.
            self.persist()
            if not isinstance(params, dict) or params.keys() - {"name", "arguments", "_meta"}:
                raise program.AgentError("Invalid documentation tool request")
            name, arguments = params.get("name"), params.get("arguments", {})
            if not isinstance(arguments, dict):
                raise program.AgentError("Documentation tool arguments must be an object")
            if name == "search_docs" and set(arguments) == {"query"}:
                result = self.docs._search(arguments["query"])
            elif name == "read_doc" and set(arguments) == {"document_id"}:
                result = self.docs._read(arguments["document_id"])
            else:
                raise program.AgentError("Unknown documentation tool or invalid arguments")
        except program.AgentError as error:
            self.persist()
            return {"content": [{"type": "text", "text": str(error)}], "isError": True}
        self.persist()
        return {"content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}], "isError": False}

    def dispatch(self, message):
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return rpc_error(None, -32600, "Invalid request")
        identifier = message.get("id")
        method = message.get("method")
        if not isinstance(method, str):
            return rpc_error(identifier if type(identifier) in (str, int) else None, -32600, "Invalid request")
        if "id" not in message:
            if method == "notifications/initialized" and self.initialized:
                self.ready = True
            return None
        if type(identifier) not in (str, int):
            return rpc_error(None, -32600, "Invalid request ID")
        params = message.get("params", {})
        if method == "ping":
            return rpc_result(identifier, {})
        if method == "initialize":
            if self.initialized:
                return rpc_error(identifier, -32600, "Server is already initialized")
            if (not isinstance(params, dict) or not isinstance(params.get("protocolVersion"), str)
                    or not isinstance(params.get("capabilities"), dict)
                    or not isinstance(params.get("clientInfo"), dict)):
                return rpc_error(identifier, -32602, "Invalid initialization parameters")
            selected = params["protocolVersion"]
            if selected not in PROTOCOL_VERSIONS:
                selected = PROTOCOL_VERSIONS[-1]
            self.initialized = True
            return rpc_result(identifier, {
                "protocolVersion": selected, "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "ledgence_docs", "version": "1.0.0"},
            })
        if not self.ready:
            return rpc_error(identifier, -32002, "Server is not initialized")
        if method == "tools/list":
            if not isinstance(params, dict) or params.keys() - {"_meta"}:
                return rpc_error(identifier, -32602, "Invalid tool list parameters")
            return rpc_result(identifier, {"tools": TOOLS})
        if method == "tools/call":
            return rpc_result(identifier, self.call_tool(params))
        return rpc_error(identifier, -32601, "Method not found")


def serve(server):
    while True:
        line = sys.stdin.buffer.readline(MAX_MESSAGE_BYTES + 1)
        if not line:
            return 0
        if len(line) > MAX_MESSAGE_BYTES:
            response = rpc_error(None, -32600, "Request exceeds its size limit")
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()
            return 2
        try:
            request = program._json(line, "Invalid JSON")
        except program.AgentError:
            response = rpc_error(None, -32700, "Parse error")
        else:
            response = server.dispatch(request)
        if response is not None:
            encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if len(encoded) + 1 > MAX_MESSAGE_BYTES:
                encoded = json.dumps(rpc_error(None, -32603, "Response exceeds its size limit")).encode("utf-8")
            sys.stdout.buffer.write(encoded + b"\n")
            sys.stdout.buffer.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-path", required=True, type=Path)
    args = parser.parse_args()
    try:
        # One process owns an invocation's audit at a time. A crashed server
        # releases this OS lock, and its replacement resumes the saved budget.
        descriptor = os.open(args.audit_path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, "wb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return serve(Server(args.audit_path))
    except (OSError, ValueError, UnicodeError):
        print("Documentation server could not maintain valid tool evidence", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
