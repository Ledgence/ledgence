import json
import math
import os
from pathlib import Path
import unittest

from ledgence.client import AsyncClient, InputError, ProtocolError, RetryPolicy, TraceContext
from ledgence.client import codec
from ledgence.client.models import Scope, parse_result, parse_status
from support import result, status


class CodecTests(unittest.TestCase):
    def test_shared_rust_python_fixtures(self):
        path = Path(os.environ.get("LEDGENCE_JSON_FIXTURES", str(
            Path(__file__).resolve().parents[3] / "tests/fixtures/json-values.json")))
        values = json.loads(path.read_text())
        kinds = {type(None): "null", bool: "bool", int: "integer", float: "float",
                 str: "string", dict: "object", list: "array"}
        for fixture in values:
            with self.subTest(fixture["name"]):
                def decode():
                    value = codec.decode(fixture["json"].encode())
                    codec.validate(value, codec.RESPONSE_LIMIT)
                    return value
                if fixture["valid"]:
                    value = decode()
                    self.assertEqual(kinds[type(value)], fixture["kind"])
                    restored = codec.decode(codec.encode(value))
                    self.assertEqual(type(value), type(restored))
                    self.assertEqual(value, restored)
                    if fixture["name"].startswith("negative-"):
                        self.assertEqual(math.copysign(1, value), -1)
                else:
                    with self.assertRaises((InputError, ProtocolError)):
                        decode()

    def test_reject_coercions_and_unsupported_objects(self):
        cycle = []; cycle.append(cycle)
        for value in ({1: "coerced"}, float("nan"), float("inf"), 1 << 64,
                      -(1 << 63) - 1, "\ud800", {"x": object()}, cycle):
            with self.subTest(value=type(value)):
                with self.assertRaises(InputError):
                    codec.encode(value)
        self.assertEqual(codec.decode(codec.encode((1, False))), [1, False])

    def test_strict_utf8_and_duplicate_keys(self):
        for raw in (b'{"x":1,"\\u0078":2}', '"hi"'.encode('utf-16'),
                    b'"\xff"', b'1 trailing', b'Infinity', b'{"x":1e400}'):
            with self.subTest(raw=raw):
                with self.assertRaises(ProtocolError):
                    codec.decode(raw)

    def test_exact_data_size_and_escaping(self):
        self.assertEqual(len(codec.encode("a" * (1024 * 1024 - 2), codec.DATA_LIMIT)), codec.DATA_LIMIT)
        with self.assertRaises(InputError):
            codec.encode("a" * (1024 * 1024 - 1), codec.DATA_LIMIT)
        self.assertEqual(codec.encode("\x00", 8), b'"\\u0000"')
        with self.assertRaises(InputError):
            codec.encode("\x00", 7)

    def test_identifier_policy_preserves_business_correlation(self):
        client = AsyncClient("http://localhost:8080", tenant="t", namespace="n")
        args = dict(program="echo", version="1", queue="q", data=None, idempotency_key="k")
        for key in ("", "\uffff"):
            prepared = client.tasks.prepare(**args, correlation_key=key)
            self.assertEqual(prepared.to_dict()["input"]["correlation_key"], key)
        for key in ("\n", "x" * 513):
            with self.assertRaises(InputError):
                client.tasks.prepare(**args, correlation_key=key)
        for value in (True, 1.0, -1, 1001):
            with self.assertRaises(InputError):
                RetryPolicy(value, 0)

    def test_trace_validation(self):
        parent = "00-" + "a" * 32 + "-" + "b" * 16 + "-00"
        self.assertEqual(TraceContext(parent, "vendor=value").traceparent, parent)
        for value in (parent.upper(), parent.replace("a" * 32, "0" * 32), "not a trace"):
            with self.assertRaises(InputError): TraceContext(value)
        for state in ("vendor=x,vendor=y", "Vendor=x", "vendor=", "vendor=\tbad", "v=" + "x" * 257):
            with self.assertRaises(InputError): TraceContext(parent, state)

    def test_status_relationships_and_identity(self):
        scope = Scope("tenant", "tests")
        valid = status("active")
        self.assertEqual(parse_status(valid, scope, "task").latest_attempt_id, "attempt")
        mutations = {"scope": {"tenant_id": "other", "namespace": "tests"},
                     "task_id": "other", "state": "unknown", "attempt_count": True,
                     "current_attempt_id": None, "latest_attempt_id": "other",
                     "terminal_at": 1, "extra": 1}
        for key, value in mutations.items():
            with self.subTest(key=key), self.assertRaises(ProtocolError):
                parse_status({**valid, key: value}, scope, "task")

    def test_terminal_outcome_relationships(self):
        scope = Scope("tenant", "tests")
        for value in (None, False, 0, [], {}, "x"):
            decoded = parse_result(result(output=value), scope, "task")
            self.assertEqual(decoded.outcome.output, value)
        invalid = [result("active"), result(), result(), result("cancelled")]
        invalid[0]["outcome"] = {"kind": "cancelled"}
        invalid[1]["outcome"]["attempt_id"] = "other"
        invalid[2]["outcome"]["execution_may_have_started"] = 1
        invalid[3]["outcome"]["output"] = "retained old output"
        for value in invalid:
            with self.assertRaises(ProtocolError): parse_result(value, scope, "task")

    def test_cancellation_and_timestamp_invariants(self):
        scope = Scope("tenant", "tests")
        for value in (status("cancelled"), status("queued"), status("succeeded"), status("active")):
            bad = dict(value)
            if value["state"] == "cancelled": bad["cancel_requested_at"] = None
            elif value["state"] == "active": bad["submitted_at"] = 1 << 63
            else: bad["cancel_requested_at"] = 2
            with self.assertRaises(ProtocolError): parse_status(bad, scope, "task")

    def test_terminal_execution_evidence(self):
        scope = Scope("tenant", "tests")
        success = result()
        success["outcome"]["execution_may_have_started"] = False
        application = result("failed", failure={"kind": "application", "error": {"kind": "error", "message": "x"}})
        application["outcome"]["execution_may_have_started"] = False
        lost = result("failed", failure={"kind": "attempt_lost"})
        for value in (success, application, lost):
            with self.assertRaises(ProtocolError): parse_result(value, scope, "task")
        lost["outcome"]["quiescence"] = "unconfirmed"
        for evidence in (True, False):
            lost["outcome"]["execution_may_have_started"] = evidence
            self.assertEqual(parse_result(lost, scope, "task").outcome.execution_may_have_started, evidence)

    def test_entire_compact_outcome_respects_settlement_limit(self):
        # The output itself fits 8 MiB; its required outcome fields make it too large.
        value = result(output="a" * (8 * 1024 * 1024 - 2))
        with self.assertRaises(ProtocolError):
            parse_result(value, Scope("tenant", "tests"), "task")
