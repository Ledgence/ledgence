"""Offline tests: real ADK orchestration with a scripted provider, never Gemini."""
import asyncio
import contextlib
import hashlib
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

AGENT = Path(__file__).resolve().parents[1] / "agent" / "program.py"
spec = importlib.util.spec_from_file_location("support_demo_agent_program", AGENT)
program = importlib.util.module_from_spec(spec)
spec.loader.exec_module(program)


def event(**changes):
    return {"data": {"ticket_id": "ticket-1", "question": "Does a result timeout cancel my task?", **changes}}


def draft(**changes):
    return {
        "classification": "how_to",
        "reply": "A client observation timeout does not cancel your task. [task-results]",
        "source_ids": ["task-results"],
        **changes,
    }


class ValidationTests(unittest.TestCase):
    def prepared_docs(self):
        docs = program.Documentation()
        docs.search_docs("task result timeout")
        docs.read_doc("task-results")
        docs.model_calls = 3
        return docs

    def test_input_default_model_and_multiline_question(self):
        result = program.ticket_input(event(question="A question\nwith a tab\tand Unicode café"))
        self.assertEqual(result["model"], "gemini-3.8-flash")
        self.assertEqual(result["ticket_id"], "ticket-1")

    def test_invalid_inputs_fail_before_a_provider_is_created(self):
        bad = [
            {}, {"data": None}, {"data": {}}, event(ticket_id="with space"),
            event(ticket_id="é"), event(ticket_id="a" * 129), event(question=" "),
            event(question="é" * 4097), event(question="x\x00"), event(question="\ud800"),
            event(question="x\x85"), event(question="x\ufdd0"),
            event(model=""), event(model="x\n"), event(model="x" * 129),
            event(api_key="not-accepted"),
        ]
        with patch.object(program, "_create_model") as provider:
            for value in bad:
                with self.subTest(value=repr(value)[:80]), self.assertRaises(program.AgentError):
                    program.handle(value)
            provider.assert_not_called()

    def test_only_google_api_key_environment_is_accepted(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "ignored-test-key"}, clear=True):
            with self.assertRaisesRegex(program.AgentError, "GOOGLE_API_KEY"):
                program.handle(event())

    def test_corpus_matches_its_recorded_source_bytes(self):
        manifest = json.loads((program.CORPUS / "SOURCE.json").read_text())
        self.assertRegex(manifest["source_revision"], r"^[a-f0-9]{40}$")
        self.assertEqual(set(manifest["documents"]), set(program.DOCUMENTS))
        for key, entry in manifest["documents"].items():
            self.assertEqual(entry["location"], program.DOCUMENTS[key]["location"])
            self.assertEqual(entry["sha256"], hashlib.sha256((program.CORPUS / f"{key}.md").read_bytes()).hexdigest())

    def test_tools_search_and_read_real_documentation(self):
        docs = program.Documentation()
        results = docs.search_docs("task result timeout")["matches"]
        self.assertIn("task-results", [value["id"] for value in results])
        document = docs.read_doc("task-results")
        self.assertIn("result", document["text"])
        self.assertIn("timeout", document["text"])
        self.assertEqual(document["location"], "docs/task-results.md")
        self.assertEqual(docs.read_ids, {"task-results"})
        self.assertEqual(docs.tool_calls, 2)

    def test_tools_require_search_and_exact_corpus_ids(self):
        docs = program.Documentation()
        with self.assertRaisesRegex(program.AgentError, "Search"):
            docs.read_doc("task-results")
        docs.search_docs("task")
        for key in ("../program.py", "/etc/passwd", "not-a-document", None):
            with self.subTest(key=key), self.assertRaisesRegex(program.AgentError, "Unknown"):
                docs.read_doc(key)
        self.assertEqual(docs.read_ids, set())

    def test_tool_budget_counts_real_executions_and_stops_before_ninth(self):
        docs = program.Documentation()
        for _ in range(program.MAX_TOOL_CALLS):
            docs.search_docs("task")
        with self.assertRaisesRegex(program.AgentError, "tool-call budget"):
            docs.search_docs("task")
        self.assertEqual(docs.tool_calls, 8)

    def test_model_callback_budget_stops_before_seventh(self):
        docs = program.Documentation()
        for _ in range(program.MAX_MODEL_CALLS):
            docs.before_model(None, None)
        with self.assertRaisesRegex(program.AgentError, "model-call budget"):
            docs.before_model(None, None)
        self.assertEqual(docs.model_calls, 6)

    def test_draft_uses_canonical_metadata_and_stays_small(self):
        result = program.validated_output(json.dumps(draft()), program.ticket_input(event()), self.prepared_docs())
        self.assertEqual(result["sources"], [program.DOCUMENTS["task-results"]])
        self.assertEqual((result["model_calls"], result["tool_calls"]), (3, 2))
        self.assertLess(len(json.dumps(result).encode()), program.MAX_OUTPUT_BYTES)

    def test_draft_rejects_unread_fabricated_or_duplicate_sources(self):
        for ids in ([], ["workflows"], ["invented"], ["task-results", "task-results"], [None]):
            with self.subTest(ids=ids), self.assertRaises(program.AgentError):
                program.validated_output(json.dumps(draft(source_ids=ids)), program.ticket_input(event()), self.prepared_docs())

    def test_draft_rejects_wrong_schema_types_and_oversized_reply(self):
        bad = [
            [], draft(extra=True), draft(classification="approved"),
            draft(classification=[]), draft(reply=""), draft(reply="é" * 4097),
            draft(reply="x\x00"), draft(reply="\ud800"), draft(source_ids="task-results"),
        ]
        for value in bad:
            with self.subTest(value=repr(value)[:80]), self.assertRaises(program.AgentError):
                program.validated_output(json.dumps(value), program.ticket_input(event()), self.prepared_docs())

    def test_draft_rejects_duplicate_json_keys_and_non_json(self):
        for raw in ('{"classification":"how_to","classification":"troubleshooting"}', "not JSON", "```json\n{}\n```"):
            with self.subTest(raw=raw), self.assertRaises(program.AgentError):
                program.validated_output(raw, program.ticket_input(event()), self.prepared_docs())

    def test_output_requires_search_read_and_model_observations(self):
        docs = self.prepared_docs()
        for field, value in (("searched", False), ("model_calls", 0), ("model_calls", 7), ("tool_calls", 9)):
            with self.subTest(field=field, value=value), patch.object(docs, field, value):
                with self.assertRaises(program.AgentError):
                    program.validated_output(json.dumps(draft()), program.ticket_input(event()), docs)


class AdkRunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # The optional demo dependencies, not the core repository, supply ADK.
        os.environ["OTEL_SDK_DISABLED"] = "true"
        try:
            from google.adk.models.base_llm import BaseLlm
            from google.adk.models.llm_response import LlmResponse
            from google.genai import types
            from pydantic import PrivateAttr
        except ImportError as error:
            raise unittest.SkipTest("Install the demo's locked ADK dependencies to run provider-mocked integration tests") from error

        class ScriptedModel(BaseLlm):
            _script: list = PrivateAttr(default_factory=list)
            _requests: list = PrivateAttr(default_factory=list)

            async def generate_content_async(self, llm_request, stream=False):
                self._requests.append(llm_request.model_copy(deep=True))
                response = self._script.pop(0)
                if isinstance(response, Exception):
                    logging.getLogger("google_adk.test").error("provider secret-test-key raw-body")
                    raise response
                yield LlmResponse(content=types.Content(role="model", parts=response))

        cls.ScriptedModel = ScriptedModel
        cls.types = types

    def call(self, name, **args):
        return self.types.Part.from_function_call(name=name, args=args)

    def final(self, **changes):
        return self.types.Part(text=json.dumps(draft(**changes)))

    def script(self, final=None):
        return [
            [self.call("search_docs", query="task result timeout")],
            [self.call("read_doc", document_id="task-results")],
            [self.final() if final is None else final],
        ]

    def run_script(self, responses):
        model = self.ScriptedModel(model="mock-provider")
        model._script = responses
        transport = SimpleNamespace(aio=SimpleNamespace(aclose=AsyncMock()), close=Mock())
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}):
            with patch.object(program, "_create_model", return_value=(model, transport)):
                result = program.handle(event())
        return result, model, transport

    def test_actual_adk_runner_executes_both_tools_and_validates_final_json(self):
        result, model, transport = self.run_script(self.script())
        self.assertEqual(result["sources"], [program.DOCUMENTS["task-results"]])
        self.assertEqual(result["model_calls"], 3)
        self.assertEqual(result["tool_calls"], 2)
        self.assertEqual(len(model._requests), 3)
        tool_responses = [
            part.function_response
            for content in model._requests[-1].contents
            for part in (content.parts or [])
            if part.function_response
        ]
        read = next(response for response in tool_responses if response.name == "read_doc")
        self.assertIn("Task status and results", json.dumps(read.response))
        transport.aio.aclose.assert_awaited_once()
        transport.close.assert_called_once()

    def test_warm_process_invocations_get_fresh_loops_sessions_and_counters(self):
        loops = []
        sessions = []
        transports = []

        def create(model_name, api_key):
            loops.append(asyncio.get_running_loop())
            model = self.ScriptedModel(model="mock-provider")
            model._script = self.script()
            sessions.append(model)
            transport = SimpleNamespace(aio=SimpleNamespace(aclose=AsyncMock()), close=Mock())
            transports.append(transport)
            return model, transport

        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}), patch.object(program, "_create_model", side_effect=create):
            for _ in range(2):
                result = program.handle(event())
                self.assertEqual((result["model_calls"], result["tool_calls"]), (3, 2))
        self.assertIsNot(loops[0], loops[1])
        self.assertTrue(all(loop.is_closed() for loop in loops))
        self.assertTrue(all(len(model._requests[0].contents) == 1 for model in sessions))
        self.assertTrue(all(transport.aio.aclose.await_count == 1 for transport in transports))

    def test_actual_runner_cannot_accept_an_unread_citation(self):
        with self.assertRaisesRegex(program.AgentError, "actually read"):
            self.run_script(self.script(self.final(source_ids=["workflows"])))

    def test_actual_runner_cannot_skip_documentation_tools(self):
        with self.assertRaises(program.AgentError):
            self.run_script([[self.final()]])

    def test_actual_runner_stops_repeated_model_calls(self):
        model = self.ScriptedModel(model="mock-provider")
        model._script = [[self.call("search_docs", query="task")] for _ in range(10)]
        transport = SimpleNamespace(aio=SimpleNamespace(aclose=AsyncMock()), close=Mock())
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}), patch.object(program, "_create_model", return_value=(model, transport)):
            with self.assertRaises(program.AgentError):
                program.handle(event())
        self.assertEqual(len(model._requests), program.MAX_MODEL_CALLS)
        transport.aio.aclose.assert_awaited_once()

    def test_actual_runner_stops_large_parallel_tool_batch(self):
        with self.assertRaises(program.AgentError):
            self.run_script([[self.call("search_docs", query="task") for _ in range(9)]])

    def test_provider_failure_is_sanitized_and_transport_is_closed(self):
        model = self.ScriptedModel(model="mock-provider")
        model._script = [RuntimeError("secret-test-key raw-body")]
        transport = SimpleNamespace(aio=SimpleNamespace(aclose=AsyncMock()), close=Mock())
        output = io.StringIO()
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}), patch.object(program, "_create_model", return_value=(model, transport)), contextlib.redirect_stderr(output), contextlib.redirect_stdout(output):
            with self.assertRaises(program.AgentError) as raised:
                program.handle(event())
        self.assertNotIn("secret-test-key", str(raised.exception))
        self.assertEqual(output.getvalue(), "")
        self.assertTrue(raised.exception.__suppress_context__)
        transport.aio.aclose.assert_awaited_once()
        transport.close.assert_called_once()

    def test_execution_deadline_cancels_provider_and_closes_transport(self):
        started = []
        stopped = []

        async def hang(model, llm_request, stream=False):
            started.append(True)
            try:
                await asyncio.Event().wait()
                yield
            finally:
                stopped.append(True)

        model = self.ScriptedModel(model="mock-provider")
        transport = SimpleNamespace(aio=SimpleNamespace(aclose=AsyncMock()), close=Mock())
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}), patch.object(program, "_create_model", return_value=(model, transport)), patch.object(self.ScriptedModel, "generate_content_async", hang), patch.object(program, "MAX_RUN_SECONDS", 0.05):
            with self.assertRaisesRegex(program.AgentError, "execution budget"):
                program.handle(event())
        self.assertEqual(started, [True])
        self.assertEqual(stopped, [True])
        transport.aio.aclose.assert_awaited_once()
        transport.close.assert_called_once()

    def test_gemini_client_uses_explicit_key_no_vertex_and_one_http_attempt(self):
        from google import genai
        from google.adk.models import google_llm
        with patch.object(genai, "Client") as client, patch.object(google_llm, "Gemini") as model:
            result, owner = program._create_model("gemini-3.8-flash", "unit-test-placeholder")
        self.assertIs(owner, client.return_value)
        self.assertIs(result, model.return_value)
        arguments = client.call_args.kwargs
        self.assertEqual(arguments["api_key"], "unit-test-placeholder")
        self.assertIs(arguments["vertexai"], False)
        self.assertEqual(arguments["http_options"].retry_options.attempts, 1)
        self.assertEqual(arguments["http_options"].timeout, 20000)
        self.assertEqual(arguments["http_options"].base_url, "https://generativelanguage.googleapis.com")


if __name__ == "__main__":
    unittest.main()
