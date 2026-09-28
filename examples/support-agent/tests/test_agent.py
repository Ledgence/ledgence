"""Offline tests: real ADK orchestration with a scripted provider, never Gemini."""
import asyncio
import contextlib
from email.utils import formatdate
import hashlib
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
import socket
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import warnings

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


def model_response(part):
    return (200, {"candidates": [{"content": {"role": "model", "parts": [part]},
                                 "finishReason": "STOP"}]})


def support_responses():
    return [
        model_response({"functionCall": {"name": "search_docs", "args": {"query": "task result timeout"}}}),
        model_response({"functionCall": {"name": "read_doc", "args": {"document_id": "task-results"}}}),
        model_response({"text": json.dumps(draft())}),
    ]


def unavailable(*, headers=None, details=None):
    body = {"error": {"code": 503, "status": "UNAVAILABLE", "message": "private-provider-body"}}
    if details is not None:
        body["error"]["details"] = details
    return 503, body, headers or {}


@contextlib.asynccontextmanager
async def model_context(model, transport, budget):
    """Substitute only the model while retaining the runner's ownership scope."""
    model._budget = budget
    try:
        yield model, transport
    finally:
        await transport.aio.aclose()
        transport.close()


def model_factory(model, transport):
    return lambda name, key, budget: model_context(model, transport, budget)


class ValidationTests(unittest.TestCase):
    def prepared_docs(self):
        docs = program.Documentation()
        docs.search_docs("task result timeout")
        docs.read_doc("task-results")
        docs.model_calls = 3
        docs.http.attempts = 3
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

    def test_current_workflow_and_client_guides_are_searchable_and_readable(self):
        docs = program.Documentation()
        matches = docs.search_docs("typed entrypoints durable forks")["matches"]
        self.assertIn("workflow-entrypoints", [item["id"] for item in matches])
        guide = docs.read_doc("workflow-entrypoints")
        self.assertIn("await ctx.fork(", guide["text"])
        self.assertIn("ctx.join(", guide["text"])
        self.assertIn("@workflow.entrypoint", docs.read_doc("workflows")["text"])
        self.assertIn("WaitTimeout", docs.read_doc("python-client")["text"])

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
            _budget: object = PrivateAttr(default=None)

            async def generate_content_async(self, llm_request, stream=False):
                await self._budget.before_request(None)
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
            with patch.object(program, "_create_model", side_effect=model_factory(model, transport)):
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

        def create(model_name, api_key, budget):
            loops.append(asyncio.get_running_loop())
            model = self.ScriptedModel(model="mock-provider")
            model._script = self.script()
            sessions.append(model)
            transport = SimpleNamespace(aio=SimpleNamespace(aclose=AsyncMock()), close=Mock())
            transports.append(transport)
            return model_context(model, transport, budget)

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
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}), patch.object(program, "_create_model", side_effect=model_factory(model, transport)):
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
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}), patch.object(program, "_create_model", side_effect=model_factory(model, transport)), contextlib.redirect_stderr(output), contextlib.redirect_stdout(output):
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
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}), patch.object(program, "_create_model", side_effect=model_factory(model, transport)), patch.object(self.ScriptedModel, "generate_content_async", hang), patch.object(program, "MAX_RUN_SECONDS", 0.05):
            with self.assertRaisesRegex(program.AgentError, "execution budget"):
                program.handle(event())
        self.assertEqual(started, [True])
        self.assertEqual(stopped, [True])
        transport.aio.aclose.assert_awaited_once()
        transport.close.assert_called_once()

    def test_gemini_client_uses_explicit_key_no_vertex_and_bounded_http_retries(self):
        from google import genai
        from google.adk.models import google_llm
        async def create():
            async with program._create_model("gemini-3.8-flash", "unit-test-placeholder", program.HttpBudget()) as pair:
                return pair
        with patch.object(genai, "Client") as client, patch.object(google_llm, "Gemini") as model:
            client.return_value.aio.aclose = AsyncMock()
            result, owner = asyncio.run(create())
        self.assertIs(owner, client.return_value)
        self.assertIs(result, model.return_value)
        arguments = client.call_args.kwargs
        self.assertEqual(arguments["api_key"], "unit-test-placeholder")
        self.assertIs(arguments["vertexai"], False)
        self.assertEqual(arguments["http_options"].retry_options.attempts, 3)
        self.assertEqual(arguments["http_options"].retry_options.http_status_codes, [500, 502, 503, 504])
        self.assertEqual(arguments["http_options"].timeout, 20000)
        self.assertEqual(arguments["http_options"].base_url, "https://generativelanguage.googleapis.com")


class GeminiHttpTests(unittest.TestCase):
    """Exercise the actual Gemini/GenAI adapter without opening a socket."""

    @classmethod
    def setUpClass(cls):
        try:
            from google.adk.models.google_llm import Gemini
            from google.genai.errors import APIError
            import httpx
        except ImportError as error:
            raise unittest.SkipTest("Install the demo's locked ADK dependencies to run HTTP adapter tests") from error
        cls.APIError = APIError
        cls.httpx = httpx

    def run_http(self, responses, *, jitter=0.0, budget_seconds=None, sleep_action=None, caller_cancel_wait=None):
        """Run ADK, GenAI and HTTPX hooks; substitute only I/O, clocks and waits."""
        requests = []
        clients = []
        budgets = []
        timestamps = []
        waits = []
        clock = [100.0]
        wall_time = 1_800_000_000.0
        original_create = program._create_model
        original_run = program._bounded_run
        original_sleep = asyncio.sleep
        output = io.StringIO()
        caller = []

        async def bounded_run(*args):
            caller.append(asyncio.current_task())
            return await original_run(*args)

        @contextlib.asynccontextmanager
        async def create(model_name, api_key, budget):
            budgets.append(budget)
            async with original_create(model_name, api_key, budget) as pair:
                clients.append(pair[1])
                yield pair

        async def send(request):
            requests.append(request)
            timestamps.append(clock[0])
            self.assertTrue(responses, "Unexpected HTTP send after scripted responses")
            response = responses.pop(0)
            if isinstance(response, Exception):
                raise response
            if callable(response):
                return await response(request)
            status, body, *headers = response
            return self.httpx.Response(status, request=request, json=body,
                                       headers=headers[0] if headers else {})

        async def sleep(seconds):
            if seconds:
                waits.append(float(seconds))
                if len(waits) == caller_cancel_wait:
                    caller[0].cancel()
                    await original_sleep(0)
                if sleep_action:
                    await sleep_action(seconds)
                clock[0] += float(seconds)
            await original_sleep(0)

        result = error = None
        fake_time = SimpleNamespace(monotonic=lambda: clock[0], time=lambda: wall_time + clock[0] - 100.0)
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder",
                                    "OTEL_SDK_DISABLED": "true"}, clear=True), \
                patch.object(program, "_create_model", side_effect=create), \
                patch.object(program, "_bounded_run", bounded_run), \
                patch("httpx._client.AsyncHTTPTransport", return_value=self.httpx.MockTransport(send)), \
                patch.object(program, "time", fake_time), \
                patch("random.uniform", return_value=jitter), \
                patch.object(asyncio, "sleep", sleep), \
                patch.object(program, "MAX_RUN_SECONDS", program.MAX_RUN_SECONDS if budget_seconds is None else budget_seconds), \
                patch.object(socket.socket, "connect", side_effect=AssertionError("Network disabled in offline tests")) as connect, \
                patch.object(socket, "getaddrinfo", side_effect=AssertionError("DNS disabled in offline tests")) as dns, \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(output), \
                warnings.catch_warnings(record=True):
            try:
                result = program.handle(event())
            except (program.AgentError, asyncio.CancelledError) as caught:
                error = caught
        connect.assert_not_called()
        dns.assert_not_called()
        self.assertEqual(len(clients), 1)
        # These are the real client-owned transports, not fake cleanup methods.
        self.assertTrue(clients[0]._api_client._httpx_client.is_closed)
        self.assertTrue(clients[0]._api_client._async_httpx_client.is_closed)
        self.assertEqual(output.getvalue(), "")
        for request in requests:
            self.assertEqual(str(request.url),
                             "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.8-flash:generateContent")
            self.assertEqual(request.headers["x-goog-api-key"], "unit-test-placeholder")
        self.last_http = SimpleNamespace(budgets=budgets, timestamps=timestamps, waits=waits, clients=clients)
        return result, error, requests

    def test_real_provider_adapter_serializes_tools_and_validates_responses(self):
        parts = [
            {"functionCall": {"name": "search_docs", "args": {"query": "task result timeout"}}},
            {"functionCall": {"name": "read_doc", "args": {"document_id": "task-results"}}},
            {"text": json.dumps(draft())},
        ]
        responses = [
            (200, {"candidates": [{"content": {"role": "model", "parts": [part]},
                                   "finishReason": "STOP"}]})
            for part in parts
        ]
        result, error, requests = self.run_http(responses)
        self.assertIsNone(error)
        self.assertEqual((result["model_calls"], result["tool_calls"]), (3, 2))
        self.assertEqual(result["sources"], [program.DOCUMENTS["task-results"]])
        self.assertEqual(len(requests), 3)
        bodies = [json.loads(request.content) for request in requests]
        declarations = bodies[0]["tools"][0]["functionDeclarations"]
        self.assertEqual({tool["name"] for tool in declarations}, {"search_docs", "read_doc"})
        self.assertEqual(bodies[0]["generationConfig"]["maxOutputTokens"], 4096)
        tool_results = [part["functionResponse"]
                        for content in bodies[-1]["contents"] for part in content["parts"]
                        if "functionResponse" in part]
        read = next(value for value in tool_results if value["name"] == "read_doc")
        self.assertIn("Task status and results", read["response"]["text"])

    def test_503_recovers_inside_one_turn_with_counted_backoff_and_jitter(self):
        result, error, requests = self.run_http([unavailable(), unavailable(), *support_responses()], jitter=0.25)
        self.assertIsNone(error)
        self.assertEqual((result["model_calls"], result["tool_calls"]), (3, 2))
        self.assertEqual((result["http_attempts"], result["http_retries"], result["retry_wait_ms"]), (5, 2, 3500))
        self.assertEqual(self.last_http.waits, [1.25, 2.25])
        self.assertEqual([request.content for request in requests[:3]], [requests[0].content] * 3)

    def test_later_turn_retry_keeps_previous_tool_results_without_reexecuting_tools(self):
        first, second, final = support_responses()
        result, error, requests = self.run_http([first, second, unavailable(), final])
        self.assertIsNone(error)
        self.assertEqual((result["model_calls"], result["tool_calls"], result["http_retries"]), (3, 2, 1))
        self.assertEqual(requests[2].content, requests[3].content)
        results = [part["functionResponse"]["name"]
                   for content in json.loads(requests[-1].content)["contents"]
                   for part in content["parts"] if "functionResponse" in part]
        self.assertEqual(results, ["search_docs", "read_doc"])

    def test_persistent_503_stops_after_three_sends_without_a_final_wait(self):
        result, error, requests = self.run_http([unavailable()] * 3)
        self.assertIsNone(result)
        self.assertIn("HTTP 503", str(error))
        self.assertEqual(len(requests), 3)
        self.assertEqual(self.last_http.waits, [1.0, 2.0])
        self.assertEqual((self.last_http.budgets[0].attempts, self.last_http.budgets[0].retries), (3, 2))

    def test_final_request_attempt_does_not_wait_for_an_unusable_retry_hint(self):
        result, error, requests = self.run_http([unavailable(), unavailable(), unavailable(headers={"Retry-After": "90"})])
        self.assertIsNone(result)
        self.assertIn("HTTP 503", str(error))
        self.assertEqual(len(requests), 3)
        self.assertEqual(self.last_http.waits, [1.0, 2.0])

    def test_a_valid_final_response_on_exactly_the_sixth_send_is_accepted(self):
        first, second, final = support_responses()
        result, error, requests = self.run_http([unavailable(), unavailable(), first, unavailable(), second, final])
        self.assertIsNone(error)
        self.assertEqual(len(requests), 6)
        self.assertEqual((result["model_calls"], result["http_attempts"], result["http_retries"]), (3, 6, 3))
        self.assertEqual(result["retry_wait_ms"], 4000)

    def test_six_actual_sends_cap_multiple_turns_and_the_next_turn_is_not_sent(self):
        first, second, final = support_responses()
        responses = [unavailable(), unavailable(), first, unavailable(), unavailable(), second]
        result, error, requests = self.run_http(responses)
        self.assertIsNone(result)
        self.assertIn("HTTP-attempt budget", str(error))
        self.assertEqual(len(requests), 6)
        self.assertEqual(self.last_http.waits, [1.0, 2.0, 1.0, 2.0])
        self.assertEqual(self.last_http.budgets[0].retries, 4)

    def test_a_failure_at_the_global_limit_has_no_useless_backoff(self):
        first, second, final = support_responses()
        result, error, requests = self.run_http([unavailable(), first, unavailable(), second, unavailable(), unavailable()])
        self.assertIsNone(result)
        self.assertIn("HTTP-attempt budget", str(error))
        self.assertEqual(len(requests), 6)
        self.assertEqual(self.last_http.waits, [1.0, 1.0, 1.0])

    def test_invocation_http_budgets_are_fresh_when_the_process_is_reused(self):
        budgets = []
        for _ in range(2):
            result, error, requests = self.run_http([unavailable(), *support_responses()])
            self.assertIsNone(error)
            self.assertEqual((result["http_attempts"], result["http_retries"]), (4, 1))
            budgets.append(self.last_http.budgets[0])
        self.assertIsNot(budgets[0], budgets[1])

    def test_retry_after_and_retry_info_wait_for_the_later_deadline_not_both_delays(self):
        cases = (
            ({"Retry-After": "3"}, None, 3.0),
            ({"Retry-After": formatdate(1_800_000_003, usegmt=True)}, None, 3.0),
            ({}, [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "2.500s"}], 2.5),
            ({"Retry-After": "2"}, [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "3.250s"}], 3.25),
            ({"Retry-After": "0"}, None, 1.0),
            ({"Retry-After": formatdate(1_799_999_000, usegmt=True)}, None, 1.0),
        )
        for headers, details, expected in cases:
            with self.subTest(headers=headers, details=details):
                result, error, requests = self.run_http([unavailable(headers=headers, details=details), *support_responses()])
                self.assertIsNone(error)
                self.assertEqual(self.last_http.timestamps[1] - self.last_http.timestamps[0], expected)
                self.assertEqual(sum(self.last_http.waits), expected)
                self.assertEqual(result["retry_wait_ms"], int(expected * 1000))

    def test_malformed_hints_do_not_replace_sdk_backoff(self):
        bad = (None, "bad", [], {}, [{"retryDelay": "3s"}],
               [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "-2s"}],
               [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "NaNs"}])
        for details in bad:
            with self.subTest(details=details):
                result, error, _ = self.run_http([unavailable(headers={"Retry-After": "nonsense"}, details=details), *support_responses()])
                self.assertIsNone(error)
                self.assertEqual(result["retry_wait_ms"], 1000)

    def test_provider_delay_over_the_short_limit_fails_without_retrying_early(self):
        for headers, details in (
            ({"Retry-After": "5"}, None),
            ({}, [{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "4.001s"}]),
        ):
            with self.subTest(headers=headers, details=details):
                result, error, requests = self.run_http([unavailable(headers=headers, details=details)])
                self.assertIsNone(result)
                self.assertIn("longer retry delay", str(error))
                self.assertEqual(len(requests), 1)
                self.assertEqual(self.last_http.waits, [])

    def test_retry_hint_outside_remaining_deadline_aborts_before_any_sleep(self):
        result, error, requests = self.run_http([unavailable(headers={"Retry-After": "3"})], budget_seconds=2)
        self.assertIsNone(result)
        self.assertIn("execution budget", str(error))
        self.assertEqual(len(requests), 1)
        self.assertEqual(self.last_http.waits, [])

    def test_deadline_after_native_backoff_prevents_the_next_send(self):
        result, error, requests = self.run_http([unavailable()], budget_seconds=0.5)
        self.assertIsNone(result)
        self.assertIn("execution budget", str(error))
        self.assertEqual(len(requests), 1)

    def test_nonretryable_transport_error_is_not_broadened_to_all_network_errors(self):
        for cause in (self.httpx.ReadError("private-provider-body"), self.httpx.RemoteProtocolError("private-provider-body")):
            with self.subTest(cause=type(cause).__name__):
                result, error, requests = self.run_http([cause])
                self.assertIsNone(result)
                self.assertNotIn("private-provider-body", str(error))
                self.assertEqual(len(requests), 1)
                self.assertEqual(self.last_http.waits, [])

    def test_cancel_during_native_or_provider_wait_stops_sends_and_closes_clients(self):
        real_sleep = asyncio.sleep
        for cancel_on_wait in (1, 2):
            calls = []

            async def cancel(seconds):
                calls.append(seconds)
                if len(calls) == cancel_on_wait:
                    asyncio.current_task().cancel()
                    await real_sleep(0)

            with self.subTest(cancel_on_wait=cancel_on_wait):
                result, error, requests = self.run_http([unavailable(headers={"Retry-After": "3"})], sleep_action=cancel)
                self.assertIsNone(result)
                # ADK can consume cancellation of its internal producer task;
                # the invocation must still fail and cannot issue another send.
                self.assertIsInstance(error, (asyncio.CancelledError, program.AgentError))
                self.assertEqual(len(requests), 1)
                self.assertEqual(len(calls), cancel_on_wait)

    def test_inflight_http_request_is_cancelled_by_the_global_deadline(self):
        closed = []

        async def hang(request):
            try:
                await asyncio.Event().wait()
            finally:
                closed.append(True)

        result, error, requests = self.run_http([hang], budget_seconds=0.02)
        self.assertIsNone(result)
        self.assertIn("execution budget", str(error))
        self.assertEqual(len(requests), 1)
        self.assertEqual(closed, [True])

    def test_caller_cancellation_during_backoff_propagates_and_closes_clients(self):
        for wait in (1, 2):
            with self.subTest(wait=wait):
                result, error, requests = self.run_http([unavailable(headers={"Retry-After": "3"})], caller_cancel_wait=wait)
                self.assertIsNone(result)
                self.assertIsInstance(error, asyncio.CancelledError)
                self.assertEqual(len(requests), 1)

    def test_retry_after_survives_a_timeout_while_reading_the_error_body(self):
        httpx = self.httpx

        class BrokenBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                raise httpx.ReadTimeout("private-provider-body")
                yield b""  # Required to make this an async iterator.

        async def broken_response(request):
            return httpx.Response(503, request=request, headers={"Retry-After": "3"}, stream=BrokenBody())

        result, error, requests = self.run_http([broken_response, *support_responses()])
        self.assertIsNone(error)
        self.assertEqual(len(requests), 4)
        self.assertEqual(self.last_http.timestamps[1] - self.last_http.timestamps[0], 3.0)
        self.assertEqual(result["retry_wait_ms"], 3000)

    def test_nonretryable_http_status_stays_terminal_when_error_body_times_out(self):
        httpx = self.httpx

        class BrokenBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                raise httpx.ReadTimeout("private-provider-body")
                yield b""

        for status, diagnostic in ((400, "configuration"), (401, "authentication"),
                                   (403, "permissions"), (404, "--model"), (429, "quota")):
            async def broken_response(request):
                return httpx.Response(status, request=request, stream=BrokenBody())

            with self.subTest(status=status):
                result, error, requests = self.run_http([broken_response])
                self.assertIsNone(result)
                self.assertIn(diagnostic, str(error))
                self.assertNotIn("private-provider-body", str(error))
                self.assertEqual(len(requests), 1)
                self.assertEqual(self.last_http.waits, [])

    def test_constructor_failures_close_http_clients_before_the_event_loop_exits(self):
        from google import genai
        from google.adk.models import google_llm
        original_sync_init = self.httpx.Client.__init__
        original_async_init = self.httpx.AsyncClient.__init__
        for module, name in ((genai, "Client"), (google_llm, "Gemini")):
            clients = []

            def sync_init(client, *args, **kwargs):
                original_sync_init(client, *args, **kwargs)
                clients.append(client)

            def async_init(client, *args, **kwargs):
                original_async_init(client, *args, **kwargs)
                clients.append(client)

            with self.subTest(constructor=name), \
                    patch.dict(os.environ, {"GOOGLE_API_KEY": "unit-test-placeholder"}, clear=True), \
                    patch.object(self.httpx.Client, "__init__", sync_init), \
                    patch.object(self.httpx.AsyncClient, "__init__", async_init), \
                    patch.object(module, name, side_effect=RuntimeError("private-constructor-details")), \
                    patch.object(socket.socket, "connect", side_effect=AssertionError("Network disabled")) as connect, \
                    patch.object(socket, "getaddrinfo", side_effect=AssertionError("DNS disabled")) as dns:
                with self.assertRaises(program.AgentError) as caught:
                    program.handle(event())
            self.assertNotIn("private-constructor-details", str(caught.exception))
            self.assertEqual(len(clients), 2)
            self.assertTrue(all(client.is_closed for client in clients))
            connect.assert_not_called()
            dns.assert_not_called()

    def test_provider_http_errors_are_actionable_without_body_or_credential_leaks(self):
        cases = (
            (400, "INVALID_ARGUMENT", "request configuration"),
            (401, "UNAUTHENTICATED", "authentication"),
            (403, "PERMISSION_DENIED", "permissions"),
            (404, "NOT_FOUND", "--model"),
            (408, "REQUEST_TIMEOUT", "API request failed"),
            (422, "INVALID_ARGUMENT", "API request failed"),
            (429, "RESOURCE_EXHAUSTED", "quota"),
            (500, "INTERNAL", "internal error"),
            (502, "UNKNOWN", "gateway failed"),
            (503, "UNAVAILABLE", "unavailable"),
            (504, "DEADLINE_EXCEEDED", "timed out upstream"),
        )
        for code, status, hint in cases:
            with self.subTest(code=code):
                result, error, requests = self.run_http([
                    (code, {"error": {"code": code, "status": status,
                                      "message": "unit-test-placeholder raw-provider-body"}}),
                ] * (3 if code >= 500 else 1))
                self.assertIsNone(result)
                self.assertIsInstance(error, program.AgentError)
                if code not in (408, 422):
                    self.assertIn(f"HTTP {code}", str(error))
                self.assertIn(hint, str(error))
                self.assertNotIn("unit-test-placeholder", str(error))
                self.assertNotIn("raw-provider-body", str(error))
                self.assertTrue(error.__suppress_context__)
                self.assertEqual(len(requests), 3 if code >= 500 else 1)

    def test_real_sdk_transport_failures_remain_distinct_after_bounded_retries(self):
        for cause, hint in (
            (self.httpx.ReadTimeout("unit-test-placeholder raw-provider-body"), "HTTP request timed out"),
            (self.httpx.ConnectError("unit-test-placeholder raw-provider-body"), "connection failed"),
        ):
            with self.subTest(cause=type(cause).__name__):
                result, error, requests = self.run_http([cause] * 3)
                self.assertIsNone(result)
                self.assertIsInstance(error, program.AgentError)
                self.assertIn(hint, str(error))
                self.assertNotIn("unit-test-placeholder", str(error))
                self.assertNotIn("raw-provider-body", str(error))
                self.assertTrue(error.__suppress_context__)
                self.assertEqual(len(requests), 3)

    def test_unrecognized_api_codes_use_a_fixed_diagnostic(self):
        for code in (None, True, "429 raw-provider-body", [], {}, 999):
            with self.subTest(code_type=type(code).__name__):
                error = self.APIError(400, {"error": {"message": "unit-test-placeholder raw-provider-body"}})
                error.code = code
                self.assertEqual(program._execution_failure(error),
                                 "Gemini API request failed; no draft was accepted")

    def test_sdk_exception_fields_are_not_treated_as_provider_errors(self):
        error = RuntimeError("unit-test-placeholder raw-provider-body")
        error.code = 429
        self.assertEqual(program._execution_failure(error),
                         "Agent provider or SDK execution failed; no draft was accepted")


if __name__ == "__main__":
    unittest.main()
