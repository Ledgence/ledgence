"""Provider identity, owned callbacks, typed recovery and real protocol gates (MIT)."""
import asyncio
import copy
from dataclasses import FrozenInstanceError
from enum import StrEnum
import hashlib
import json
from pathlib import Path
import unittest

import test_bootstrap
import test_workflow
from test_workflow_approvals import snapshot
from ledgence.worker import bootstrap
from ledgence.worker.providers import (
    ProviderAdapter, ProviderBindingRequest, ProviderLookup, ProviderOperationResult,
    ProviderOperationTicket, ProviderReport, ProviderSelectionKind, PROBE_SCHEMA,
    _invoke_probe, _validate_artifact_registry, _validate_binding,
)
from ledgence.worker.workflow import Workflow, WorkflowContext, WorkflowError, _workflow, workflow_context

CALLS = []
CALLBACK_ERROR = None
CALLBACK_GATE = None


async def dispatch(request):
    CALLS.append(("dispatch", request.arguments, request.binding))
    if CALLBACK_GATE is not None:
        await CALLBACK_GATE.wait()
    if CALLBACK_ERROR is not None:
        raise CALLBACK_ERROR
    # Every view is an independent copy; mutations cannot change the report target.
    request.arguments["amount"] = 999
    request.target["account"] = "other"
    return ProviderReport.pending(target=request.target, provider_reference="export-1")


async def probe(request):
    CALLS.append(("probe", request.arguments, request.provider_reference))
    with_no_context = False
    try:
        workflow_context()
    except WorkflowError:
        with_no_context = True
    if not with_no_context:
        raise AssertionError("probe was given a workflow context")
    return ProviderReport.succeeded(None, target=request.target, provider_reference="export-1")


def target():
    return {"provider": "example", "environment": "test", "account": "acme", "api_variant": "v1",
            "operation": "export", "endpoint_identity": "http://127.0.0.1:9999", "dimensions": {}}


class Entry(StrEnum):
    START = "start"
    DISPATCH = "dispatch"
    DONE = "done"


def adapter(lookup=ProviderLookup.BY_BINDING):
    result = ProviderAdapter("example.export", version="1", lookup=lookup,
                             completion_criterion="The export is ready to download")
    result.dispatch(dispatch)
    if lookup != ProviderLookup.UNAVAILABLE:
        result.probe(probe)
    return result


def registry(provider):
    flow = Workflow(Entry, providers=[provider])
    for entry in Entry:
        flow.entrypoint(entry, default=entry == Entry.START)(dispatch)
    return flow.build()


def binding(provider, resolved=True):
    value = provider.binding(business={"namespace": "exports", "key": "exp-1"}, target=target(),
                             credential_reference="local-fixture", queue="probes").to_dict()
    if resolved:
        value["program"] = {"program": {"id": "export", "version": "1"},
                            "digest": "sha256:" + "a" * 64, "size": 1234}
    return value


def approval(provider, **changes):
    value = binding(provider)
    return snapshot(action={"name": value["contract"]["dispatch_callable"], "version": "1",
                            "arguments": {"amount": 50}}, protection="provider_operation",
                    provider_binding=value, **changes)


def occurrence(**changes):
    return {"scope": {"tenant_id": "tenant", "namespace": "tests"}, "workflow_id": "workflow-1",
            "activation_id": "activation-1", "approval_key": "refund", **changes}


def ticket(**changes):
    return {"occurrence": occurrence(**changes), "binding_digest": "sha256:" + "b" * 64}


def selected_wake(selection=None, **changes):
    return {"kind": "provider_operation", "key": "result:0", "occurrence": occurrence(),
            "parking_activation_id": "activation-park", "parking_revision": 1,
            "resumed_activation_id": "activation-resumed", "resumed_revision": 2,
            "selected_at": 200, "selected_effect_revision": 12,
            "selection": selection or {"kind": "provider_succeeded", "report_ref": {
                "report_id": "report-1", "source_id": "source-1", "sequence": 4,
                "recorded_revision": 10, "recorded_at": 100, "knowledge": "succeeded"}, "result": None},
            **changes}


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflow.WorkflowTests.asyncSetUp
    asyncTearDown = test_workflow.WorkflowTests.asyncTearDown

    async def asyncSetUp(self):
        await test_workflow.WorkflowTests.asyncSetUp(self)
        global CALLBACK_ERROR, CALLBACK_GATE
        CALLS.clear()
        CALLBACK_ERROR = CALLBACK_GATE = None
        self.provider = adapter()
        self.handler = registry(self.provider)
        self.requests = []
        self.mode = "dispatch"
        self.mutate_reply = None

    async def rpc(self, operation, request):
        self.requests.append((operation, copy.deepcopy(request)))
        address = ticket(workflow_id="another-workflow") if self.mode == "existing_operation" else ticket()
        if operation == "provider_operation.claim.v1":
            result = {"kind": self.mode, "ticket": address}
            if self.mode == "dispatch":
                result["source"] = {"source_id": "source-1", "claim_id": request["claim_id"],
                                    "report_id": request["report_id"], "ticket": address,
                                    "accepted_at": 50, "first_possible_dispatch_at": 50,
                                    "reconciliation_deadline": 1000}
        elif operation == "provider_operation.report.v1":
            result = {"kind": "recorded", "ticket": request["ticket"], "source_id": request["source_id"],
                      "report_id": request["report_id"], "accepted_at": 55, "recorded_revision": 4,
                      "accepted_via": "live_dispatch", "already_accepted": False}
        else:
            raise AssertionError("C must never use a local journal or generic effect RPC")
        if self.mutate_reply is not None:
            self.mutate_reply(operation, result)
        return result

    def context(self, raw=None, *, enabled=True):
        context = WorkflowContext(raw or test_workflow.payload(revision=1,
            wake={"kind": "approval", "approval": approval(self.provider)}), self.rpc,
            effect_evidence_v2=True, native_effect_resolution=True, provider_operation=enabled)
        context._providers = self.handler._ledgence_providers
        self.contexts.append(context)
        return context

    async def test_one_dispatch_one_frozen_report_no_journal_and_repeated_await(self):
        context = self.context()
        first = context.approved_provider_operation()
        second = context.approved_provider_operation()
        result = await first
        self.assertEqual(result, await second)
        self.assertEqual([x[0] for x in CALLS], ["dispatch"])
        self.assertEqual(CALLS[0][1], {"amount": 50})
        self.assertEqual([x[0] for x in self.requests], ["provider_operation.claim.v1", "provider_operation.report.v1"])
        self.assertEqual(self.requests[1][1]["report"]["reported_target"], target())
        self.assertEqual(context._records, {})
        decision = context.wait_provider_operation("result", ticket=result, resume="done", state={})
        self.assertEqual(decision["wait"]["occurrence"], occurrence())
        self.assertEqual(context._validate_decision(decision), decision)
        self.assertEqual(len(self.requests), 2)

    async def test_checked_callback_survives_scheduling_and_claim_await_mutations(self):
        replacements = []

        async def replacement(request):
            replacements.append(request.arguments)
            return ProviderReport.pending(target=request.target)

        for timing in ("scheduled", "claim_reply"):
            with self.subTest(timing=timing):
                self.provider._dispatch = dispatch
                self.requests.clear()
                CALLS.clear()
                self.mutate_reply = None
                context = self.context()
                pending = context.approved_provider_operation()
                if timing == "scheduled":
                    self.provider._dispatch = replacement
                else:
                    def mutate(op, _reply):
                        if op == "provider_operation.claim.v1":
                            self.provider._dispatch = replacement
                    self.mutate_reply = mutate
                result = await pending
                self.assertEqual(result.to_dict(), ticket())
                self.assertEqual([call[0] for call in CALLS], ["dispatch"])
                self.assertEqual(CALLS[0][1], {"amount": 50})
                self.assertEqual(replacements, [])
                self.assertEqual([op for op, _ in self.requests], [
                    "provider_operation.claim.v1", "provider_operation.report.v1",
                ])
        self.provider._dispatch = dispatch

    async def test_existing_receipt_never_dispatches_or_reports(self):
        self.mode = "existing"
        result = await self.context().approved_provider_operation()
        self.assertEqual(result.to_dict(), ticket())
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(CALLS, [])

    async def test_cross_workflow_existing_ticket_has_no_ownership_transfer(self):
        self.mode = "existing_operation"
        context = self.context()
        result = await context.approved_provider_operation()
        self.assertEqual(result.occurrence.workflow_id, "another-workflow")
        with self.assertRaisesRegex(WorkflowError, "another workflow"):
            context.wait_provider_operation("result", ticket=result, resume="done", state={})
        self.assertEqual(CALLS, [])

    async def test_callback_failure_returns_ticket_for_durable_recovery(self):
        global CALLBACK_ERROR
        CALLBACK_ERROR = TimeoutError("provider response lost")
        context = self.context()
        result = await context.approved_provider_operation()
        self.assertEqual(result.to_dict(), ticket())
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(context._records, {})
        self.assertEqual(len(CALLS), 1)

    async def test_cancelled_observer_cannot_abandon_the_callback(self):
        global CALLBACK_GATE
        CALLBACK_GATE = asyncio.Event()
        context = self.context()
        async def observe():
            return await context.approved_provider_operation()
        observer = asyncio.create_task(observe())
        while not CALLS:
            await asyncio.sleep(0)
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await observer
        drain = asyncio.create_task(context._drain())
        await asyncio.sleep(0)
        self.assertFalse(drain.done())
        CALLBACK_GATE.set()
        await drain
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(len(CALLS), 1)

    async def test_mismatched_claim_is_fatal_before_callback(self):
        self.mutate_reply = lambda op, value: value["source"].update(report_id="other")
        context = self.context()
        with self.assertRaisesRegex(WorkflowError, "differs"):
            await context.approved_provider_operation()
        self.assertEqual(CALLS, [])
        with self.assertRaisesRegex(WorkflowError, "acknowledgement failed"):
            context.complete(None)

    async def test_changed_report_receipt_is_fatal_after_single_dispatch(self):
        def mutate(op, value):
            if op.endswith("report.v1"):
                value["source_id"] = "different"
        self.mutate_reply = mutate
        context = self.context()
        with self.assertRaisesRegex(WorkflowError, "acknowledgement differs"):
            await context.approved_provider_operation()
        self.assertEqual(len(CALLS), 1)
        with self.assertRaisesRegex(WorkflowError, "acknowledgement failed"):
            context.wait_provider_operation("result", ticket=ProviderOperationTicket.from_dict(ticket()),
                                            resume="done", state={})

    async def test_cross_ingress_receipt_preserves_evidence_only(self):
        def mutate(op, value):
            if op.endswith("report.v1"):
                value.update(kind="recorded_elsewhere", accepted_via="evidence_only", already_accepted=True)
        self.mutate_reply = mutate
        self.assertEqual((await self.context().approved_provider_operation()).to_dict(), ticket())
        self.assertEqual(len(CALLS), 1)

    async def test_a_and_generic_helpers_cannot_execute_c_approval(self):
        context = self.context()
        for invoke in (lambda: context.approved_local(dispatch, version="1"),
                       lambda: context.approved_effect(dispatch, lookup=probe, version="1")):
            with self.assertRaisesRegex(WorkflowError, "approved_provider_operation"):
                invoke()
        self.assertEqual(self.requests, [])
        self.assertEqual(CALLS, [])

    async def test_contract_drift_refuses_before_claim(self):
        value = approval(self.provider)
        value["provider_binding"]["contract"]["digest"] = "sha256:" + "d" * 64
        context = self.context(test_workflow.payload(revision=1, wake={"kind": "approval", "approval": value}))
        with self.assertRaisesRegex(WorkflowError, "registered adapter"):
            await context.approved_provider_operation()
        self.assertEqual(self.requests, [])

    async def test_request_frozen_binding_and_action_omit_resolved_descriptor(self):
        context = self.context(test_workflow.payload())
        arguments = {"amount": 50}
        request = ProviderBindingRequest.from_dict(binding(self.provider, resolved=False))
        decision = context.request_provider_approval("export", provider=self.provider, arguments=arguments,
            binding=request, resume="dispatch", state={}, timeout_ms=60_000)
        arguments["amount"] = 999
        self.assertEqual(decision["wait"]["action"]["arguments"], {"amount": 50})
        self.assertNotIn("program", decision["wait"]["provider_binding"])
        self.assertEqual(context._validate_decision(decision), decision)
        self.assertEqual(self.requests, [])

    async def test_provider_wake_is_bound_to_activation_and_defaults_followup_revision(self):
        wake = selected_wake()
        context = self.context(test_workflow.payload(activation_id="activation-resumed", revision=2, wake=wake))
        result = context.provider_operation_result
        self.assertEqual(result.kind, ProviderSelectionKind.PROVIDER_SUCCEEDED)
        self.assertIsNone(result.result)
        decision = context.wait_provider_operation("next", resume="done", state={})
        self.assertEqual(decision["wait"]["after_effect_revision"], 12)
        self.assertEqual(decision["wait"]["occurrence"], occurrence())
        wake["selection"]["result"] = "changed"
        self.assertIsNone(result.result)
        with self.assertRaises(FrozenInstanceError):
            result._encoded = b"{}"
        for changes in ({"activation_id": "other"}, {"revision": 3}, {"workflow_id": "other"}):
            raw = test_workflow.payload(activation_id="activation-resumed", revision=2, wake=selected_wake())
            raw.update(changes)
            with self.assertRaisesRegex(WorkflowError, "another workflow activation"):
                self.context(raw)

    async def test_provider_feature_required_for_wake_and_raw_wait(self):
        with self.assertRaisesRegex(WorkflowError, "unavailable"):
            self.context(enabled=False)
        old = self.context(test_workflow.payload(), enabled=False)
        current = self.context(test_workflow.payload())
        decision = current.wait_provider_operation("result", ticket=ProviderOperationTicket.from_dict(ticket()),
                                                   resume="done", state={})
        with self.assertRaisesRegex(WorkflowError, "unavailable"):
            old._validate_decision(decision)

    async def test_probe_calls_registered_callback_without_workflow_context(self):
        payload = self.probe_payload()
        result = await _invoke_probe(self.handler, self.probe_event(), "attempt-1", payload)
        self.assertEqual(result["kind"], "report")
        self.assertEqual(result["report"]["knowledge"], {"kind": "succeeded", "result": None})
        self.assertEqual(CALLS, [("probe", {"amount": 50}, "export-1")])

    def probe_payload(self):
        return {"ticket": ticket(), "binding": binding(self.provider), "action": approval(self.provider)["action"],
                "probe_id": "probe-1", "generation": 1, "task_id": "probe-task", "attempt_id": "attempt-1",
                "source_id": "source-2", "report_id": "report-2", "provider_reference": "export-1",
                "check_timeout_ms": 10_000}

    def probe_event(self):
        return {"ldgtaskid": "probe-task", "ldgtenantid": "tenant", "ldgnamespace": "tests", "ldgworkflowid": "workflow-1"}

    async def test_probe_rejects_malformed_context_and_contract_without_callbacks(self):
        for key, value in (("task_id", "other"), ("attempt_id", "other"), ("generation", True),
                           ("check_timeout_ms", 30_001), ("provider_reference", "")):
            payload = self.probe_payload()
            payload[key] = value
            result = await _invoke_probe(self.handler, self.probe_event(), "attempt-1", payload)
            self.assertEqual(result, {"v": 1, "kind": "rejected", "failure": "context_invalid"})
        payload = self.probe_payload()
        payload["binding"]["contract"]["version"] = "other"
        result = await _invoke_probe(self.handler, self.probe_event(), "attempt-1", payload)
        self.assertEqual(result["failure"], "contract_mismatch")
        self.assertEqual(CALLS, [])


    async def test_probe_rejects_foreign_workflow_or_controller_marker(self):
        for changed in ({"ldgworkflowid": "other"}, {"ldgactivationid": "controller"}, {"ldgworkflowid": None}):
            event = dict(self.probe_event(), **changed)
            result = await _invoke_probe(self.handler, event, "attempt-1", self.probe_payload())
            self.assertEqual(result, {"v": 1, "kind": "rejected", "failure": "context_invalid"})
        self.assertEqual(CALLS, [])
        nested = dict(self.probe_event(), ldgparentworkflowid="parent", ldgrootworkflowid="root")
        result = await _invoke_probe(self.handler, nested, "attempt-1", self.probe_payload())
        self.assertEqual(result["kind"], "report")


class ProviderValueTests(unittest.TestCase):
    def test_binding_policy_boundaries_and_required_nulls(self):
        provider = adapter()
        registry(provider)
        valid = binding(provider)
        _validate_binding(valid, resolved=True)
        mutations = [lambda v: v.pop("deduplication"), lambda v: v.update(v=True),
                     lambda v: v["contract"].update(reconcile_callable=None),
                     lambda v: v["reconciliation"].update(max_checks=257),
                     lambda v: v["reconciliation"].update(check_timeout_ms=999),
                     lambda v: v["reconciliation"].update(initial_delay_ms=3_600_000),
                     lambda v: v["reconciliation"].update(maximum_interval_ms=1000),
                     lambda v: v["program"].update(size=0)]
        for mutate in mutations:
            value = copy.deepcopy(valid)
            mutate(value)
            with self.assertRaises(WorkflowError):
                _validate_binding(value, resolved=True)

    def test_no_lookup_requires_only_dispatch(self):
        provider = adapter(ProviderLookup.UNAVAILABLE)
        registry(provider)
        value = binding(provider)
        self.assertIsNone(value["contract"]["reconcile_callable"])
        _validate_binding(value, resolved=True)
        with self.assertRaisesRegex(WorkflowError, "frozen"):
            provider.probe(probe)

    def test_registry_rejects_duplicates_missing_callbacks_and_unstable_functions(self):
        provider = adapter()
        with self.assertRaisesRegex(WorkflowError, "distinct"):
            flow = Workflow(Entry, providers=[provider, provider])
            for entry in Entry:
                flow.entrypoint(entry, default=entry == Entry.START)(dispatch)
            flow.build()
        missing = ProviderAdapter("missing", version="1", lookup=ProviderLookup.BY_BINDING,
                                  completion_criterion="Ready")
        missing.dispatch(dispatch)
        with self.assertRaisesRegex(WorkflowError, "missing"):
            registry(missing)
        unstable = ProviderAdapter("unstable", version="1", lookup=ProviderLookup.UNAVAILABLE,
                                   completion_criterion="Ready")
        with self.assertRaisesRegex(WorkflowError, "module-level"):
            unstable.dispatch(lambda request: None)
        with self.assertRaisesRegex(WorkflowError, "artifact"):
            _validate_artifact_registry(registry(adapter()), Path('/nonexistent'))

    def test_reports_preserve_typed_null_and_require_target_and_knowledge(self):
        report = ProviderReport.succeeded(None, target=target())
        self.assertEqual(report.to_dict()["knowledge"], {"kind": "succeeded", "result": None})
        report.to_dict()["reported_target"]["account"] = "changed"
        self.assertEqual(report.to_dict()["reported_target"], target())
        for knowledge in ({"kind": "succeeded"}, {"kind": "disputed"}, {"kind": "pending", "result": 1},
                           {"kind": "unknown", "reason": ""}, {"kind": "succeeded", "result": "x" * 131072}):
            value = report.to_dict()
            value["knowledge"] = knowledge
            with self.assertRaises(WorkflowError):
                ProviderReport.from_dict(value)

    def test_all_selected_outcomes_and_inconsistent_reference_bounds(self):
        reference = {"resolution_id": "r1", "sequence": 3, "basis_revision": 4, "accepted_revision": 5,
                     "accepted_at": 100, "assessment_kind": "close_unknown", "supersedes_resolution_id": None,
                     "acceptance_event_id": "event-1"}
        selections = [
            {"kind": "operator_assessment", "reference": reference, "outcome": {"kind": "closed_unknown"}},
            {"kind": "disputed", "first_conflicting_entry_sequence": 3},
            {"kind": "needs_review", "reason": "lookup_unavailable", "checks_used": 0, "deadline_at": 1000},
        ]
        for selection in selections:
            result = ProviderOperationResult.from_dict(selected_wake(selection))
            self.assertEqual(result.selection, selection)
            with self.assertRaisesRegex(WorkflowError, "no successful result"):
                _ = result.result
        for changes in ({"selected_effect_revision": True}, {"parking_revision": 1 << 64},
                        {"resumed_activation_id": "activation-park"}, {"resumed_revision": 3}):
            with self.assertRaises(WorkflowError):
                ProviderOperationResult.from_dict(selected_wake(**changes))
        wake = selected_wake()
        wake["selection"]["report_ref"]["recorded_revision"] = 13
        with self.assertRaisesRegex(WorkflowError, "contradicts"):
            ProviderOperationResult.from_dict(wake)


    def test_disputed_assessment_requires_new_evidence_after_a_definite_assessment(self):
        reference = {"resolution_id": "r1", "sequence": 3, "basis_revision": 4,
                     "accepted_revision": 5, "accepted_at": 100,
                     "assessment_kind": "record_success", "supersedes_resolution_id": None,
                     "acceptance_event_id": "event-1"}
        selection = {"kind": "operator_assessment", "reference": reference,
                     "outcome": {"kind": "disputed", "first_conflicting_entry_sequence": 6}}
        self.assertEqual(ProviderOperationResult.from_dict(selected_wake(selection)).selection, selection)
        for sequence in (1, 3, 12, 13):
            changed = copy.deepcopy(selection)
            changed["outcome"]["first_conflicting_entry_sequence"] = sequence
            with self.assertRaises(WorkflowError):
                ProviderOperationResult.from_dict(selected_wake(changed))
        selection["reference"]["assessment_kind"] = "close_unknown"
        with self.assertRaises(WorkflowError):
            ProviderOperationResult.from_dict(selected_wake(selection))


PROTOCOL_SOURCE = '''from enum import StrEnum
from ledgence.worker.workflow import Workflow, workflow_context, WorkflowError
from ledgence.worker.providers import ProviderAdapter, ProviderLookup, ProviderReport
provider = ProviderAdapter('example.export', version='1', lookup=ProviderLookup.BY_BINDING,
                           completion_criterion='The export is ready to download')
@provider.dispatch
async def dispatch(request):
    raise AssertionError('DISPATCH_ENTERED')
@provider.probe
async def probe(request):
    try:
        workflow_context()
    except WorkflowError:
        pass
    else:
        raise AssertionError('WORKFLOW_CONTEXT_LEAKED')
    return ProviderReport.succeeded(None, target=request.target)
class Entry(StrEnum):
    START = 'start'
workflow = Workflow(Entry, providers=[provider])
@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    raise AssertionError('WORKFLOW_ENTERED')
handle = workflow.build()
'''


class ProviderProtocolTests(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation

    def message(self):
        provider = adapter()
        registry(provider)
        bound = binding(provider)
        contract = bound["contract"]
        contract.update(dispatch_callable="program:dispatch", reconcile_callable="program:probe")
        contract.pop("digest")
        encoded = json.dumps(contract, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        contract["digest"] = "sha256:" + hashlib.sha256(encoded).hexdigest()
        message = self.invocation()
        message["event"].update(ldgtaskid="probe-task", ldgtenantid="tenant", ldgnamespace="tests")
        message["event"].pop("ldgactivationid", None)
        message["extension"] = {"schema": PROBE_SCHEMA, "payload": {
            "ticket": ticket(), "binding": bound,
            "action": {"name": "program:dispatch", "version": "1", "arguments": {"amount": 50}},
            "probe_id": "probe-1", "generation": 1, "task_id": "probe-task",
            "attempt_id": message["attempt_id"], "source_id": "source-2", "report_id": "report-2",
            "provider_reference": None, "check_timeout_ms": 10_000}}
        message["runtime_capabilities"] = {"approved_effect_evidence_v2": True}
        message["runtime_features"] = {"v": 1, "supported": ["native_effect_resolution.v1", "provider_operation.v1"]}
        return message

    def test_real_isolated_bootstrap_dispatches_only_registered_probe(self):
        message = self.message()
        result, frames = self.launch(PROTOCOL_SOURCE, [message], protocol=3,
                                     input_limit=128 * 1024, output_limit=256 * 1024)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(frames[-1]["status"], "success", result.stderr)
        self.assertEqual(frames[-1]["output"]["report"]["knowledge"], {"kind": "succeeded", "result": None})
        self.assertNotIn(b"WORKFLOW_ENTERED", result.stderr)
        self.assertNotIn(b"DISPATCH_ENTERED", result.stderr)

    def test_missing_feature_fails_before_application_and_capability_never_leaks(self):
        first = self.message()
        second = copy.deepcopy(first)
        second.pop("runtime_features")
        result, frames = self.launch(PROTOCOL_SOURCE, [first, second], protocol=3,
                                     input_limit=128 * 1024, output_limit=256 * 1024)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(frames[1]["status"], "success")
        self.assertEqual(frames[2]["status"], "runtime_error")
        self.assertNotIn(b"WORKFLOW_ENTERED", result.stderr)
        self.assertNotIn(b"DISPATCH_ENTERED", result.stderr)
        bad = self.message()
        bad["runtime_features"]["supported"] = ["provider_operation.v1"]
        result, frames = self.launch(PROTOCOL_SOURCE, [bad], protocol=3, input_limit=128 * 1024)
        self.assertEqual(result.returncode, 70)
        self.assertEqual([frame["type"] for frame in frames], ["ready"])

    def test_callback_exception_is_distinct_from_invalid_return(self):
        for line, expected in (("raise WorkflowError('application failure')", "invocation_failed"),
                               ("return {'kind': 'pending'}", "result_invalid")):
            source = PROTOCOL_SOURCE.replace("return ProviderReport.succeeded(None, target=request.target)", line)
            result, frames = self.launch(source, [self.message()], protocol=3,
                                         input_limit=128 * 1024, output_limit=256 * 1024)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(frames[-1]["output"], {"v": 1, "kind": "rejected", "failure": expected})

    def test_historical_helpers_reject_probe_extension_before_user_handler(self):
        from test_effect_compatibility import HistoricalEffectCompatibilityTests, PINS
        historical = HistoricalEffectCompatibilityTests()
        for name in PINS:
            with historical.helper(name):
                result, frames = self.launch("def handle(event): raise AssertionError('HANDLER_ENTERED')",
                    [self.message()], protocol=3, input_limit=128 * 1024, output_limit=256 * 1024)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(frames[-1]["status"], "runtime_error")
            self.assertNotIn(b"HANDLER_ENTERED", result.stderr)
            self.assertFalse(any(frame["type"] == "runtime_request" for frame in frames))
