"""Approve one export, wait for shared capacity, then recover its business result (MIT)."""

from enum import StrEnum
import http.client
import json
from urllib.parse import quote, urlsplit

from ledgence.worker.provider_admission import ProviderAdmissionOutcomeKind
from ledgence.worker.providers import (
    ProviderAdapter,
    ProviderAdmissionRequirement,
    ProviderDispatchOutcome,
    ProviderLookup,
    ProviderReport,
    ProviderSelectionKind,
)
from ledgence.worker.workflow import ApprovalStatus, Workflow


class Entry(StrEnum):
    REQUEST = "request"
    RESERVE = "reserve"
    DISPATCH = "dispatch"
    RESULT = "result"


exports = ProviderAdapter(
    "example.admitted-export",
    version="1",
    lookup=ProviderLookup.BY_BINDING,
    completion_criterion="The provider committed a CSV artifact or reported definitive failure or cancellation",
)


def exchange(request, method):
    """One local HTTP attempt, returning business evidence and call-completion evidence."""
    target = request.target
    origin = urlsplit(target["endpoint_identity"])
    if (origin.scheme != "http" or origin.hostname != "127.0.0.1" or not origin.port
            or origin.path or origin.query or origin.fragment or origin.username):
        raise ValueError("use the synthetic provider's http://127.0.0.1:PORT origin")
    key = request.binding["deduplication"]["key"]
    path = "/exports" if method == "POST" else "/exports?key=" + quote(key, safe="")
    body = json.dumps(request.arguments).encode() if method == "POST" else None
    connection = http.client.HTTPConnection(origin.hostname, origin.port, timeout=2)
    try:
        connection.request(method, path, body, {
            "Accept": "application/json", "Content-Type": "application/json",
            "X-Provider-Account": target["account"], "Idempotency-Key": key,
        })
        response = connection.getresponse()
        payload = response.read(8193)
        if len(payload) > 8192:
            return ProviderReport.unknown("Response exceeds the example's bound", target=target), False
        # A complete bounded HTTP response establishes call completion, even
        # when it does not establish the export's business outcome.
        if response.status not in (200, 202):
            return ProviderReport.unknown("HTTP response did not establish export state", target=target), True
        try:
            value = json.loads(payload)
        except (ValueError, UnicodeError):
            return ProviderReport.unknown("Malformed completed response", target=target), True
        return classify(target, value), True
    except (OSError, http.client.HTTPException, ValueError):
        # A timeout or missing response can follow a committed provider write.
        return ProviderReport.unknown("Original call completion is uncertain", target=target), False
    finally:
        connection.close()


def classify(target, value):
    if type(value) is not dict or type(value.get("id")) is not str:
        return ProviderReport.unknown("Unrecognized export response", target=target)
    source = {"target": target, "provider_reference": value["id"]}
    if value.get("status") == "pending":
        return ProviderReport.pending(**source)
    if value.get("status") == "succeeded" and "result" in value:
        return ProviderReport.succeeded(value["result"], **source)
    failure = value.get("failure")
    if (value.get("status") == "failed" and type(failure) is dict
            and all(type(failure.get(key)) is str and failure[key] for key in ("kind", "message"))):
        return ProviderReport.failed(failure["kind"], failure["message"], **source)
    if value.get("status") == "cancelled" and type(value.get("reason")) is str and value["reason"]:
        return ProviderReport.cancelled(value["reason"], **source)
    return ProviderReport.unknown("Export needs inspection", **source)


@exports.dispatch
def start_export(request):
    report, call_completed = exchange(request, "POST")
    return (ProviderDispatchOutcome.completed(report) if call_completed
            else ProviderDispatchOutcome.unknown(report))


@exports.probe
def inspect_export(request):
    # Read-only probes report business state. They cannot release uncertain
    # occupancy from the original dispatch and are outside this resource limit.
    report, _ = exchange(request, "GET")
    return report


workflow = Workflow(Entry, providers=[exports])


@workflow.entrypoint(Entry.REQUEST, default=True)
async def request(event, ctx):
    data = event["data"]
    if type(data.get("dataset")) is not str or not 1 <= len(data["dataset"].encode()) <= 128:
        return ctx.fail("invalid_dataset", "dataset must contain 1–128 UTF-8 bytes")
    key = data["export_key"]
    binding = exports.binding(
        business={"namespace": "exports", "key": key},
        target={"provider": "example", "environment": "local", "account": "demo",
                "api_variant": "v1", "operation": "export",
                "endpoint_identity": data["provider_url"], "dimensions": {}},
        credential_reference="synthetic-local-account",
        queue=data.get("probe_queue", "provider-checks"),
        admission=ProviderAdmissionRequirement(resource="export-dispatch"),
        deduplication={
            "domain": {"provider": "example", "namespace": "account", "dimensions": {"account": "demo"}},
            "key": key, "horizon": {"kind": "minimum_duration", "duration_ms": 3_600_000},
        },
        reconciliation={"initial_delay_ms": 1000, "interval_ms": 1000,
                        "maximum_interval_ms": 5000, "max_checks": 4,
                        "max_elapsed_ms": 60_000, "check_timeout_ms": 1000},
    )
    # The resource, adapter, target and arguments are reviewed together.
    return ctx.request_provider_approval(
        "export", provider=exports, arguments={"dataset": data["dataset"]},
        binding=binding, resume=Entry.RESERVE, state=None, timeout_ms=3_600_000,
    )


@workflow.entrypoint(Entry.RESERVE)
async def reserve(event, ctx):
    if ctx.approval.status is not ApprovalStatus.APPROVED:
        return ctx.complete({"status": ctx.approval.status.value, "export_started": False})
    return ctx.request_provider_admission(
        "export-capacity", resume=Entry.DISPATCH, state=None, timeout_ms=60_000,
    )


@workflow.entrypoint(Entry.DISPATCH)
async def dispatch(event, ctx):
    admitted = await ctx.admitted_provider_operation()
    if admitted.kind in (ProviderAdmissionOutcomeKind.WAIT_REQUIRED, ProviderAdmissionOutcomeKind.NEEDS_REVIEW):
        return ctx.wait_admission(resume=Entry.DISPATCH, state=None, timeout_ms=60_000)
    if admitted.kind is ProviderAdmissionOutcomeKind.CLOSED:
        return ctx.fail("admission_closed", "Dispatch was closed without a provider ticket")
    ticket = admitted.ticket
    if ticket.occurrence.workflow_id != ctx.workflow_id:
        return ctx.complete({"status": "existing_operation", "occurrence": ticket.occurrence.to_dict()})
    return ctx.wait_provider_operation("export-result", ticket=ticket, resume=Entry.RESULT, state=None)


@workflow.entrypoint(Entry.RESULT)
async def result(event, ctx):
    outcome = ctx.provider_operation_result
    if outcome.kind is ProviderSelectionKind.PROVIDER_SUCCEEDED:
        return ctx.complete({"status": "succeeded", "export": outcome.result})
    return ctx.complete({"status": outcome.kind.value, "selection": outcome.selection})


handle = workflow.build()
