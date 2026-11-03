"""Approve an export once, then release the worker while its provider finishes (MIT)."""

from enum import StrEnum
import http.client
import json
from urllib.parse import quote, urlsplit

from ledgence.worker.providers import (
    ProviderAdapter,
    ProviderLookup,
    ProviderReport,
    ProviderSelectionKind,
)
from ledgence.worker.workflow import ApprovalStatus, Workflow


class Entry(StrEnum):
    REQUEST = "request"
    DISPATCH = "dispatch"
    COMPLETE = "complete"


exports = ProviderAdapter(
    "example.export",
    version="1",
    lookup=ProviderLookup.BY_BINDING,
    completion_criterion="The provider committed a CSV artifact, or reported a definitive failure or cancellation",
)


def exchange(request, method):
    """One bounded HTTP attempt; http.client neither retries nor follows redirects."""
    binding, target = request.binding, request.target
    origin = urlsplit(target["endpoint_identity"])
    # This runnable example intentionally talks only to its local synthetic provider.
    if (
        origin.scheme != "http"
        or origin.hostname != "127.0.0.1"
        or not origin.port
        or origin.path
        or origin.query
        or origin.fragment
        or origin.username
    ):
        raise ValueError("the example provider must be an http://127.0.0.1:PORT origin")
    key = binding["deduplication"]["key"]
    path = "/exports" if method == "POST" else "/exports?key=" + quote(key, safe="")
    body = (
        json.dumps(request.arguments, separators=(",", ":")).encode()
        if method == "POST"
        else None
    )
    connection = http.client.HTTPConnection(origin.hostname, origin.port, timeout=2)
    try:
        connection.request(
            method,
            path,
            body=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Provider-Account": target["account"],
                "Idempotency-Key": key,
            },
        )
        response = connection.getresponse()
        payload = response.read(8193)
        if len(payload) > 8192:
            return None
        if response.status not in (200, 202):
            # Missing resources, 5xx and redirects do not establish business failure.
            return None
        return json.loads(payload)
    except (OSError, http.client.HTTPException, ValueError):
        return None
    finally:
        connection.close()


def classified(request, value):
    target = request.target
    if type(value) is not dict or type(value.get("id")) is not str:
        return ProviderReport.unknown(
            "The response did not establish the operation outcome", target=target
        )
    source = {"target": target, "provider_reference": value["id"]}
    if value.get("status") == "pending":
        return ProviderReport.pending(**source)
    if value.get("status") == "succeeded" and "result" in value:
        return ProviderReport.succeeded(value["result"], **source)
    if value.get("status") == "failed" and type(value.get("failure")) is dict:
        return ProviderReport.failed(
            value["failure"]["kind"], value["failure"]["message"], **source
        )
    if value.get("status") == "cancelled":
        return ProviderReport.cancelled(value["reason"], **source)
    if value.get("status") == "requires_action":
        return ProviderReport.requires_action(value["action_kind"], **source)
    return ProviderReport.unknown(
        "The response contained no recognized completion state", **source
    )


@exports.dispatch
def start_export(request):
    return classified(request, exchange(request, "POST"))


@exports.probe
def inspect_export(request):
    return classified(request, exchange(request, "GET"))


workflow = Workflow(Entry, providers=[exports])


@workflow.entrypoint(Entry.REQUEST, default=True)
async def request(event, ctx):
    data = event["data"]
    if (
        type(data.get("dataset")) is not str
        or not 1 <= len(data["dataset"].encode()) <= 128
    ):
        return ctx.fail("invalid_dataset", "dataset must contain 1–128 UTF-8 bytes")
    business_key = data["export_key"]
    binding = exports.binding(
        business={"namespace": "exports", "key": business_key},
        target={
            "provider": "example",
            "environment": "local",
            "account": "demo",
            "api_variant": "v1",
            "operation": "export",
            "endpoint_identity": data["provider_url"],
            "dimensions": {},
        },
        credential_reference="synthetic-local-account",
        queue=data.get("probe_queue", "provider-checks"),
        deduplication={
            "domain": {
                "provider": "example",
                "namespace": "account",
                "dimensions": {"account": "demo"},
            },
            "key": business_key,
            "horizon": {"kind": "minimum_duration", "duration_ms": 3_600_000},
        },
        reconciliation={
            "initial_delay_ms": 1000,
            "interval_ms": 1000,
            "maximum_interval_ms": 5000,
            "max_checks": 4,
            "max_elapsed_ms": 60_000,
            "check_timeout_ms": 1000,
        },
    )
    return ctx.request_provider_approval(
        "export",
        provider=exports,
        arguments={"dataset": data["dataset"]},
        binding=binding,
        resume=Entry.DISPATCH,
        state=None,
        timeout_ms=3_600_000,
    )


@workflow.entrypoint(Entry.DISPATCH)
async def dispatch(event, ctx):
    if ctx.approval.status is not ApprovalStatus.APPROVED:
        return ctx.complete(
            {"status": ctx.approval.status.value, "export_started": False}
        )
    ticket = await ctx.approved_provider_operation()
    if ticket.occurrence.workflow_id != ctx.workflow_id:
        return ctx.complete(
            {"status": "existing_operation", "occurrence": ticket.occurrence.to_dict()}
        )
    return ctx.wait_provider_operation(
        "export-result", ticket=ticket, resume=Entry.COMPLETE, state=None
    )


@workflow.entrypoint(Entry.COMPLETE)
async def complete(event, ctx):
    outcome = ctx.provider_operation_result
    if outcome.kind is ProviderSelectionKind.PROVIDER_SUCCEEDED:
        return ctx.complete({"status": "succeeded", "export": outcome.result})
    # Review-required and disputed are intentional application outcomes, not
    # claims that the external operation failed. Inspect Effects to decide next.
    return ctx.complete({"status": outcome.kind.value, "selection": outcome.selection})


handle = workflow.build()
