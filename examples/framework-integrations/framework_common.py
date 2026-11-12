"""Shared read data and controller-owned approval/admission bridge (MIT)."""
from enum import StrEnum
import re

from ledgence.worker.provider_admission import ProviderAdmissionOutcomeKind
from ledgence.worker.providers import (
    ProviderAdapter, ProviderAdmissionRequirement, ProviderDispatchOutcome,
    ProviderLookup, ProviderReport, ProviderSelectionKind,
)
from ledgence.worker.workflow import ApprovalStatus, Workflow


class Entry(StrEnum):
    PROPOSE = "propose"
    RESERVE = "reserve"
    DISPATCH = "dispatch"
    RESULT = "result"


def read_policy(topic: str) -> dict:
    """Static read fixture: no credentials, network, mutation or workflow control."""
    policies = {
        "maintenance": {"title": "Planned maintenance", "body": "Read-only access remains available during maintenance."},
        "delivery": {"title": "Delivery update", "body": "Track the delivery before requesting a replacement."},
    }
    return dict(policies[topic])


def proposal(value):
    if type(value) is not dict or set(value) != {"title", "body"}:
        raise ValueError("Expected a notice title and body")
    for field, limit in (("title", 80), ("body", 512)):
        if type(value[field]) is not str or not 1 <= len(value[field].encode("utf-8")) <= limit:
            raise ValueError("Notice text exceeds its bound")
    return dict(value)


notices = ProviderAdapter("example.framework-notice", version="1", lookup=ProviderLookup.UNAVAILABLE,
                         completion_criterion="The simulated notice write returned its complete receipt")


@notices.dispatch
def simulate_notice_write(request):
    """A simulated business write, called only after controller authorization."""
    return ProviderDispatchOutcome.completed(ProviderReport.succeeded(
        {"simulated": True, "notice": proposal(request.arguments)}, target=request.target))


def build_controller(propose):
    workflow = Workflow(Entry, providers=[notices])

    @workflow.entrypoint(Entry.PROPOSE, default=True)
    async def begin(event, ctx):
        data = event.get("data")
        if (type(data) is not dict or data.get("topic") not in ("maintenance", "delivery")
                or type(data.get("notice_key")) is not str or not 1 <= len(data["notice_key"].encode()) <= 128
                or type(data.get("queue")) is not str or re.fullmatch(r"[a-z][a-z0-9-]{0,63}", data["queue"]) is None):
            return ctx.fail("invalid_input", "Supply topic, notice_key, and queue")
        # SDK callbacks finish before the controller creates any approved intent.
        arguments = proposal(await propose(ctx, data["topic"]))
        binding = notices.binding(
            business={"namespace": "notices", "key": data["notice_key"]},
            target={"provider": "example", "environment": "offline", "account": "demo",
                    "api_variant": "v1", "operation": "publish-notice",
                    "endpoint_identity": "offline-notice-board", "dimensions": {}},
            credential_reference="no-credentials", queue=data["queue"],
            admission=ProviderAdmissionRequirement(resource="notice-write"),
        )
        return ctx.request_provider_approval("publish-notice", provider=notices, arguments=arguments,
            binding=binding, resume=Entry.RESERVE, state=None, timeout_ms=3_600_000)

    @workflow.entrypoint(Entry.RESERVE)
    def reserve(event, ctx):
        approval = ctx.approval
        if approval is None or approval.key != "publish-notice":
            return ctx.fail("missing_approval", "Expected the notice approval")
        if approval.status is not ApprovalStatus.APPROVED:
            return ctx.complete({"status": approval.status.value, "dispatched": False})
        # The original approved arguments and resource are read from the wake.
        return ctx.request_provider_admission("notice-capacity", resume=Entry.DISPATCH,
                                              state=None, timeout_ms=60_000)

    @workflow.entrypoint(Entry.DISPATCH)
    async def dispatch(event, ctx):
        admitted = await ctx.admitted_provider_operation()
        if admitted.kind in (ProviderAdmissionOutcomeKind.WAIT_REQUIRED, ProviderAdmissionOutcomeKind.NEEDS_REVIEW):
            return ctx.wait_admission(resume=Entry.DISPATCH, state=None, timeout_ms=60_000)
        if admitted.kind is ProviderAdmissionOutcomeKind.CLOSED:
            return ctx.fail("admission_closed", "No provider dispatch grant is available")
        ticket = admitted.ticket
        if ticket.occurrence.workflow_id != ctx.workflow_id:
            return ctx.complete({"status": "existing_operation", "occurrence": ticket.occurrence.to_dict()})
        return ctx.wait_provider_operation("notice-result", ticket=ticket, resume=Entry.RESULT, state=None)

    @workflow.entrypoint(Entry.RESULT)
    def result(event, ctx):
        outcome = ctx.provider_operation_result
        if outcome.kind is ProviderSelectionKind.PROVIDER_SUCCEEDED:
            return ctx.complete({"status": "succeeded", "receipt": outcome.result})
        return ctx.complete({"status": outcome.kind.value, "selection": outcome.selection})

    return workflow.build()
