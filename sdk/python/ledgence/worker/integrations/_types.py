"""Framework-independent, bounded integration results and errors (MIT)."""
from dataclasses import dataclass
from enum import StrEnum


class AgentCapability(StrEnum):
    MODEL_REPLAY = "model_replay"
    TOOL_REPLAY = "tool_replay"
    DISCOVERY_REPLAY = "discovery_replay"
    RESULT_REPLAY = "result_replay"
    REPORTED_USAGE = "reported_usage"
    STOP_REQUEST = "stop_request"
    SESSION_RESUME = "session_resume"


@dataclass(frozen=True, slots=True)
class AgentLimits:
    max_model_turns: int | None = None
    max_tool_calls: int | None = None
    max_discoveries: int | None = None
    max_parallel_calls: int = 4
    timeout_ms: int = 60_000


@dataclass(frozen=True, slots=True)
class AgentRunSpec:
    key: str
    version: str
    configuration: dict
    branch: str = "main"
    limits: AgentLimits = AgentLimits()


@dataclass(frozen=True, slots=True)
class AdapterProfile:
    name: str
    version: str
    capabilities: frozenset[AgentCapability]


class AgentStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ABORTED = "aborted"


class UsageSource(StrEnum):
    MODEL_RESPONSE = "model_response"
    ASSISTANT_MESSAGE = "assistant_message"
    RUN_TOTAL = "run_total"


class UsageOrigin(StrEnum):
    ACCEPTED_REPLAY = "accepted_replay"
    CURRENT_DISPATCH = "current_dispatch"


class UsageCoverage(StrEnum):
    REPORTED = "reported"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class UsageObservation:
    operation_key: str
    source_kind: UsageSource
    source_id: str
    origin: UsageOrigin
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    reported_cost: str | None = None
    currency: str | None = None
    coverage: UsageCoverage = UsageCoverage.UNKNOWN


@dataclass(frozen=True, slots=True)
class LifecycleEvidence:
    origin: UsageOrigin = UsageOrigin.CURRENT_DISPATCH
    stop_requested: bool = False
    stop_acknowledged: bool | None = None
    terminal_status: AgentStatus | None = None
    transport_closed: bool | None = None
    local_process_exit_observed: bool | None = None
    session_id: str | None = None
    checkpoint_verified: bool = False


@dataclass(frozen=True, slots=True)
class AgentResult:
    status: AgentStatus
    output: object
    native: object
    profile: AdapterProfile
    usage: tuple[UsageObservation, ...]
    lifecycle: LifecycleEvidence


class AgentIntegrationError(Exception):
    """Bounded diagnostic; the local cause may contain SDK-specific details."""
    def __init__(self, code, *, operation_key=None, external_work_may_have_occurred=False,
                 lifecycle=None, reserved_bytes=None):
        # Never incorporate provider messages, prompts or argument values.
        if (type(code) is not str or not code or len(code) > 80
                or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_" for c in code)):
            raise ValueError("invalid integration error code")
        self.code = code
        self.operation_key = operation_key
        self.external_work_may_have_occurred = external_work_may_have_occurred
        self.lifecycle = lifecycle
        self.reserved_bytes = reserved_bytes
        super().__init__(code)


class AgentDependencyError(AgentIntegrationError): pass
class AgentCapabilityError(AgentIntegrationError): pass
class AgentRunConflict(AgentIntegrationError): pass
class AgentProfileError(AgentIntegrationError): pass
class AgentBindingError(AgentIntegrationError): pass
class AgentLimitError(AgentIntegrationError): pass
class AgentExecutionError(AgentIntegrationError): pass
