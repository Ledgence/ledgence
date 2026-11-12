"""Optional framework adapters; importing this module loads no vendor SDK (MIT).

Select a framework module explicitly. Install its qualified SDK in the program's
own dependencies; Ledgence's worker helper has no framework dependencies.
"""
from ._types import (
    AdapterProfile, AgentCapability, AgentLimits, AgentRunSpec, AgentStatus,
    AgentResult, UsageSource, UsageOrigin, UsageCoverage, UsageObservation,
    LifecycleEvidence, AgentIntegrationError, AgentDependencyError,
    AgentCapabilityError, AgentRunConflict, AgentProfileError, AgentBindingError,
    AgentLimitError, AgentExecutionError,
)

__all__ = [
    "AdapterProfile", "AgentCapability", "AgentLimits", "AgentRunSpec", "AgentStatus",
    "AgentResult", "UsageSource", "UsageOrigin", "UsageCoverage", "UsageObservation",
    "LifecycleEvidence", "AgentIntegrationError", "AgentDependencyError",
    "AgentCapabilityError", "AgentRunConflict", "AgentProfileError", "AgentBindingError",
    "AgentLimitError", "AgentExecutionError",
]
