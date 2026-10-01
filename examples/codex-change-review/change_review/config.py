"""Shared program identities and defaults (MIT)."""
VERSION = "1.1.0"
WORKFLOW = "codex-change-review"
IMPLEMENT = "codex-change-implement"
FINALIZE = "codex-change-finalize"
DEFAULT_MODEL = "gpt-6-luna"
CONTROL_QUEUE = "change-review"
AGENT_QUEUE = "change-review-agents"
APPROVAL_KEY = "approval:0"
DEFAULT_APPROVAL_TIMEOUT_MS = 3_600_000
MAX_APPROVAL_TIMEOUT_MS = 86_400_000
MAX_CANDIDATE_BYTES = 24 * 1024
MAX_REPORT_BYTES = 16 * 1024
MAX_STATE_BYTES = 56 * 1024
TEST_COUNT = 6
REQUIREMENTS = (
    "shipping_cost(total_cents) returns 0 for totals >= 10000 cents and 500 otherwise. "
    "Reject bool and every non-int input with TypeError. Reject negative integers with ValueError. "
    "Change only shipping.py. Keep the implementation small and use only the Python standard library."
)
