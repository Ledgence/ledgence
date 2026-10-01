"""Shared program identities and defaults (MIT)."""
VERSION = "1.2.0"
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
    "page_count(item_count) returns the number of pages for document search results, with 100 items per page. "
    "Returns 0 for no results; exact multiples of 100 must not add an empty page. "
    "Reject bool and every non-int input with TypeError. Reject negative integers with ValueError. "
    "Change only pagination.py. Keep the implementation small and use only the Python standard library."
)
