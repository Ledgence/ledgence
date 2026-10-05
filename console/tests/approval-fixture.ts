// SPDX-License-Identifier: MIT
import { approval, type Approval } from "../src/api/approvals";
import { parseUserJson } from "../src/api/json";

export function approvalFixture(
  status: Approval["status"] = "pending",
): Approval {
  return approval({
    workflow_id: "workflow-review",
    key: "publish-report",
    activation_id: "activation-review",
    revision: "9007199254740993",
    action: {
      name: "publish_report",
      version: "release-3",
      arguments: parseUserJson(
        '{"destination":"reports/approved","amount":18446744073709551615,"ratio":1.0,"offset":-0.0}',
        32768,
      ),
    },
    proposed_arguments: { destination: "reports/draft" },
    created_at: 1,
    deadline: 100000,
    status,
    decision:
      status === "approved" || status === "rejected"
        ? {
            decision_id: "decision-recorded",
            decision: status === "approved" ? "approve" : "reject",
            reviewer: "Casey",
            reason: "Reviewed destination",
            decided_at: 5000,
          }
        : null,
    resumed_activation_id:
      status === "pending" || status === "cancelled"
        ? null
        : "activation-resumed",
  });
}
