// SPDX-License-Identifier: MIT
import { approvalFixture } from "./approval-fixture";
import type { Approval } from "../src/api/approvals";
export function acceptedApproval(): Approval {
  const approval = approvalFixture();
  return {
    ...approval,
    status: "approved",
    decision: {
      decision_id: "decision-original",
      decision: "approve",
      reviewer: "Caller label",
      reason: null,
      decided_at: approval.created_at + 1,
    },
  };
}
export function acceptanceEvent(
  instanceId = "instance-test",
  approval = acceptedApproval(),
) {
  return {
    event_id: "acceptance-original",
    recorded_at: approval.decision!.decided_at,
    audience: {
      authority_id: "authority-test",
      instance_id: instanceId,
      scope: { tenant_id: "acme", namespace: "demo" },
    },
    actor: {
      issuer: "urn:ledgence:authority:authority-test",
      subject: "Original operator",
      class: "operator" as const,
      credential_id: "original-credential",
    },
    operation: {
      kind: "approval_accepted" as const,
      workflow_id: approval.workflow_id,
      key: approval.key,
      activation_id: approval.activation_id,
      revision: approval.revision,
      decision_id: approval.decision!.decision_id,
      decision: approval.decision!.decision,
    },
  };
}
