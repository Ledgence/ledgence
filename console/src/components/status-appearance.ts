// SPDX-License-Identifier: MIT
import { ArrowRight, Check, Circle, Clock3, Play, X } from "lucide-react";

export function statusAppearance(value: string) {
  switch (value) {
    case "succeeded":
    case "accepted":
    case "approved":
    case "fresh":
    case "warm":
    case "returned":
    case "replayed":
      return { Icon: Check, tone: "success" } as const;
    case "failed":
    case "failing":
    case "lost":
    case "cleanup_pending":
    case "rejected":
    case "decision rejected":
      return { Icon: X, tone: "failure" } as const;
    case "active":
    case "running":
      return { Icon: Play, tone: "active" } as const;
    case "pending":
    case "waiting":
    case "queued":
    case "cancelling":
    case "open":
    case "wait registered":
    case "awaiting operator resolution":
      return { Icon: Clock3, tone: "waiting" } as const;
    default:
      return {
        Icon: value.includes("scheduled") ? ArrowRight : Circle,
        tone: "neutral",
      } as const;
  }
}
