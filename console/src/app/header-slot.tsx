// SPDX-License-Identifier: MIT
import { useContext, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { ShellHeaderTargetContext } from "./header-slot-context";

export function ShellHeaderContent({ children }: { children: ReactNode }) {
  const target = useContext(ShellHeaderTargetContext);
  return target ? createPortal(children, target) : children;
}
