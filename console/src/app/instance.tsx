import { createContext, useContext } from "react";
import type { ConsoleConfig } from "../api/contracts";
export const InstanceContext = createContext<ConsoleConfig | null>(null);
export function useInstance(): ConsoleConfig {
  const config = useContext(InstanceContext);
  if (!config) throw new Error("Console configuration is required.");
  return config;
}
