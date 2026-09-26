// Adapted from shadcn/ui; see third_party/shadcn/provenance.json (MIT).
import type { ComponentProps } from "react";
export function Input({
  className = "",
  type = "text",
  ...props
}: ComponentProps<"input">) {
  return (
    <input
      type={type}
      data-slot="input"
      className={`input ${className}`}
      {...props}
    />
  );
}
