// Adapted from shadcn/ui; see third_party/shadcn/provenance.json (MIT).
import type { ComponentProps } from "react";
type ButtonProps = ComponentProps<"button"> & {
  variant?: "primary" | "outline" | "ghost" | "destructive";
};
export function Button({
  className = "",
  variant = "primary",
  type = "button",
  ...props
}: ButtonProps) {
  return (
    <button
      type={type}
      data-slot="button"
      className={`button button-${variant} ${className}`}
      {...props}
    />
  );
}
