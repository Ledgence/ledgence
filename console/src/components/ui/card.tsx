// Adapted from shadcn/ui; see third_party/shadcn/provenance.json (MIT).
import type { ComponentProps } from "react";
export function Card({ className = "", ...props }: ComponentProps<"section">) {
  return (
    <section data-slot="card" className={`card ${className}`} {...props} />
  );
}
export function CardHeader({
  className = "",
  ...props
}: ComponentProps<"div">) {
  return (
    <div
      data-slot="card-header"
      className={`card-header ${className}`}
      {...props}
    />
  );
}
export function CardTitle(props: ComponentProps<"h2">) {
  return <h2 data-slot="card-title" {...props} />;
}
export function CardDescription({
  className = "",
  ...props
}: ComponentProps<"p">) {
  return (
    <p
      data-slot="card-description"
      className={`card-description ${className}`}
      {...props}
    />
  );
}
export function CardContent({
  className = "",
  ...props
}: ComponentProps<"div">) {
  return (
    <div
      data-slot="card-content"
      className={`card-content ${className}`}
      {...props}
    />
  );
}
export function CardFooter({
  className = "",
  ...props
}: ComponentProps<"div">) {
  return (
    <div
      data-slot="card-footer"
      className={`card-footer ${className}`}
      {...props}
    />
  );
}
