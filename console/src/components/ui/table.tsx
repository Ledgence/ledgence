// Adapted from shadcn/ui; see third_party/shadcn/provenance.json (MIT).
import type { ComponentProps } from "react";
export function Table({ className = "", ...props }: ComponentProps<"table">) {
  return (
    <div data-slot="table-container" className="table-container">
      <table data-slot="table" className={`table ${className}`} {...props} />
    </div>
  );
}
export function TableHeader(props: ComponentProps<"thead">) {
  return <thead {...props} />;
}
export function TableBody(props: ComponentProps<"tbody">) {
  return <tbody {...props} />;
}
export function TableRow(props: ComponentProps<"tr">) {
  return <tr {...props} />;
}
export function TableHead(props: ComponentProps<"th">) {
  return <th scope="col" {...props} />;
}
export function TableCell(props: ComponentProps<"td">) {
  return <td {...props} />;
}
