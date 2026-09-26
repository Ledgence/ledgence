// Adapted from shadcn/ui; see third_party/shadcn/provenance.json (MIT).
import type { ComponentProps } from "react";
import * as Primitive from "@radix-ui/react-dialog";
import { X } from "lucide-react";
export function Dialog(props: ComponentProps<typeof Primitive.Root>) {
  return <Primitive.Root {...props} />;
}
export function DialogTrigger(props: ComponentProps<typeof Primitive.Trigger>) {
  return <Primitive.Trigger {...props} />;
}
export function DialogClose(props: ComponentProps<typeof Primitive.Close>) {
  return <Primitive.Close {...props} />;
}
export function DialogContent({
  children,
  className = "",
  ...props
}: ComponentProps<typeof Primitive.Content>) {
  return (
    <Primitive.Portal>
      <Primitive.Overlay className="dialog-overlay" />
      <Primitive.Content className={`dialog-content ${className}`} {...props}>
        {children}
        <Primitive.Close className="dialog-close" aria-label="Close dialog">
          <X aria-hidden="true" />
        </Primitive.Close>
      </Primitive.Content>
    </Primitive.Portal>
  );
}
export function DialogTitle(props: ComponentProps<typeof Primitive.Title>) {
  return <Primitive.Title className="dialog-title" {...props} />;
}
export function DialogDescription(
  props: ComponentProps<typeof Primitive.Description>,
) {
  return <Primitive.Description className="dialog-description" {...props} />;
}
