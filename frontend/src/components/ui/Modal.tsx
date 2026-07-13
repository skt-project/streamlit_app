import { useEffect, useRef } from "react";
import Icon from "./Icon";
import { useFocusTrap } from "@/hooks/useFocusTrap";

interface ModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  children: React.ReactNode;
  footer?: React.ReactNode;
  maxWidth?: "sm" | "md" | "lg" | "xl" | "2xl";
}

const MAX_W = {
  sm:  "max-w-sm",
  md:  "max-w-md",
  lg:  "max-w-lg",
  xl:  "max-w-xl",
  "2xl": "max-w-2xl",
};

export default function Modal({
  open,
  onClose,
  title,
  children,
  footer,
  maxWidth = "md",
}: ModalProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useFocusTrap(panelRef, open);

  useEffect(() => {
    if (!open) return;
    const handleEscape = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", handleEscape);
    return () => document.removeEventListener("keydown", handleEscape);
  }, [open, onClose]);

  useEffect(() => {
    if (open) panelRef.current?.focus();
  }, [open]);

  if (!open) return null;

  return (
    <div
      className="modal-backdrop"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div
        ref={panelRef}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-labelledby="modal-title"
        className={`modal-panel ${MAX_W[maxWidth]} outline-none`}
      >
        <div className="modal-header">
          <h2 id="modal-title" className="modal-title">{title}</h2>
          <button
            onClick={onClose}
            className="btn-icon -m-1 shrink-0"
            aria-label="Tutup"
          >
            <Icon name="x-mark" className="w-5 h-5" />
          </button>
        </div>

        <div className="modal-body">{children}</div>

        {footer && <div className="modal-footer">{footer}</div>}
      </div>
    </div>
  );
}
