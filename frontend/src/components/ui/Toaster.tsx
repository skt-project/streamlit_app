import { useToastStore } from "@/store/toastStore";
import Icon from "./Icon";
import type { ToastType } from "@/store/toastStore";
import type { IconName } from "./Icon";

const ICON_MAP: Record<ToastType, IconName> = {
  success: "check-circle",
  error:   "exclamation-circle",
  warning: "exclamation-triangle",
  info:    "information-circle",
};

const CLS_MAP: Record<ToastType, string> = {
  success: "toast-success",
  error:   "toast-error",
  warning: "toast-warning",
  info:    "toast-info",
};

const ICON_CLS_MAP: Record<ToastType, string> = {
  success: "text-emerald-500",
  error:   "text-red-500",
  warning: "text-amber-500",
  info:    "text-blue-500",
};

export default function Toaster() {
  const { toasts, dismiss } = useToastStore();

  // Always render the container so aria-live region is in the DOM before toasts appear.
  // Screen readers only announce additions to live regions they've already seen.
  return (
    <div
      className="toast-container"
      aria-live="polite"
      aria-atomic="false"
      aria-label="Notifikasi sistem"
    >
      {toasts.map((t) => (
        <div key={t.id} className={CLS_MAP[t.type]} role={t.type === "error" ? "alert" : "status"}>
          <Icon
            name={ICON_MAP[t.type]}
            className={`toast-icon ${ICON_CLS_MAP[t.type]}`}
            aria-hidden={true}
          />
          <p className="flex-1 text-sm leading-snug">{t.message}</p>
          <button
            onClick={() => dismiss(t.id)}
            className="shrink-0 text-current opacity-50 hover:opacity-100 transition-opacity"
            aria-label="Tutup notifikasi"
          >
            <Icon name="x-mark" className="w-4 h-4" aria-hidden={true} />
          </button>
        </div>
      ))}
    </div>
  );
}
