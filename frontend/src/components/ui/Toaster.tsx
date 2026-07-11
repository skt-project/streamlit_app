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

  if (toasts.length === 0) return null;

  return (
    <div className="toast-container" role="region" aria-label="Notifikasi">
      {toasts.map((t) => (
        <div key={t.id} className={CLS_MAP[t.type]}>
          <Icon
            name={ICON_MAP[t.type]}
            className={`toast-icon ${ICON_CLS_MAP[t.type]}`}
          />
          <p className="flex-1 text-sm leading-snug">{t.message}</p>
          <button
            onClick={() => dismiss(t.id)}
            className="shrink-0 text-current opacity-50 hover:opacity-100 transition-opacity"
            aria-label="Tutup"
          >
            <Icon name="x-mark" className="w-4 h-4" />
          </button>
        </div>
      ))}
    </div>
  );
}
