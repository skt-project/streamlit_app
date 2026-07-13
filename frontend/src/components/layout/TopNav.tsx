import { useEffect } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { Icon } from "@/components/ui";
import type { Notification } from "@/types";
import { fetchNotifications } from "@/api/notifications";

interface Props {
  title: string;
  subtitle?: string;
  actions?: React.ReactNode;
}

export default function TopNav({ title, subtitle, actions }: Props) {
  useEffect(() => {
    document.title = `${title} — STEP`;
    return () => { document.title = "STEP — Territory & Execution Platform"; };
  }, [title]);

  const { data: notifications } = useQuery<Notification[]>({
    queryKey: ["notifications"],
    queryFn: fetchNotifications,
    staleTime: 60_000,
    refetchOnWindowFocus: true,
  });
  const notifCount = (notifications ?? []).filter((n) => !n.is_read).length;
  return (
    <header className="h-14 bg-white border-b border-slate-200 flex items-center justify-between px-6 shrink-0 sticky top-0 z-10 shadow-[0_1px_0_0_theme(colors.slate.200)]">
      <div className="flex flex-col justify-center min-w-0">
        <h1 className="text-base font-semibold text-slate-900 tracking-tight leading-tight truncate">
          {title}
        </h1>
        {subtitle && (
          <p className="text-xs text-slate-400 mt-0.5 truncate">{subtitle}</p>
        )}
      </div>

      <div className="flex items-center gap-1.5 shrink-0 ml-4">
        {actions}

        <Link
          to="/notifications"
          className="relative btn-icon"
          aria-label={notifCount > 0 ? `Notifikasi, ${notifCount} belum dibaca` : "Notifikasi"}
        >
          <Icon name="bell" className="w-5 h-5" aria-hidden={true} />
          {notifCount > 0 && (
            <span
              aria-hidden="true"
              className="absolute -top-0.5 -right-0.5 min-w-[1.1rem] h-[1.1rem] flex items-center justify-center
                             rounded-full bg-red-500 text-white text-[10px] font-bold leading-none px-0.5"
            >
              {notifCount > 99 ? "99+" : notifCount}
            </span>
          )}
        </Link>
      </div>
    </header>
  );
}
