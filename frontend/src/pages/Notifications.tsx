import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, Skeleton } from "@/components/ui";
import type { IconName } from "@/components/ui/Icon";
import { api } from "@/api/client";
import type { Notification } from "@/types";
import { format } from "date-fns";
import { fetchNotifications } from "@/api/notifications";

type TypeConfig = { icon: IconName; badgeCls: string };
const TYPE_CONFIG: Record<string, TypeConfig> = {
  approval:     { icon: "check-circle",       badgeCls: "icon-badge-green"  },
  announcement: { icon: "megaphone",           badgeCls: "icon-badge-blue"   },
  compliance:   { icon: "exclamation-triangle",badgeCls: "icon-badge-amber"  },
  target:       { icon: "arrow-trending-up",   badgeCls: "icon-badge-purple" },
  system:       { icon: "information-circle",  badgeCls: "icon-badge-slate"  },
};
const DEFAULT_CONFIG: TypeConfig = { icon: "bell", badgeCls: "icon-badge-slate" };

function NotificationSkeleton() {
  return (
    <div className="space-y-2" aria-hidden="true">
      {Array.from({ length: 6 }).map((_, i) => (
        <div key={i} className="card flex items-start gap-4">
          <Skeleton className="w-9 h-9 rounded-lg shrink-0" />
          <div className="flex-1 space-y-1.5">
            <Skeleton className="h-4 w-40" />
            <Skeleton className="h-3 w-60" />
          </div>
          <Skeleton className="h-3 w-12 shrink-0" />
        </div>
      ))}
    </div>
  );
}

export default function Notifications() {
  const qc = useQueryClient();

  const { data: items = [], isLoading } = useQuery<Notification[]>({
    queryKey: ["notifications"],
    queryFn:  fetchNotifications,
    staleTime: 60_000,
    placeholderData: (prev) => prev,
  });

  const markAllMutation = useMutation({
    mutationFn: () => api.post("/notifications/mark-all-read"),
    onSuccess:  () => qc.invalidateQueries({ queryKey: ["notifications"] }),
  });

  const markOneMutation = useMutation({
    mutationFn: (id: string) => api.post(`/notifications/${id}/read`),
    onSuccess:  () => qc.invalidateQueries({ queryKey: ["notifications"] }),
  });

  const unreadCount = items.filter((n) => !n.is_read).length;

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Notifikasi"
        actions={
          unreadCount > 0 ? (
            <button
              onClick={() => markAllMutation.mutate()}
              className="btn-secondary text-sm"
              disabled={markAllMutation.isPending}
            >
              Tandai semua dibaca ({unreadCount})
            </button>
          ) : undefined
        }
      />

      <main className="flex-1 overflow-y-auto p-6">
        <div className="max-w-2xl">
        {isLoading ? (
          <NotificationSkeleton />
        ) : items.length === 0 ? (
          <EmptyState
            icon="bell"
            title="Tidak ada notifikasi"
            description="Semua pemberitahuan penting akan muncul di sini"
          />
        ) : (
          <div className="space-y-2">
            {items.map((n) => {
              const cfg = TYPE_CONFIG[n.type] ?? DEFAULT_CONFIG;
              return (
                <div
                  key={n.notification_id}
                  role="button"
                  tabIndex={0}
                  aria-label={`${n.title}${!n.is_read ? " — belum dibaca" : ""}`}
                  onClick={() => { if (!n.is_read) markOneMutation.mutate(n.notification_id); }}
                  onKeyDown={(e) => {
                    if ((e.key === "Enter" || e.key === " ") && !n.is_read) {
                      e.preventDefault();
                      markOneMutation.mutate(n.notification_id);
                    }
                  }}
                  className={`card flex items-start gap-4 cursor-pointer transition-colors ${
                    !n.is_read
                      ? "border-l-4 border-primary-400 bg-primary-50/30"
                      : "hover:bg-slate-50"
                  }`}
                >
                  <span className={`icon-badge ${cfg.badgeCls} shrink-0 mt-0.5`}>
                    <Icon name={cfg.icon} className="w-4 h-4" />
                  </span>

                  <div className="flex-1 min-w-0">
                    <div className="flex items-start justify-between gap-2">
                      <p className={`text-sm ${!n.is_read ? "font-semibold text-slate-800" : "text-slate-700"}`}>
                        {n.title}
                      </p>
                      <p className="text-xs text-slate-400 shrink-0 mt-0.5">
                        {format(new Date(n.created_at), "d MMM HH:mm")}
                      </p>
                    </div>
                    <p className="text-xs text-slate-500 mt-0.5 line-clamp-2">{n.body}</p>
                  </div>

                  {!n.is_read && (
                    <div className="w-2 h-2 rounded-full bg-primary-500 shrink-0 mt-2" aria-hidden="true" />
                  )}
                </div>
              );
            })}
          </div>
        )}
        </div>
      </main>
    </div>
  );
}
