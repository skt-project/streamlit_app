import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import type { Notification } from "@/types";
import { format } from "date-fns";

const fetchNotifications = () => api.get("/notifications").then((r) => r.data);

const TYPE_ICON: Record<string, string> = {
  approval:     "✅",
  announcement: "📢",
  compliance:   "⚠️",
  target:       "🎯",
  system:       "ℹ️",
};

export default function Notifications() {
  const qc = useQueryClient();

  const { data: items = [], isLoading } = useQuery<Notification[]>({
    queryKey: ["notifications"],
    queryFn: fetchNotifications,
  });

  const markAllMutation = useMutation({
    mutationFn: () => api.post("/notifications/mark-all-read"),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["notifications"] }),
  });

  const markOneMutation = useMutation({
    mutationFn: (id: string) => api.post(`/notifications/${id}/read`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["notifications"] }),
  });

  const unreadCount = items.filter((n) => !n.is_read).length;

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Notifikasi"
        actions={
          unreadCount > 0 ? (
            <button onClick={() => markAllMutation.mutate()} className="btn-secondary text-sm" disabled={markAllMutation.isPending}>
              Tandai semua dibaca ({unreadCount})
            </button>
          ) : undefined
        }
      />

      <main className="flex-1 overflow-y-auto p-6 max-w-2xl">
        {isLoading ? (
          <p className="text-sm text-slate-400">Memuat...</p>
        ) : items.length === 0 ? (
          <div className="text-center py-20 text-slate-400">
            <p className="text-5xl mb-3">🔔</p>
            <p>Tidak ada notifikasi</p>
          </div>
        ) : (
          <div className="space-y-2">
            {items.map((n) => (
              <div
                key={n.notification_id}
                onClick={() => { if (!n.is_read) markOneMutation.mutate(n.notification_id); }}
                className={`card flex items-start gap-4 cursor-pointer transition-colors ${!n.is_read ? "border-l-4 border-primary-400 bg-primary-50/30" : "hover:bg-slate-50"}`}
              >
                <span className="text-2xl shrink-0 mt-0.5">{TYPE_ICON[n.type] ?? "🔔"}</span>
                <div className="flex-1 min-w-0">
                  <div className="flex items-start justify-between gap-2">
                    <p className={`text-sm ${!n.is_read ? "font-semibold text-slate-800" : "text-slate-700"}`}>{n.title}</p>
                    <p className="text-xs text-slate-400 shrink-0 mt-0.5">{format(new Date(n.created_at), "d MMM HH:mm")}</p>
                  </div>
                  <p className="text-xs text-slate-500 mt-0.5 line-clamp-2">{n.body}</p>
                </div>
                {!n.is_read && <div className="w-2 h-2 rounded-full bg-primary-500 shrink-0 mt-2" />}
              </div>
            ))}
          </div>
        )}
      </main>
    </div>
  );
}
