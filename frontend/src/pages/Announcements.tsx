import { useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, Skeleton } from "@/components/ui";
import { api } from "@/api/client";
import { useFocusTrap } from "@/hooks/useFocusTrap";
import type { Announcement, AnnouncementType } from "@/types";
import { format } from "date-fns";
import { useAuthStore } from "@/store/authStore";

const TYPES: AnnouncementType[] = ["Campaign", "Policy", "Meeting", "Distributor", "Training"];

const TYPE_BADGE: Record<string, string> = {
  Campaign:    "badge-blue",
  Policy:      "badge-yellow",
  Meeting:     "badge-green",
  Distributor: "badge-purple",
  Training:    "badge-gray",
};

const fetchAnnouncements = (type: string) =>
  api.get("/announcements", { params: type !== "Semua" ? { type } : {} }).then((r) => r.data);

export default function Announcements() {
  const user = useAuthStore((s) => s.user);
  const qc = useQueryClient();
  const [activeType, setActiveType] = useState<string>("Semua");
  const [showModal, setShowModal] = useState(false);
  const [form, setForm] = useState({ type: "Campaign", title: "", body: "", audience: "Semua" });
  const modalTriggerRef = useRef<Element | null>(null);
  const modalPanelRef = useRef<HTMLDivElement>(null);
  useFocusTrap(modalPanelRef, showModal);
  const closeModal = () => {
    setShowModal(false);
    setTimeout(() => { (modalTriggerRef.current as HTMLElement | null)?.focus(); }, 0);
  };

  const { data: items = [], isLoading } = useQuery<Announcement[]>({
    queryKey: ["announcements", activeType],
    queryFn: () => fetchAnnouncements(activeType),
    staleTime: 5 * 60 * 1000,  // matches backend 300s cache
    placeholderData: (prev) => prev,
  });

  const createMutation = useMutation({
    mutationFn: () => api.post("/announcements", form),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["announcements"] }); closeModal(); setForm({ type: "Campaign", title: "", body: "", audience: "Semua" }); },
  });

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Pusat Pengumuman"
        actions={
          user?.role === "ho_admin" ? (
            <button onClick={() => { modalTriggerRef.current = document.activeElement; setShowModal(true); }} className="btn-primary text-sm">
              + Pengumuman Baru
            </button>
          ) : undefined
        }
      />

      <main className="flex-1 overflow-y-auto p-6">
        {/* Type Tabs */}
        <div className="flex gap-2 mb-6 flex-wrap">
          {["Semua", ...TYPES].map((t) => (
            <button
              key={t}
              onClick={() => setActiveType(t)}
              className={`chip ${activeType === t ? "chip-active" : ""}`}
              aria-pressed={activeType === t}
            >
              {t}
            </button>
          ))}
        </div>

        {isLoading ? (
          <div className="space-y-4 max-w-2xl" aria-hidden="true">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="card space-y-2">
                <div className="flex items-center gap-2">
                  <Skeleton className="h-5 w-20 rounded-full" />
                  <Skeleton className="h-4 w-24 ml-auto" />
                </div>
                <Skeleton className="h-5 w-48" />
                <Skeleton className="h-4 w-full" />
                <Skeleton className="h-4 w-3/4" />
              </div>
            ))}
          </div>
        ) : items.length === 0 ? (
          <EmptyState
            icon="megaphone"
            title="Belum ada pengumuman"
            description="Pengumuman baru akan muncul di sini"
          />
        ) : (
          <div className="space-y-4 max-w-2xl">
            {items.map((a) => (
              <div key={a.announcement_id} className="card">
                <div className="flex items-start gap-3 mb-2">
                  <span className={`${TYPE_BADGE[a.type] ?? "badge-blue"} shrink-0`}>{a.type}</span>
                  <p className="text-xs text-slate-400 ml-auto shrink-0">{format(new Date(a.created_at), "d MMM yyyy")}</p>
                </div>
                <h3 className="font-semibold text-slate-800 mb-1">{a.title}</h3>
                <p className="text-sm text-slate-600">{a.body}</p>
                <p className="text-xs text-slate-400 mt-3">Audience: {a.audience}</p>
              </div>
            ))}
          </div>
        )}
      </main>

      {showModal && (
        <div
          className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4"
          onClick={(e) => { if (e.target === e.currentTarget) closeModal(); }}
        >
          <div
            ref={modalPanelRef}
            role="dialog"
            aria-modal="true"
            aria-labelledby="ann-modal-title"
            className="bg-white rounded-2xl shadow-2xl w-full max-w-md"
          >
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 id="ann-modal-title" className="font-semibold text-slate-800">Buat Pengumuman Baru</h3>
              <button onClick={closeModal} className="text-slate-400 hover:text-slate-600 p-1 rounded-lg hover:bg-slate-100 transition-colors" aria-label="Tutup"><Icon name="x-mark" className="w-5 h-5" /></button>
            </div>
            <div className="p-5 space-y-4">
              <div>
                <label htmlFor="ann-type" className="block text-sm font-medium text-slate-700 mb-1">Tipe</label>
                <select id="ann-type" className="input" value={form.type} onChange={(e) => setForm((f) => ({ ...f, type: e.target.value }))}>
                  {TYPES.map((t) => <option key={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label htmlFor="ann-title" className="block text-sm font-medium text-slate-700 mb-1">Judul</label>
                <input id="ann-title" className="input" value={form.title} onChange={(e) => setForm((f) => ({ ...f, title: e.target.value }))} autoFocus />
              </div>
              <div>
                <label htmlFor="ann-body" className="block text-sm font-medium text-slate-700 mb-1">Isi</label>
                <textarea id="ann-body" className="input" rows={4} value={form.body} onChange={(e) => setForm((f) => ({ ...f, body: e.target.value }))} />
              </div>
              <div>
                <label htmlFor="ann-audience" className="block text-sm font-medium text-slate-700 mb-1">Audience</label>
                <input id="ann-audience" className="input" value={form.audience} onChange={(e) => setForm((f) => ({ ...f, audience: e.target.value }))} />
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={closeModal} className="btn-secondary">Batal</button>
              <button onClick={() => createMutation.mutate()} className="btn-primary" disabled={!form.title || !form.body || createMutation.isPending}>
                {createMutation.isPending ? "Memproses..." : "Publikasikan"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
