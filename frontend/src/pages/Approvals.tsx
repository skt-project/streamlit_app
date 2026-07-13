import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, Skeleton, EmptyState } from "@/components/ui";
import { toast } from "@/store/toastStore";
import { api } from "@/api/client";
import type { ApprovalRequest, ApprovalStatus } from "@/types";
import { format } from "date-fns";
import { useAuthStore } from "@/store/authStore";

const fetchApprovals = (status: string) =>
  api.get("/approvals", { params: { status } }).then((r) => r.data);

const TYPE_LABELS: Record<string, string> = {
  target_adjust: "Penyesuaian Target",
  tier_override:  "Override Tier",
  reopen:         "Buka Kembali",
};

const TYPE_ICON: Record<string, React.ReactNode> = {
  target_adjust: <Icon name="chart-bar" className="w-4 h-4" />,
  tier_override:  <Icon name="star" className="w-4 h-4" />,
  reopen:         <Icon name="arrow-path" className="w-4 h-4" />,
};

function StatusPill({ status }: { status: ApprovalStatus }) {
  if (status === "pending")  return <span className="badge-yellow">Menunggu</span>;
  if (status === "approved") return <span className="badge-green">Disetujui</span>;
  if (status === "rejected") return <span className="badge-red">Ditolak</span>;
  return <span className="badge-gray">Revisi</span>;
}

function ApprovalListSkeleton() {
  return (
    <div className="divide-y divide-slate-100">
      {[1, 2, 3, 4].map((i) => (
        <div key={i} className="p-4 space-y-2">
          <div className="flex items-center justify-between gap-2">
            <Skeleton className="h-4 w-40" />
            <Skeleton className="h-5 w-16 rounded-full" />
          </div>
          <Skeleton className="h-3 w-24 rounded-full" />
          <Skeleton className="h-3 w-32" />
        </div>
      ))}
    </div>
  );
}

export default function Approvals() {
  const user = useAuthStore((s) => s.user);
  const qc = useQueryClient();
  const [tab, setTab] = useState<"pending" | "history">("pending");
  const [selected, setSelected] = useState<ApprovalRequest | null>(null);
  const [comment, setComment] = useState("");

  const { data: approvals = [], isLoading } = useQuery<ApprovalRequest[]>({
    queryKey: ["approvals", tab],
    queryFn: () => fetchApprovals(tab),
    staleTime: 30_000,
    placeholderData: (prev) => prev,
  });

  const decideMutation = useMutation({
    mutationFn: ({ id, decision }: { id: string; decision: "approve" | "reject" }) =>
      api.post(`/approvals/${id}/${decision}`, { comment }),
    onSuccess: (_, { decision }) => {
      qc.invalidateQueries({ queryKey: ["approvals"] });
      setSelected(null);
      setComment("");
      toast.success(decision === "approve" ? "Permintaan disetujui." : "Permintaan ditolak.");
    },
    onError: () => toast.error("Gagal memproses permintaan."),
  });

  const canDecide = user?.role === "asm" || user?.role === "dm" || user?.role === "ho_admin";

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Approvals" subtitle="Manajemen persetujuan permintaan tim" />

      <div className="flex flex-1 min-h-0">
        {/* ── List Panel ── */}
        <div className="w-80 border-r border-slate-200 bg-white flex flex-col shrink-0">
          {/* Chip filter tabs */}
          <div className="px-4 pt-4 pb-3 border-b border-slate-100">
            <div className="chip-group">
              <button
                className={`chip ${tab === "pending" ? "chip-active" : ""}`}
                onClick={() => { setTab("pending"); setSelected(null); }}
                aria-pressed={tab === "pending"}
              >
                <Icon name="clock" className="w-3.5 h-3.5" />
                Menunggu
                {tab === "pending" && approvals.length > 0 && (
                  <span className="ml-1 inline-flex items-center justify-center w-4 h-4 text-2xs rounded-full bg-primary-100 text-primary-700">
                    {approvals.length}
                  </span>
                )}
              </button>
              <button
                className={`chip ${tab === "history" ? "chip-active" : ""}`}
                onClick={() => { setTab("history"); setSelected(null); }}
                aria-pressed={tab === "history"}
              >
                <Icon name="document-text" className="w-3.5 h-3.5" />
                Riwayat
              </button>
            </div>
          </div>

          {/* Items */}
          <div className="flex-1 overflow-y-auto">
            {isLoading ? (
              <ApprovalListSkeleton />
            ) : approvals.length === 0 ? (
              <EmptyState
                icon="inbox"
                title={tab === "pending" ? "Tidak ada permintaan" : "Belum ada riwayat"}
                description={tab === "pending" ? "Semua permintaan sudah diproses." : "Riwayat approval akan muncul di sini."}
                className="py-10"
              />
            ) : (
              <div className="divide-y divide-slate-50">
                {approvals.map((req) => (
                  <button
                    key={req.approval_id}
                    onClick={() => setSelected(req)}
                    aria-current={selected?.approval_id === req.approval_id ? "true" : undefined}
                    className={`approval-item w-full ${selected?.approval_id === req.approval_id ? "approval-item-active" : ""}`}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="icon-badge icon-badge-sm icon-badge-blue shrink-0">
                          {TYPE_ICON[req.type] ?? <Icon name="document-text" className="w-3.5 h-3.5" />}
                        </span>
                        <p className="text-sm font-medium text-slate-800 line-clamp-2 text-left leading-snug">{req.title}</p>
                      </div>
                      <StatusPill status={req.status} />
                    </div>
                    <div className="flex items-center gap-2 mt-2 pl-9">
                      <span className="text-2xs font-medium text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded">
                        {TYPE_LABELS[req.type] ?? req.type}
                      </span>
                    </div>
                    <p className="text-2xs text-slate-400 mt-1.5 pl-9">
                      {req.submitted_by} · {format(new Date(req.submitted_at), "d MMM yyyy")}
                    </p>
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* ── Detail Panel ── */}
        <div className="flex-1 overflow-y-auto bg-slate-50">
          {!selected ? (
            <div className="flex flex-col items-center justify-center h-full gap-3">
              <div className="icon-badge icon-badge-slate icon-badge-lg">
                <Icon name="clipboard-document-list" className="w-6 h-6" />
              </div>
              <p className="text-sm text-slate-400 font-medium">Pilih permintaan untuk melihat detail</p>
            </div>
          ) : (
            <div className="p-6 max-w-2xl mx-auto space-y-5">
              {/* Hero */}
              <div className="card">
                <div className="flex items-start gap-4">
                  <span className="icon-badge icon-badge-blue icon-badge-lg shrink-0">
                    {TYPE_ICON[selected.type] ?? <Icon name="document-text" className="w-5 h-5" />}
                  </span>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap mb-1.5">
                      <span className="text-2xs font-semibold text-slate-500 uppercase tracking-wide">
                        {TYPE_LABELS[selected.type] ?? selected.type}
                      </span>
                      <StatusPill status={selected.status} />
                    </div>
                    <h2 className="text-lg font-bold text-slate-900 leading-snug">{selected.title}</h2>
                    <p className="text-xs text-slate-400 mt-1">
                      Diajukan oleh <span className="font-medium text-slate-600">{selected.submitted_by}</span>{" "}
                      pada {format(new Date(selected.submitted_at), "d MMM yyyy, HH:mm")}
                    </p>
                  </div>
                </div>
              </div>

              {/* Detail grid */}
              <div className="card">
                <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-4">Detail Perubahan</p>
                <div className="info-grid">
                  <div className="data-row">
                    <span className="data-label">Nilai Saat Ini</span>
                    <span className="data-value">{String(selected.current_value ?? "—")}</span>
                  </div>
                  <div className="data-row">
                    <span className="data-label">Nilai Diusulkan</span>
                    <span className="data-value text-primary-600 font-semibold">{String(selected.proposed_value)}</span>
                  </div>
                </div>
              </div>

              {/* Reason */}
              <div className="card">
                <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Alasan Pengajuan</p>
                <div className="rail-blue p-4 rounded-xl">
                  <p className="text-sm text-slate-700 leading-relaxed">{selected.reason}</p>
                </div>
              </div>

              {/* Comments timeline */}
              {selected.comments.length > 0 && (
                <div className="card">
                  <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-4">
                    Komentar &amp; Timeline
                  </p>
                  <div className="space-y-3">
                    {selected.comments.map((c) => (
                      <div key={`${c.author}-${c.created_at}`} className="flex gap-3">
                        <div className="icon-badge icon-badge-slate shrink-0" style={{ width: 28, height: 28 }}>
                          <Icon name="user" className="w-3.5 h-3.5" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-xs font-semibold text-slate-800">{c.author}</span>
                            <span className="text-2xs font-medium text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded">
                              {c.role}
                            </span>
                            <span className="text-2xs text-slate-400 ml-auto">
                              {format(new Date(c.created_at), "d MMM, HH:mm")}
                            </span>
                          </div>
                          <p className="text-sm text-slate-600 bg-slate-50 rounded-lg px-3 py-2 leading-relaxed">
                            {c.body}
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Action area (approver) */}
              {canDecide && selected.status === "pending" && (
                <div className="card">
                  <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Tindakan</p>
                  <textarea
                    className="input text-sm"
                    placeholder="Tambahkan komentar (wajib jika menolak)…"
                    aria-label="Komentar persetujuan"
                    rows={3}
                    value={comment}
                    onChange={(e) => setComment(e.target.value)}
                  />
                  <div className="flex gap-3 mt-4">
                    <button
                      className="btn-danger flex-1"
                      onClick={() => decideMutation.mutate({ id: selected.approval_id, decision: "reject" })}
                      disabled={!comment.trim() || decideMutation.isPending}
                    >
                      <Icon name="x-mark" className="w-4 h-4" />
                      Tolak
                    </button>
                    <button
                      className="btn-primary flex-1"
                      onClick={() => decideMutation.mutate({ id: selected.approval_id, decision: "approve" })}
                      disabled={decideMutation.isPending}
                    >
                      {decideMutation.isPending ? (
                        <Icon name="arrow-path" className="w-4 h-4 animate-spin" />
                      ) : (
                        <Icon name="check" className="w-4 h-4" />
                      )}
                      {decideMutation.isPending ? "Memproses…" : "Setujui"}
                    </button>
                  </div>
                </div>
              )}

              {/* Revise (submitter after rejection) */}
              {selected.status === "rejected" && user?.role === "spv" && (
                <div className="card">
                  <div className="flex items-start gap-3 p-3 bg-amber-50 rounded-xl border border-amber-100">
                    <Icon name="information-circle" className="w-4 h-4 text-amber-600 shrink-0 mt-0.5" />
                    <div>
                      <p className="text-sm font-semibold text-amber-800 mb-0.5">Permintaan Ditolak</p>
                      <p className="text-xs text-amber-700 leading-relaxed">
                        Buat pengajuan baru dari halaman yang relevan (Target, dll.) dengan penyesuaian sesuai catatan penolakan.
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
