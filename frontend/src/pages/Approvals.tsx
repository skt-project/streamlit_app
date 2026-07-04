import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
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

function StatusPill({ status }: { status: ApprovalStatus }) {
  if (status === "pending")  return <span className="badge-yellow">Menunggu</span>;
  if (status === "approved") return <span className="badge-green">Disetujui</span>;
  if (status === "rejected") return <span className="badge-red">Ditolak</span>;
  return <span className="badge-gray">Revisi</span>;
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
  });

  const decideMutation = useMutation({
    mutationFn: ({ id, decision }: { id: string; decision: "approve" | "reject" }) =>
      api.post(`/approvals/${id}/${decision}`, { comment }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["approvals"] });
      setSelected(null);
      setComment("");
    },
  });

  const canDecide = user?.role === "asm" || user?.role === "dm" || user?.role === "ho_admin";

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Approvals" />

      <div className="flex flex-1 min-h-0">
        {/* List */}
        <div className="w-80 border-r border-slate-200 bg-white flex flex-col">
          {/* Tabs */}
          <div className="flex border-b border-slate-200">
            {(["pending", "history"] as const).map((t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`flex-1 py-3 text-sm font-medium transition-colors border-b-2 ${
                  tab === t ? "border-primary-600 text-primary-600" : "border-transparent text-slate-500"
                }`}
              >
                {t === "pending" ? "Menunggu" : "Riwayat"}
              </button>
            ))}
          </div>

          {/* Items */}
          <div className="flex-1 overflow-y-auto divide-y divide-slate-50">
            {isLoading ? (
              <p className="p-4 text-sm text-slate-400">Memuat...</p>
            ) : approvals.length === 0 ? (
              <div className="p-6 text-center text-slate-400 text-sm">
                {tab === "pending" ? "Tidak ada permintaan menunggu." : "Belum ada riwayat."}
              </div>
            ) : (
              approvals.map((req) => (
                <button
                  key={req.approval_id}
                  onClick={() => setSelected(req)}
                  className={`w-full text-left p-4 hover:bg-slate-50 transition-colors ${
                    selected?.approval_id === req.approval_id ? "bg-primary-50" : ""
                  }`}
                >
                  <div className="flex items-start justify-between gap-2 mb-1">
                    <p className="text-sm font-medium text-slate-700 line-clamp-2">{req.title}</p>
                    <StatusPill status={req.status} />
                  </div>
                  <div className="flex items-center gap-2 mt-1">
                    <span className="badge-gray text-xs">{TYPE_LABELS[req.type]}</span>
                  </div>
                  <p className="text-xs text-slate-400 mt-1">
                    {req.submitted_by} · {format(new Date(req.submitted_at), "d MMM yyyy")}
                  </p>
                </button>
              ))
            )}
          </div>
        </div>

        {/* Detail */}
        <div className="flex-1 overflow-y-auto bg-slate-50">
          {!selected ? (
            <div className="flex items-center justify-center h-full text-slate-400 text-sm">
              Pilih permintaan di kiri untuk melihat detail
            </div>
          ) : (
            <div className="p-6 max-w-2xl">
              <div className="card space-y-5">
                {/* Header */}
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <span className="badge-blue">{TYPE_LABELS[selected.type]}</span>
                      <StatusPill status={selected.status} />
                    </div>
                    <h2 className="text-lg font-semibold text-slate-800">{selected.title}</h2>
                  </div>
                </div>

                {/* Detail Grid */}
                <div className="grid grid-cols-2 gap-4 bg-slate-50 rounded-xl p-4 text-sm">
                  <div><p className="text-xs text-slate-400">Diajukan Oleh</p><p className="font-medium text-slate-700">{selected.submitted_by}</p></div>
                  <div><p className="text-xs text-slate-400">Tanggal Submit</p><p className="font-medium text-slate-700">{format(new Date(selected.submitted_at), "d MMM yyyy HH:mm")}</p></div>
                  <div><p className="text-xs text-slate-400">Saat Ini</p><p className="font-medium text-slate-700">{String(selected.current_value ?? "—")}</p></div>
                  <div><p className="text-xs text-slate-400">Usulan</p><p className="font-medium text-primary-600">{String(selected.proposed_value)}</p></div>
                </div>

                {/* Reason */}
                <div>
                  <p className="text-xs text-slate-400 mb-1">Alasan</p>
                  <p className="text-sm text-slate-700 bg-slate-50 rounded-lg p-3">{selected.reason}</p>
                </div>

                {/* Comments */}
                {selected.comments.length > 0 && (
                  <div>
                    <p className="text-xs text-slate-400 mb-2">Komentar & Timeline</p>
                    <div className="space-y-2">
                      {selected.comments.map((c, i) => (
                        <div key={i} className="bg-slate-50 rounded-lg p-3">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-xs font-medium text-slate-700">{c.author}</span>
                            <span className="badge-gray text-xs">{c.role}</span>
                            <span className="text-xs text-slate-400 ml-auto">{format(new Date(c.created_at), "d MMM HH:mm")}</span>
                          </div>
                          <p className="text-sm text-slate-600">{c.body}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Actions */}
                {canDecide && selected.status === "pending" && (
                  <div className="border-t border-slate-100 pt-4 space-y-3">
                    <textarea
                      className="input text-sm"
                      placeholder="Komentar (wajib untuk Tolak)..."
                      rows={3}
                      value={comment}
                      onChange={(e) => setComment(e.target.value)}
                    />
                    <div className="flex gap-3">
                      <button
                        className="btn-danger flex-1"
                        onClick={() => decideMutation.mutate({ id: selected.approval_id, decision: "reject" })}
                        disabled={!comment || decideMutation.isPending}
                      >
                        Tolak
                      </button>
                      <button
                        className="btn-primary flex-1"
                        onClick={() => decideMutation.mutate({ id: selected.approval_id, decision: "approve" })}
                        disabled={decideMutation.isPending}
                      >
                        {decideMutation.isPending ? "Memproses..." : "Setujui"}
                      </button>
                    </div>
                  </div>
                )}

                {selected.status === "rejected" && user?.role === "spv" && (
                  <button className="btn-primary w-full">Revisi &amp; Submit Ulang</button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
