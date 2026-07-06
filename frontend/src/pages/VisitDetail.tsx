import { useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { getVisit, approveVisit, rejectVisit } from "@/api/visit";
import { useAuthStore } from "@/store/authStore";
import type { Visit, VisitApprovalStatus } from "@/types";

// ── helpers ──────────────────────────────────────────────────────────────────

const APPROVAL_LABELS: Record<VisitApprovalStatus, { label: string; cls: string }> = {
  DRAFT:             { label: "Draft",           cls: "badge-gray"   },
  SUBMITTED:         { label: "Submitted",       cls: "badge-yellow" },
  PENDING_SPV:       { label: "Menunggu SPV",    cls: "badge-yellow" },
  SPV_APPROVED:      { label: "Disetujui SPV",   cls: "badge-blue"   },
  ASM_APPROVED:      { label: "Disetujui ASM",   cls: "badge-blue"   },
  DDM_APPROVED:      { label: "Disetujui DDM",   cls: "badge-blue"   },
  REVISION_REQUIRED: { label: "Perlu Revisi",    cls: "badge-red"    },
  COMPLETED:         { label: "Selesai",         cls: "badge-green"  },
  REJECTED:          { label: "Ditolak",         cls: "badge-red"    },
};

function ApprovalBadge({ status }: { status: string | null }) {
  const s = (status ?? "DRAFT") as VisitApprovalStatus;
  const { label, cls } = APPROVAL_LABELS[s] ?? { label: s, cls: "badge-gray" };
  return <span className={`${cls} text-sm`}>{label}</span>;
}

function fmt(dt: string | null) {
  if (!dt) return "—";
  return new Date(dt).toLocaleString("id-ID", {
    day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit",
  });
}

function canApprove(approvalStatus: string | null, role: string): boolean {
  const map: Record<string, string[]> = {
    spv:      ["PENDING_SPV", "SUBMITTED"],
    asm:      ["SPV_APPROVED"],
    ddm:      ["ASM_APPROVED"],
    ho_admin: ["DDM_APPROVED"],
  };
  return map[role]?.includes(approvalStatus ?? "") ?? false;
}

function canReject(approvalStatus: string | null, role: string): boolean {
  const rejectableStatuses = [
    "PENDING_SPV", "SUBMITTED", "SPV_APPROVED", "ASM_APPROVED", "DDM_APPROVED",
  ];
  const canApproveRoles = ["spv", "asm", "ddm", "ho_admin"];
  return canApproveRoles.includes(role) && rejectableStatuses.includes(approvalStatus ?? "");
}

// ── component ─────────────────────────────────────────────────────────────────

export default function VisitDetail() {
  const { visitId } = useParams<{ visitId: string }>();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const user = useAuthStore((s) => s.user);
  const role = user?.role ?? "";

  const [rejectOpen, setRejectOpen] = useState(false);
  const [rejectNotes, setRejectNotes] = useState("");

  const { data: visit, isLoading, error } = useQuery<Visit>({
    queryKey: ["visit", visitId],
    queryFn: () => getVisit(visitId!),
    enabled: !!visitId,
  });

  const invalidate = () => qc.invalidateQueries({ queryKey: ["visit", visitId] });

  const approveMut = useMutation({
    mutationFn: () => approveVisit(visitId!),
    onSuccess: invalidate,
  });

  const rejectMut = useMutation({
    mutationFn: () => rejectVisit(visitId!, rejectNotes),
    onSuccess: () => { invalidate(); setRejectOpen(false); setRejectNotes(""); },
  });

  if (isLoading) {
    return (
      <div className="flex flex-col h-full">
        <TopNav title="Detail Kunjungan" />
        <div className="flex-1 flex items-center justify-center text-slate-400">
          Memuat data...
        </div>
      </div>
    );
  }

  if (error || !visit) {
    return (
      <div className="flex flex-col h-full">
        <TopNav title="Detail Kunjungan" />
        <div className="flex-1 flex items-center justify-center text-red-500">
          Kunjungan tidak ditemukan.
        </div>
      </div>
    );
  }

  const totalQty = visit.items.reduce((s, i) => s + (i.qty ?? 0), 0);
  const brandGroups = [...new Set(visit.items.map((i) => i.brand).filter(Boolean))];

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Detail Kunjungan"
        actions={
          <button
            className="btn-secondary text-sm"
            onClick={() => navigate("/visits")}
          >
            ← Kembali
          </button>
        }
      />

      <main className="flex-1 overflow-y-auto p-6">
        <div className="max-w-5xl mx-auto space-y-6">

          {/* ── Visit header ──────────────────────────────────────────────── */}
          <div className="flex items-start justify-between gap-4 flex-wrap">
            <div>
              <p className="text-xs text-slate-400 font-mono mb-1">{visit.visit_id}</p>
              <h2 className="text-xl font-bold text-slate-800">
                {visit.store_name ?? visit.outlet_sk ?? "Toko Tidak Diketahui"}
              </h2>
              <p className="text-slate-500 mt-0.5">
                {visit.salesman_name ?? visit.salesman_sk} · {visit.visit_date}
              </p>
            </div>
            <ApprovalBadge status={visit.approval_status} />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

            {/* ── Left: visit info + items ───────────────────────────────── */}
            <div className="lg:col-span-2 space-y-6">

              {/* Visit metadata */}
              <div className="card">
                <h3 className="text-sm font-semibold text-slate-700 mb-4">
                  Info Kunjungan
                </h3>
                <dl className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm">
                  <div>
                    <dt className="text-slate-400 text-xs">Check-in</dt>
                    <dd className="font-medium text-slate-700">{fmt(visit.checkin_time)}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs">Check-out</dt>
                    <dd className="font-medium text-slate-700">{fmt(visit.checkout_time)}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs">Durasi</dt>
                    <dd className="font-medium text-slate-700">
                      {visit.duration_minutes != null
                        ? `${visit.duration_minutes} menit`
                        : "—"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs">Jarak GPS (check-in)</dt>
                    <dd className={`font-medium ${visit.gps_warning ? "text-amber-600" : "text-slate-700"}`}>
                      {visit.checkin_distance_m != null
                        ? `${Math.round(visit.checkin_distance_m)} m${visit.gps_warning ? " ⚠" : ""}`
                        : "—"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs">Total Demand</dt>
                    <dd className="font-semibold text-primary-700 text-base">
                      {visit.total_demand != null
                        ? `Rp ${visit.total_demand.toLocaleString("id-ID")}`
                        : "—"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs">Efektif Call</dt>
                    <dd>
                      {visit.effective_call === "YES" ? (
                        <span className="badge-green">YA</span>
                      ) : visit.effective_call === "NO" ? (
                        <span className="badge-gray">TIDAK</span>
                      ) : (
                        <span className="text-slate-400">—</span>
                      )}
                    </dd>
                  </div>
                  {visit.brand_group && (
                    <div>
                      <dt className="text-slate-400 text-xs">Grup Bisnis</dt>
                      <dd><span className="badge-blue">{visit.brand_group}</span></dd>
                    </div>
                  )}
                  {visit.revision_count != null && visit.revision_count > 0 && (
                    <div>
                      <dt className="text-slate-400 text-xs">Revisi ke-</dt>
                      <dd className="font-medium text-amber-600">{visit.revision_count}</dd>
                    </div>
                  )}
                </dl>
                {visit.notes && (
                  <div className="mt-4 pt-4 border-t border-slate-100">
                    <p className="text-xs text-slate-400 mb-1">Catatan Salesman</p>
                    <p className="text-sm text-slate-700 leading-relaxed">{visit.notes}</p>
                  </div>
                )}
                {visit.rejection_notes && (
                  <div className="mt-4 pt-4 border-t border-slate-100">
                    <p className="text-xs text-red-500 mb-1">Catatan Revisi</p>
                    <p className="text-sm text-red-700 leading-relaxed">
                      {visit.rejection_notes}
                    </p>
                  </div>
                )}
              </div>

              {/* Demand items */}
              <div className="card">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-sm font-semibold text-slate-700">
                    Detail Demand
                  </h3>
                  <span className="text-xs text-slate-400">
                    {visit.items.length} SKU · {totalQty} pcs
                  </span>
                </div>

                {visit.items.length === 0 ? (
                  <p className="text-sm text-slate-400 text-center py-6">
                    Tidak ada item demand.
                  </p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-slate-100">
                          <th className="text-left py-2 text-xs font-medium text-slate-400">
                            Kode SKU
                          </th>
                          <th className="text-left py-2 text-xs font-medium text-slate-400">
                            Nama Produk
                          </th>
                          <th className="text-left py-2 text-xs font-medium text-slate-400">
                            Brand
                          </th>
                          <th className="text-left py-2 text-xs font-medium text-slate-400">
                            Kategori
                          </th>
                          <th className="text-right py-2 text-xs font-medium text-slate-400">
                            Qty
                          </th>
                          <th className="text-right py-2 text-xs font-medium text-slate-400">
                            Demand
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {visit.items.map((item) => (
                          <tr
                            key={item.visit_item_id}
                            className="border-b border-slate-50"
                          >
                            <td className="py-2.5 font-mono text-xs text-slate-500 pr-3">
                              {item.sku_id}
                            </td>
                            <td className="py-2.5 font-medium text-slate-700 pr-3">
                              {item.sku_name ?? "—"}
                            </td>
                            <td className="py-2.5 text-slate-500 pr-3">
                              {item.brand ?? "—"}
                            </td>
                            <td className="py-2.5 text-slate-500 pr-3">
                              {item.category ?? "—"}
                            </td>
                            <td className="py-2.5 text-right font-semibold text-slate-700">
                              {item.qty ?? 0}
                            </td>
                            <td className="py-2.5 text-right font-semibold text-primary-700">
                              {item.demand != null
                                ? `Rp ${item.demand.toLocaleString("id-ID")}`
                                : "—"}
                            </td>
                          </tr>
                        ))}
                        {/* Total row */}
                        <tr className="border-t-2 border-slate-200">
                          <td colSpan={4} className="py-2.5 text-xs font-semibold text-slate-500">
                            TOTAL
                          </td>
                          <td className="py-2.5 text-right font-bold text-slate-800">
                            {totalQty}
                          </td>
                          <td className="py-2.5 text-right font-bold text-primary-700">
                            {visit.total_demand != null
                              ? `Rp ${visit.total_demand.toLocaleString("id-ID")}`
                              : "—"}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Brand summary */}
                {brandGroups.length > 1 && (
                  <div className="mt-4 pt-4 border-t border-slate-100">
                    <p className="text-xs text-slate-400 mb-2">Ringkasan per Brand</p>
                    <div className="flex flex-wrap gap-3">
                      {brandGroups.map((brand) => {
                        const brandItems = visit.items.filter((i) => i.brand === brand);
                        const brandDemand = brandItems.reduce(
                          (s, i) => s + (i.demand ?? 0),
                          0,
                        );
                        return (
                          <div
                            key={brand}
                            className="bg-slate-50 rounded-lg px-4 py-2 text-sm"
                          >
                            <p className="font-semibold text-slate-700">{brand}</p>
                            <p className="text-xs text-slate-500">
                              {brandItems.length} SKU ·{" "}
                              Rp {brandDemand.toLocaleString("id-ID")}
                            </p>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* ── Right: approval panel ──────────────────────────────────── */}
            <div className="space-y-6">

              {/* Approval timeline */}
              <div className="card">
                <h3 className="text-sm font-semibold text-slate-700 mb-4">
                  Alur Approval
                </h3>
                <ol className="space-y-4">
                  {[
                    {
                      stage: "SPV",
                      approver: visit.spv_username,
                      approvedAt: visit.spv_approved_at,
                      active: visit.approval_status === "PENDING_SPV",
                      done: ["SPV_APPROVED","ASM_APPROVED","DDM_APPROVED","COMPLETED"]
                        .includes(visit.approval_status ?? ""),
                    },
                    {
                      stage: "ASM",
                      approver: visit.asm_username,
                      approvedAt: visit.asm_approved_at,
                      active: visit.approval_status === "SPV_APPROVED",
                      done: ["ASM_APPROVED","DDM_APPROVED","COMPLETED"]
                        .includes(visit.approval_status ?? ""),
                    },
                    {
                      stage: "DDM",
                      approver: visit.ddm_username,
                      approvedAt: visit.ddm_approved_at,
                      active: ["ASM_APPROVED","DDM_APPROVED"].includes(
                        visit.approval_status ?? "",
                      ),
                      done: ["DDM_APPROVED","COMPLETED"].includes(
                        visit.approval_status ?? "",
                      ),
                    },
                    {
                      stage: "Final",
                      approver: null,
                      approvedAt: null,
                      active: visit.approval_status === "DDM_APPROVED",
                      done: visit.approval_status === "COMPLETED",
                    },
                  ].map(({ stage, approver, approvedAt, active, done }) => (
                    <li key={stage} className="flex items-start gap-3">
                      <div
                        className={`w-6 h-6 rounded-full flex-shrink-0 flex items-center justify-center text-xs font-bold mt-0.5 ${
                          done
                            ? "bg-green-100 text-green-700"
                            : active
                            ? "bg-yellow-100 text-yellow-700"
                            : "bg-slate-100 text-slate-400"
                        }`}
                      >
                        {done ? "✓" : active ? "●" : "○"}
                      </div>
                      <div>
                        <p className={`text-sm font-medium ${done ? "text-green-700" : active ? "text-yellow-700" : "text-slate-400"}`}>
                          {stage}
                        </p>
                        {approver && (
                          <p className="text-xs text-slate-500">{approver}</p>
                        )}
                        {approvedAt && (
                          <p className="text-xs text-slate-400">{fmt(approvedAt)}</p>
                        )}
                        {active && !approver && (
                          <p className="text-xs text-yellow-600">Menunggu persetujuan</p>
                        )}
                      </div>
                    </li>
                  ))}
                </ol>

                {visit.approval_status === "REVISION_REQUIRED" && (
                  <div className="mt-4 pt-4 border-t border-slate-100 bg-red-50 rounded-lg p-3">
                    <p className="text-xs font-semibold text-red-600 mb-1">
                      Diminta Revisi
                    </p>
                    <p className="text-xs text-red-700">{visit.rejection_notes}</p>
                  </div>
                )}
              </div>

              {/* Approve / Reject actions */}
              {(canApprove(visit.approval_status, role) ||
                canReject(visit.approval_status, role)) && (
                <div className="card space-y-3">
                  <h3 className="text-sm font-semibold text-slate-700">Tindakan</h3>

                  {canApprove(visit.approval_status, role) && (
                    <button
                      className="btn-primary w-full"
                      disabled={approveMut.isPending}
                      onClick={() => approveMut.mutate()}
                    >
                      {approveMut.isPending ? "Menyetujui..." : "✓ Setujui Kunjungan"}
                    </button>
                  )}

                  {approveMut.isError && (
                    <p className="text-xs text-red-600">
                      Gagal menyetujui. Coba lagi.
                    </p>
                  )}

                  {canReject(visit.approval_status, role) && !rejectOpen && (
                    <button
                      className="btn-secondary w-full text-red-600 border-red-200 hover:bg-red-50"
                      onClick={() => setRejectOpen(true)}
                    >
                      ✗ Minta Revisi
                    </button>
                  )}

                  {rejectOpen && (
                    <div className="space-y-2">
                      <textarea
                        className="input text-sm resize-none"
                        rows={3}
                        placeholder="Tulis alasan revisi..."
                        value={rejectNotes}
                        onChange={(e) => setRejectNotes(e.target.value)}
                        autoFocus
                      />
                      <div className="flex gap-2">
                        <button
                          className="btn-danger flex-1 text-sm"
                          disabled={!rejectNotes.trim() || rejectMut.isPending}
                          onClick={() => rejectMut.mutate()}
                        >
                          {rejectMut.isPending ? "Mengirim..." : "Kirim Revisi"}
                        </button>
                        <button
                          className="btn-secondary flex-1 text-sm"
                          onClick={() => {
                            setRejectOpen(false);
                            setRejectNotes("");
                          }}
                        >
                          Batal
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
