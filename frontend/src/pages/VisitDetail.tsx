import { useState, useCallback, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { getVisit, approveVisit, rejectVisit, updateFinalQty, downloadVisitPdf } from "@/api/visit";
import { useAuthStore } from "@/store/authStore";
import type { Visit, VisitApprovalStatus, VisitItem } from "@/types";

// ── helpers ───────────────────────────────────────────────────────────────────

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

function fmtRp(val: number | null | undefined) {
  if (val == null) return "—";
  return `Rp ${val.toLocaleString("id-ID")}`;
}

function canApprove(approvalStatus: string | null, role: string): boolean {
  const map: Record<string, string[]> = {
    spv:               ["PENDING_SPV", "SUBMITTED"],
    distributor_admin: ["SPV_APPROVED"],
  };
  return map[role]?.includes(approvalStatus ?? "") ?? false;
}

function canReject(approvalStatus: string | null, role: string): boolean {
  const rejectableStatuses = ["PENDING_SPV", "SUBMITTED", "SPV_APPROVED"];
  return ["spv", "distributor_admin"].includes(role) &&
    rejectableStatuses.includes(approvalStatus ?? "");
}

function canEditFinalQty(approvalStatus: string | null, role: string): boolean {
  return role === "spv" &&
    ["PENDING_SPV", "SUBMITTED"].includes(approvalStatus ?? "");
}

// ── component ─────────────────────────────────────────────────────────────────

export default function VisitDetail() {
  const { visitId } = useParams<{ visitId: string }>();
  const navigate    = useNavigate();
  const qc          = useQueryClient();
  const user        = useAuthStore((s) => s.user);
  const role        = user?.role ?? "";
  const isDistAdm   = false; // distributor_admin is now a full approver, not read-only

  const [rejectOpen,  setRejectOpen]  = useState(false);
  const [rejectNotes, setRejectNotes] = useState("");
  const [pdfLoading,  setPdfLoading]  = useState(false);

  // Final Qty state — keyed by sku_id
  const [finalQtyMap,  setFinalQtyMap]  = useState<Record<string, number>>({});
  const [fqtyEditing,  setFqtyEditing]  = useState(false);
  const [fqtyDirty,    setFqtyDirty]    = useState(false);

  const { data: visit, isLoading, error } = useQuery<Visit>({
    queryKey: ["visit", visitId],
    queryFn:  () => getVisit(visitId!),
    enabled:  !!visitId,
  });

  // Seed finalQtyMap once visit data arrives (only when not actively editing)
  useEffect(() => {
    if (visit && !fqtyDirty && visit.items.length > 0) {
      const init: Record<string, number> = {};
      for (const it of visit.items) {
        init[it.sku_id] = it.final_qty ?? it.qty ?? 0;
      }
      setFinalQtyMap(init);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [visit?.visit_id]);

  const invalidate = useCallback(
    () => qc.invalidateQueries({ queryKey: ["visit", visitId] }),
    [qc, visitId],
  );

  const approveMut = useMutation({
    mutationFn: () => approveVisit(visitId!),
    onSuccess:  invalidate,
  });

  const rejectMut = useMutation({
    mutationFn: () => rejectVisit(visitId!, rejectNotes),
    onSuccess:  () => { invalidate(); setRejectOpen(false); setRejectNotes(""); },
  });

  const finalQtyMut = useMutation({
    mutationFn: () =>
      updateFinalQty(
        visitId!,
        Object.entries(finalQtyMap).map(([sku_id, final_qty]) => ({ sku_id, final_qty })),
      ),
    onSuccess: () => { setFqtyDirty(false); setFqtyEditing(false); invalidate(); },
  });

  const handlePdfDownload = async () => {
    setPdfLoading(true);
    try { await downloadVisitPdf(visitId!); }
    catch { alert("Gagal mengunduh PDF. Coba lagi."); }
    finally { setPdfLoading(false); }
  };

  // ── Loading / Error ────────────────────────────────────────────────────────

  if (isLoading) {
    return (
      <div className="flex flex-col h-full">
        <TopNav title="Detail Kunjungan" />
        <div className="flex-1 flex items-center justify-center text-slate-400">Memuat data...</div>
      </div>
    );
  }

  if (error || !visit) {
    return (
      <div className="flex flex-col h-full">
        <TopNav title="Detail Kunjungan" />
        <div className="flex-1 flex items-center justify-center text-red-500">Kunjungan tidak ditemukan.</div>
      </div>
    );
  }

  // ── Derived values ─────────────────────────────────────────────────────────

  const totalQty = visit.items.reduce((s, i) => s + (i.qty ?? 0), 0);
  const totalFinalQty = visit.items.reduce((s, i) => s + (finalQtyMap[i.sku_id] ?? i.qty ?? 0), 0);
  const liveFinalDemand = visit.items.reduce(
    (s, i) => s + (finalQtyMap[i.sku_id] ?? i.final_qty ?? i.qty ?? 0) * (i.stp ?? 0),
    0,
  );
  const brandGroups = [...new Set(visit.items.map((i) => i.brand).filter(Boolean))];
  const showFinalQtyCol = canEditFinalQty(visit.approval_status, role) || visit.items.some((i) => i.final_qty != null);
  const canDownloadPdf  = ["spv", "asm", "ddm", "ho_admin", "distributor_admin"].includes(role);

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Detail Kunjungan"
        actions={
          <div className="flex items-center gap-2">
            {canDownloadPdf && (
              <button
                className="btn-secondary text-sm flex items-center gap-1.5"
                onClick={handlePdfDownload}
                disabled={pdfLoading}
              >
                {pdfLoading ? "Mengunduh..." : "⬇ Unduh PDF"}
                {visit.download_count > 0 && (
                  <span className="text-xs text-slate-400">({visit.download_count}×)</span>
                )}
              </button>
            )}
            <button className="btn-secondary text-sm" onClick={() => navigate("/visits")}>
              ← Kembali
            </button>
          </div>
        }
      />

      <main className="flex-1 overflow-y-auto p-6">
        <div className="max-w-5xl mx-auto space-y-6">

          {/* ── Visit header ──────────────────────────────────── */}
          <div className="flex items-start justify-between gap-4 flex-wrap">
            <div>
              <p className="text-xs text-slate-400 font-mono mb-1">{visit.visit_id}</p>
              <h2 className="text-xl font-bold text-slate-800">
                {visit.store_name ?? visit.outlet_sk ?? "Toko Tidak Diketahui"}
              </h2>
              <p className="text-slate-500 mt-0.5">
                <span className="font-medium">{visit.salesman_name ?? visit.salesman_sk}</span>
                {visit.distributor_code && (
                  <span className="text-slate-400"> · {visit.distributor_code}</span>
                )}
                {" · "}{visit.visit_date}
              </p>
            </div>
            <ApprovalBadge status={visit.approval_status} />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

            {/* ── Left: visit info + items ───────────────────── */}
            <div className="lg:col-span-2 space-y-6">

              {/* Visit metadata */}
              <div className="card">
                <h3 className="text-sm font-semibold text-slate-700 mb-4">Info Kunjungan</h3>
                <dl className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm">
                  <div>
                    <dt className="text-slate-400 text-xs">Salesman</dt>
                    <dd className="font-medium text-slate-700">{visit.salesman_name ?? visit.salesman_sk}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs">Toko</dt>
                    <dd className="font-medium text-slate-700">{visit.store_name ?? visit.outlet_sk ?? "—"}</dd>
                  </div>
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
                      {visit.duration_minutes != null ? `${visit.duration_minutes} menit` : "—"}
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
                    <dt className="text-slate-400 text-xs">Total Demand (SE)</dt>
                    <dd className="font-semibold text-primary-700 text-base">{fmtRp(visit.total_demand)}</dd>
                  </div>
                  {(visit.final_demand != null || fqtyDirty) && (
                    <div>
                      <dt className="text-slate-400 text-xs">Total Demand (Final)</dt>
                      <dd className="font-semibold text-green-700 text-base">{fmtRp(liveFinalDemand)}</dd>
                    </div>
                  )}
                  <div>
                    <dt className="text-slate-400 text-xs">Efektif Call</dt>
                    <dd>
                      {visit.effective_call === "YES" ? (
                        <span className="badge-green">YA</span>
                      ) : visit.effective_call === "NO" ? (
                        <span className="badge-gray">TIDAK</span>
                      ) : <span className="text-slate-400">—</span>}
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
                    <p className="text-sm text-red-700 leading-relaxed">{visit.rejection_notes}</p>
                  </div>
                )}
              </div>

              {/* Demand items */}
              <div className="card">
                <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
                  <h3 className="text-sm font-semibold text-slate-700">Detail Demand</h3>
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-slate-400">
                      {visit.items.length} SKU · {totalQty} pcs SE
                      {showFinalQtyCol && ` · ${totalFinalQty} pcs Final`}
                    </span>
                    {canEditFinalQty(visit.approval_status, role) && !fqtyEditing && (
                      <button
                        className="btn-secondary text-xs px-2 py-1"
                        onClick={() => setFqtyEditing(true)}
                      >
                        ✎ Edit Qty Final
                      </button>
                    )}
                    {fqtyEditing && (
                      <div className="flex gap-2">
                        <button
                          className="btn-primary text-xs px-2 py-1"
                          disabled={finalQtyMut.isPending || !fqtyDirty}
                          onClick={() => finalQtyMut.mutate()}
                        >
                          {finalQtyMut.isPending ? "Menyimpan..." : "Simpan"}
                        </button>
                        <button
                          className="btn-secondary text-xs px-2 py-1"
                          onClick={() => {
                            setFqtyEditing(false);
                            setFqtyDirty(false);
                            // Reset to saved values
                            const reset: Record<string, number> = {};
                            for (const it of visit.items) reset[it.sku_id] = it.final_qty ?? it.qty ?? 0;
                            setFinalQtyMap(reset);
                          }}
                        >
                          Batal
                        </button>
                      </div>
                    )}
                  </div>
                </div>

                {visit.items.length === 0 ? (
                  <p className="text-sm text-slate-400 text-center py-6">Tidak ada item demand.</p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-slate-100">
                          <th className="text-left py-2 text-xs font-medium text-slate-400">Kode SKU</th>
                          <th className="text-left py-2 text-xs font-medium text-slate-400">Nama Produk</th>
                          <th className="text-left py-2 text-xs font-medium text-slate-400">Brand</th>
                          <th className="text-left py-2 text-xs font-medium text-slate-400">Ukuran</th>
                          <th className="text-right py-2 text-xs font-medium text-slate-400">Qty SE</th>
                          {showFinalQtyCol && (
                            <th className="text-right py-2 text-xs font-medium text-slate-400">
                              Qty Final {fqtyEditing && <span className="text-yellow-600">✎</span>}
                            </th>
                          )}
                          <th className="text-right py-2 text-xs font-medium text-slate-400">Stok Gudang Dist.</th>
                          <th className="text-right py-2 text-xs font-medium text-slate-400">Demand</th>
                        </tr>
                      </thead>
                      <tbody>
                        {visit.items.map((item: VisitItem) => {
                          const effQty   = finalQtyMap[item.sku_id] ?? item.final_qty ?? item.qty ?? 0;
                          const effDemand = effQty * (item.stp ?? 0);
                          const changed  = fqtyEditing && (finalQtyMap[item.sku_id] ?? item.qty ?? 0) !== (item.qty ?? 0);

                          return (
                            <tr
                              key={item.visit_item_id}
                              className={`border-b border-slate-50 ${changed ? "bg-yellow-50" : ""}`}
                            >
                              <td className="py-2.5 font-mono text-xs text-slate-500 pr-3">{item.sku_id}</td>
                              <td className="py-2.5 font-medium text-slate-700 pr-3">{item.sku_name ?? "—"}</td>
                              <td className="py-2.5 text-slate-500 pr-3">{item.brand ?? "—"}</td>
                              <td className="py-2.5 text-slate-500 pr-3">{item.sku_size ?? "—"}</td>
                              <td className="py-2.5 text-right font-semibold text-slate-600">{item.qty ?? 0}</td>
                              {showFinalQtyCol && (
                                <td className="py-2.5 text-right">
                                  {fqtyEditing ? (
                                    <input
                                      type="number"
                                      min={0}
                                      className="w-20 text-right border border-slate-300 rounded px-1 py-0.5 text-sm font-semibold"
                                      value={finalQtyMap[item.sku_id] ?? item.final_qty ?? item.qty ?? 0}
                                      onChange={(e) => {
                                        const v = Math.max(0, parseInt(e.target.value) || 0);
                                        setFinalQtyMap((m) => ({ ...m, [item.sku_id]: v }));
                                        setFqtyDirty(true);
                                      }}
                                    />
                                  ) : (
                                    <span className={`font-semibold ${item.final_qty != null && item.final_qty !== item.qty ? "text-green-700" : "text-slate-700"}`}>
                                      {effQty}
                                    </span>
                                  )}
                                </td>
                              )}
                              <td className="py-2.5 text-right text-slate-600">
                                {item.warehouse_stock_qty != null ? item.warehouse_stock_qty : "—"}
                              </td>
                              <td className="py-2.5 text-right font-semibold text-primary-700">
                                {fmtRp(effDemand)}
                              </td>
                            </tr>
                          );
                        })}

                        {/* Total row */}
                        <tr className="border-t-2 border-slate-200">
                          <td colSpan={4} className="py-2.5 text-xs font-semibold text-slate-500">
                            TOTAL
                          </td>
                          <td className="py-2.5 text-right font-bold text-slate-800">{totalQty}</td>
                          {showFinalQtyCol && (
                            <td className="py-2.5 text-right font-bold text-green-700">{totalFinalQty}</td>
                          )}
                          <td className="py-2.5 text-right text-slate-400">—</td>
                          <td className="py-2.5 text-right font-bold text-primary-700">
                            {fmtRp(liveFinalDemand)}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                )}

                {finalQtyMut.isError && (
                  <p className="text-xs text-red-600 mt-2">Gagal menyimpan Qty Final. Coba lagi.</p>
                )}

                {/* Brand summary */}
                {brandGroups.length > 1 && (
                  <div className="mt-4 pt-4 border-t border-slate-100">
                    <p className="text-xs text-slate-400 mb-2">Ringkasan per Brand</p>
                    <div className="flex flex-wrap gap-3">
                      {brandGroups.map((brand) => {
                        const brandItems  = visit.items.filter((i) => i.brand === brand);
                        const brandDemand = brandItems.reduce(
                          (s, i) => s + (finalQtyMap[i.sku_id] ?? i.final_qty ?? i.qty ?? 0) * (i.stp ?? 0), 0,
                        );
                        return (
                          <div key={brand} className="bg-slate-50 rounded-lg px-4 py-2 text-sm">
                            <p className="font-semibold text-slate-700">{brand}</p>
                            <p className="text-xs text-slate-500">
                              {brandItems.length} SKU · {fmtRp(brandDemand)}
                            </p>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* ── Right: approval panel ─────────────────────── */}
            <div className="space-y-6">

              {/* Approval timeline */}
              <div className="card">
                <h3 className="text-sm font-semibold text-slate-700 mb-4">Alur Approval</h3>
                <ol className="space-y-4">
                  {[
                    {
                      stage: "SPV",
                      approver: visit.spv_username,
                      approvedAt: visit.spv_approved_at,
                      active: ["PENDING_SPV", "SUBMITTED"].includes(visit.approval_status ?? ""),
                      done:   ["SPV_APPROVED", "COMPLETED"].includes(visit.approval_status ?? ""),
                    },
                    {
                      stage: "Distributor Admin",
                      approver: visit.ddm_username,
                      approvedAt: visit.ddm_approved_at,
                      active: visit.approval_status === "SPV_APPROVED",
                      done:   visit.approval_status === "COMPLETED",
                    },
                  ].map(({ stage, approver, approvedAt, active, done }) => (
                    <li key={stage} className="flex items-start gap-3">
                      <div className={`w-6 h-6 rounded-full flex-shrink-0 flex items-center justify-center text-xs font-bold mt-0.5 ${
                        done ? "bg-green-100 text-green-700" : active ? "bg-yellow-100 text-yellow-700" : "bg-slate-100 text-slate-400"
                      }`}>
                        {done ? "✓" : active ? "●" : "○"}
                      </div>
                      <div>
                        <p className={`text-sm font-medium ${done ? "text-green-700" : active ? "text-yellow-700" : "text-slate-400"}`}>
                          {stage}
                        </p>
                        {approver && <p className="text-xs text-slate-500">{approver}</p>}
                        {approvedAt && <p className="text-xs text-slate-400">{fmt(approvedAt)}</p>}
                        {active && !approver && <p className="text-xs text-yellow-600">Menunggu persetujuan</p>}
                      </div>
                    </li>
                  ))}
                </ol>

                {visit.approval_status === "REVISION_REQUIRED" && (
                  <div className="mt-4 pt-4 border-t border-slate-100 bg-red-50 rounded-lg p-3">
                    <p className="text-xs font-semibold text-red-600 mb-1">Diminta Revisi</p>
                    <p className="text-xs text-red-700">{visit.rejection_notes}</p>
                  </div>
                )}
              </div>

              {/* Approve / Reject actions — hidden for distributor_admin */}
              {!isDistAdm && (canApprove(visit.approval_status, role) || canReject(visit.approval_status, role)) && (
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
                    <p className="text-xs text-red-600">Gagal menyetujui. Coba lagi.</p>
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
                          onClick={() => { setRejectOpen(false); setRejectNotes(""); }}
                        >
                          Batal
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* Distributor Admin — read-only notice */}
              {isDistAdm && (
                <div className="card bg-blue-50 border border-blue-100">
                  <p className="text-sm text-blue-700 font-medium mb-1">Mode Distributor</p>
                  <p className="text-xs text-blue-600">
                    Anda dapat melihat detail demand yang telah disetujui SPV dan mengunduh PDF untuk proses distribusi.
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
