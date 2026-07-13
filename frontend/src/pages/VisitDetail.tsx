import { useRef, useState, useCallback, useEffect, useMemo } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, Skeleton, EmptyState } from "@/components/ui";
import { toast } from "@/store/toastStore";
import { getVisit, approveVisit, rejectVisit, updateFinalQty, updateStorePrice, updateAdjustment, downloadVisitPdf } from "@/api/visit";
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
    spv:      ["PENDING_SPV", "SUBMITTED"],
    asm:      ["SPV_APPROVED"],
    dm:       ["SPV_APPROVED", "ASM_APPROVED"],
    ho_admin: ["PENDING_SPV", "SUBMITTED", "SPV_APPROVED", "ASM_APPROVED"],
  };
  return map[role]?.includes(approvalStatus ?? "") ?? false;
}

function canReject(approvalStatus: string | null, role: string): boolean {
  const rejectableStatuses = ["PENDING_SPV", "SUBMITTED", "SPV_APPROVED", "ASM_APPROVED"];
  return ["spv", "asm", "dm", "ho_admin"].includes(role) &&
    rejectableStatuses.includes(approvalStatus ?? "");
}

function canEditFinalQty(approvalStatus: string | null, role: string): boolean {
  if (role === "spv") return ["PENDING_SPV", "SUBMITTED"].includes(approvalStatus ?? "");
  if (role === "dm" || role === "asm") return approvalStatus === "SPV_APPROVED";
  return false;
}

// ── Loading skeleton ──────────────────────────────────────────────────────────

function VisitDetailSkeleton() {
  return (
    <main className="flex-1 overflow-y-auto p-4 lg:p-8">
      <div className="max-w-5xl mx-auto space-y-6">
        <div className="flex items-start justify-between gap-4">
          <div className="space-y-2">
            <Skeleton className="h-3 w-40" />
            <Skeleton className="h-7 w-64" />
            <Skeleton className="h-4 w-48" />
          </div>
          <Skeleton className="h-6 w-28 rounded-full" />
        </div>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2 space-y-6">
            <div className="card space-y-4">
              <Skeleton className="h-4 w-32" />
              <div className="grid grid-cols-2 gap-4">
                {[1,2,3,4,5,6].map(i => (
                  <div key={i} className="space-y-1.5">
                    <Skeleton className="h-3 w-20" />
                    <Skeleton className="h-4 w-32" />
                  </div>
                ))}
              </div>
            </div>
            <div className="card space-y-3">
              <Skeleton className="h-4 w-28" />
              <Skeleton className="h-48 w-full" />
            </div>
          </div>
          <div className="card space-y-4">
            <Skeleton className="h-4 w-28" />
            {[1,2].map(i => (
              <div key={i} className="flex gap-3">
                <Skeleton className="w-6 h-6 rounded-full shrink-0" />
                <div className="space-y-1.5 flex-1">
                  <Skeleton className="h-4 w-20" />
                  <Skeleton className="h-3 w-32" />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </main>
  );
}

// ── component ─────────────────────────────────────────────────────────────────

export default function VisitDetail() {
  const { visitId } = useParams<{ visitId: string }>();
  const navigate    = useNavigate();
  const qc          = useQueryClient();
  const user        = useAuthStore((s) => s.user);
  const role        = user?.role ?? "";
  const isDistAdm   = role === "dm" || role === "ho_admin";

  const [rejectOpen,  setRejectOpen]  = useState(false);
  const [rejectNotes, setRejectNotes] = useState("");
  const rejectBtnRef = useRef<HTMLButtonElement>(null);
  const [pdfLoading,  setPdfLoading]  = useState(false);

  const [finalQtyMap,  setFinalQtyMap]  = useState<Record<string, number>>({});
  const [priceMap,     setPriceMap]     = useState<Record<string, number>>({});
  const [fqtyEditing,  setFqtyEditing]  = useState(false);
  const [fqtyDirty,    setFqtyDirty]    = useState(false);
  const [priceDirty,   setPriceDirty]   = useState(false);
  const [isSaving,     setIsSaving]     = useState(false);

  const [adjEditing,   setAdjEditing]   = useState(false);
  const [adjAmount,    setAdjAmount]    = useState<number>(0);
  const [adjNote,      setAdjNote]      = useState("");

  const { data: visit, isLoading, error } = useQuery<Visit>({
    queryKey: ["visit", visitId],
    queryFn:  () => getVisit(visitId!),
    enabled:  !!visitId,
    staleTime: 30_000,
    placeholderData: (prev) => prev,
  });

  useEffect(() => {
    if (visit && !fqtyDirty && !priceDirty && visit.items.length > 0) {
      const fqInit: Record<string, number> = {};
      const prInit: Record<string, number> = {};
      for (const it of visit.items) {
        fqInit[it.sku_id] = it.final_qty ?? it.qty ?? 0;
        // Price template: default Harga Toko/PCS from Harga Rekomendasi (STP) when not yet set
        prInit[it.sku_id] = it.price_for_store ?? it.stp ?? 0;
      }
      setFinalQtyMap(fqInit);
      setPriceMap(prInit);
    }
    if (visit) {
      setAdjAmount(visit.adjustment_amount ?? 0);
      setAdjNote(visit.adjustment_note ?? "");
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [visit?.updated_at]);

  const invalidate = useCallback(
    () => qc.invalidateQueries({ queryKey: ["visit", visitId] }),
    [qc, visitId],
  );

  const approveMut = useMutation({
    mutationFn: () => approveVisit(visitId!),
    onSuccess:  () => { invalidate(); toast.success("Kunjungan berhasil disetujui."); },
    onError:    () => toast.error("Gagal menyetujui. Coba lagi."),
  });

  const rejectMut = useMutation({
    mutationFn: () => rejectVisit(visitId!, rejectNotes),
    onSuccess:  () => {
      invalidate();
      setRejectOpen(false);
      setRejectNotes("");
      toast.success("Permintaan revisi dikirim.");
    },
    onError: () => toast.error("Gagal mengirim revisi."),
  });

  const handleSave = async () => {
    setIsSaving(true);
    try {
      await updateFinalQty(
        visitId!,
        Object.entries(finalQtyMap).map(([sku_id, final_qty]) => ({ sku_id, final_qty })),
      );
      if (isDistAdm) {
        const priceItems = Object.entries(priceMap)
          .filter(([, p]) => p > 0)
          .map(([sku_id, price_for_store]) => ({ sku_id, price_for_store }));
        if (priceItems.length > 0) await updateStorePrice(visitId!, priceItems);
      }
      setFqtyDirty(false);
      setPriceDirty(false);
      setFqtyEditing(false);
      invalidate();
      toast.success("Perubahan berhasil disimpan.");
    } catch {
      toast.error("Gagal menyimpan. Coba lagi.");
    } finally {
      setIsSaving(false);
    }
  };

  const handleCancelEdit = () => {
    setFqtyEditing(false);
    setFqtyDirty(false);
    setPriceDirty(false);
    if (visit) {
      const fqReset: Record<string, number> = {};
      const prReset: Record<string, number> = {};
      for (const it of visit.items) {
        fqReset[it.sku_id] = it.final_qty ?? it.qty ?? 0;
        prReset[it.sku_id] = it.price_for_store ?? it.stp ?? 0;
      }
      setFinalQtyMap(fqReset);
      setPriceMap(prReset);
    }
  };

  const adjustMut = useMutation({
    mutationFn: () => updateAdjustment(visitId!, adjAmount || 0, adjNote.trim() || null),
    onSuccess:  () => { invalidate(); setAdjEditing(false); toast.success("Penyesuaian invoice disimpan."); },
    onError:    () => toast.error("Gagal menyimpan penyesuaian."),
  });

  const buildPdfFilename = (): string => {
    const rawStore = visit?.store_name ?? visit?.outlet_sk ?? "Order";
    const store = rawStore.replace(/[^A-Za-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "Order";
    let datePart = "";
    if (visit?.visit_date) {
      const [y, m, d] = visit.visit_date.split("-");
      datePart = d && m && y ? `${d}${m}${y}` : visit.visit_date.replace(/-/g, "");
    }
    return `${store}_${datePart}.pdf`;
  };

  const handlePdfDownload = async () => {
    setPdfLoading(true);
    try {
      await downloadVisitPdf(visitId!, buildPdfFilename());
      toast.success("PDF berhasil diunduh.");
    } catch {
      toast.error("Gagal mengunduh PDF. Coba lagi.");
    } finally {
      setPdfLoading(false);
    }
  };

  // ── Loading / Error ────────────────────────────────────────────────────────

  if (isLoading) {
    return (
      <div className="flex flex-col h-full">
        <TopNav title="Detail Kunjungan" />
        <VisitDetailSkeleton />
      </div>
    );
  }

  if (error || !visit) {
    return (
      <div className="flex flex-col h-full">
        <TopNav title="Detail Kunjungan" />
        <div className="flex-1 flex items-center justify-center">
          <EmptyState
            icon="exclamation-circle"
            title="Kunjungan tidak ditemukan"
            description="Data kunjungan tidak dapat dimuat."
            action={
              <button className="btn-secondary btn-sm" onClick={() => navigate("/visits")}>
                Kembali ke Daftar
              </button>
            }
          />
        </div>
      </div>
    );
  }

  // ── Derived values (memoized — finalQtyMap/priceMap change on each keystroke) ──

  const totalQty = useMemo(
    () => visit.items.reduce((s, i) => s + (i.qty ?? 0), 0),
    [visit.items],
  );

  const totalFinalQty = useMemo(
    () => visit.items.reduce((s, i) => s + (finalQtyMap[i.sku_id] ?? i.qty ?? 0), 0),
    [visit.items, finalQtyMap],
  );

  const liveFinalDemand = useMemo(
    () => visit.items.reduce(
      (s, i) => s + (finalQtyMap[i.sku_id] ?? i.final_qty ?? i.qty ?? 0) * (i.stp ?? 0),
      0,
    ),
    [visit.items, finalQtyMap],
  );

  const grandTotal = useMemo(
    () => visit.items.reduce((s, i) => {
      const qty   = finalQtyMap[i.sku_id] ?? i.final_qty ?? i.qty ?? 0;
      const price = priceMap[i.sku_id] ?? i.price_for_store ?? i.stp ?? 0;
      return s + qty * price;
    }, 0),
    [visit.items, finalQtyMap, priceMap],
  );

  const brandGroups = useMemo(
    () => [...new Set(visit.items.map((i) => i.brand).filter(Boolean))],
    [visit.items],
  );

  const showFinalQtyCol = useMemo(
    () => canEditFinalQty(visit.approval_status, role) || visit.items.some((i) => i.final_qty != null),
    [visit.approval_status, role, visit.items],
  );

  const showPriceCol = useMemo(
    () => isDistAdm || visit.items.some((i) => (i.price_for_store ?? 0) > 0),
    [isDistAdm, visit.items],
  );

  const stockWarningCount = useMemo(
    () => visit.items.filter((i) => {
      const effQty = finalQtyMap[i.sku_id] ?? i.final_qty ?? i.qty ?? 0;
      return i.warehouse_stock_qty != null && effQty > i.warehouse_stock_qty;
    }).length,
    [visit.items, finalQtyMap],
  );

  const canDownloadPdf = ["spv", "asm", "dm", "ho_admin"].includes(role);
  const isDirty        = fqtyDirty || priceDirty;

  // ── Approval timeline steps ────────────────────────────────────────────────

  const timelineSteps = [
    {
      stage: "SPV",
      approver: visit.spv_username,
      approvedAt: visit.spv_approved_at,
      active: ["PENDING_SPV", "SUBMITTED"].includes(visit.approval_status ?? ""),
      done:   ["SPV_APPROVED", "COMPLETED"].includes(visit.approval_status ?? ""),
    },
    {
      stage: "Distributor Manager",
      approver: visit.ddm_username,
      approvedAt: visit.ddm_approved_at,
      active: ["SPV_APPROVED", "ASM_APPROVED"].includes(visit.approval_status ?? ""),
      done:   visit.approval_status === "COMPLETED",
    },
  ];

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Detail Kunjungan"
        subtitle={visit.store_name ?? visit.outlet_sk ?? undefined}
        actions={
          <div className="flex items-center gap-2">
            {canDownloadPdf && (
              <button
                className="btn-secondary btn-sm"
                onClick={handlePdfDownload}
                disabled={pdfLoading}
              >
                {pdfLoading
                  ? <Icon name="arrow-path" className="w-4 h-4 animate-spin" />
                  : <Icon name="arrow-down-tray" className="w-4 h-4" />}
                {pdfLoading ? "Mengunduh…" : "Unduh PDF"}
                {visit.download_count > 0 && (
                  <span className="text-2xs text-slate-400 ml-0.5">({visit.download_count}×)</span>
                )}
              </button>
            )}
            <button className="btn-secondary btn-sm" onClick={() => navigate("/visits")}>
              <Icon name="arrow-left" className="w-4 h-4" />
              Kembali
            </button>
          </div>
        }
      />

      <main className="flex-1 overflow-y-auto p-4 lg:p-8">
        <div className="max-w-5xl mx-auto space-y-6">

          {/* ── Visit header ──────────────────────────────────── */}
          <div className="card">
            <div className="flex items-start justify-between gap-4 flex-wrap">
              <div className="flex items-start gap-4">
                <div className="icon-badge icon-badge-blue icon-badge-lg shrink-0">
                  <Icon name="building-storefront" className="w-5 h-5" />
                </div>
                <div>
                  <p className="text-2xs text-slate-400 font-mono mb-1">{visit.visit_id}</p>
                  <h2 className="text-xl font-bold text-slate-900 leading-tight">
                    {visit.store_name ?? visit.outlet_sk ?? "Toko Tidak Diketahui"}
                  </h2>
                  <p className="text-sm text-slate-500 mt-1">
                    <span className="font-medium text-slate-700">{visit.salesman_name ?? visit.salesman_sk}</span>
                    {visit.distributor_code && <span className="text-slate-400"> · {visit.distributor_code}</span>}
                    <span className="text-slate-400"> · {visit.visit_date}</span>
                  </p>
                </div>
              </div>
              <ApprovalBadge status={visit.approval_status} />
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

            {/* ── Left: info + items ─────────────────────────── */}
            <div className="lg:col-span-2 space-y-6">

              {/* Metadata */}
              <div className="card">
                <div className="section-heading mb-5">
                  <p className="section-heading-title">Info Kunjungan</p>
                </div>
                <dl className="grid grid-cols-2 gap-x-8 gap-y-4 text-sm">
                  <div>
                    <dt className="text-slate-400 text-xs mb-0.5">Check-in</dt>
                    <dd className="font-medium text-slate-700">{fmt(visit.checkin_time)}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs mb-0.5">Check-out</dt>
                    <dd className="font-medium text-slate-700">{fmt(visit.checkout_time)}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs mb-0.5">Durasi</dt>
                    <dd className="font-medium text-slate-700">
                      {visit.duration_minutes != null ? `${visit.duration_minutes} menit` : "—"}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs mb-0.5">Jarak GPS (check-in)</dt>
                    <dd className={`font-medium flex items-center gap-1 ${visit.gps_warning ? "text-amber-600" : "text-slate-700"}`}>
                      {visit.checkin_distance_m != null ? `${Math.round(visit.checkin_distance_m)} m` : "—"}
                      {visit.gps_warning && <Icon name="exclamation-triangle" className="w-3.5 h-3.5 text-amber-500" aria-label="Peringatan GPS" />}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-slate-400 text-xs mb-0.5">Total Order (SE)</dt>
                    <dd className="font-semibold text-primary-700 text-base tabular-nums">{fmtRp(visit.total_demand)}</dd>
                  </div>
                  {(visit.final_demand != null || fqtyDirty) && (
                    <div>
                      <dt className="text-slate-400 text-xs mb-0.5">Total Order (Final)</dt>
                      <dd className="font-semibold text-emerald-700 text-base tabular-nums">{fmtRp(liveFinalDemand)}</dd>
                    </div>
                  )}
                  <div>
                    <dt className="text-slate-400 text-xs mb-0.5">Efektif Call</dt>
                    <dd>
                      {visit.effective_call === "YES" ? (
                        <span className="badge-green">YA</span>
                      ) : visit.effective_call === "NO" ? (
                        <span className="badge-gray">TIDAK</span>
                      ) : <span className="text-slate-400 text-sm">—</span>}
                    </dd>
                  </div>
                  {visit.brand_group && (
                    <div>
                      <dt className="text-slate-400 text-xs mb-0.5">Grup Bisnis</dt>
                      <dd><span className="badge-blue">{visit.brand_group}</span></dd>
                    </div>
                  )}
                  {visit.revision_count != null && visit.revision_count > 0 && (
                    <div>
                      <dt className="text-slate-400 text-xs mb-0.5">Revisi ke-</dt>
                      <dd className="font-medium text-amber-600">{visit.revision_count}</dd>
                    </div>
                  )}
                </dl>

                {visit.notes && (
                  <div className="mt-5 pt-4 border-t border-slate-100">
                    <p className="text-xs text-slate-400 mb-1">Catatan Salesman</p>
                    <p className="text-sm text-slate-700 leading-relaxed">{visit.notes}</p>
                  </div>
                )}
                {visit.rejection_notes && (
                  <div className="mt-5 pt-4 border-t border-slate-100">
                    <div className="rail-red p-3 rounded-xl">
                      <p className="text-xs font-semibold text-red-600 mb-1">Catatan Revisi</p>
                      <p className="text-sm text-red-700 leading-relaxed">{visit.rejection_notes}</p>
                    </div>
                  </div>
                )}
              </div>

              {/* Demand items table */}
              <div className="card">
                <div className="section-heading mb-4">
                  <div>
                    <p className="section-heading-title">Detail Order</p>
                    <p className="section-heading-sub">
                      {visit.items.length} SKU · {totalQty} pcs SE
                      {showFinalQtyCol && ` · ${totalFinalQty} pcs Final`}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    {canEditFinalQty(visit.approval_status, role) && !fqtyEditing && (
                      <button
                        className="btn-secondary btn-sm"
                        onClick={() => setFqtyEditing(true)}
                      >
                        <Icon name="pencil" className="w-3.5 h-3.5" />
                        {isDistAdm ? "Edit Qty & Harga" : "Edit Qty Final"}
                      </button>
                    )}
                    {fqtyEditing && (
                      <div className="flex gap-2">
                        <button
                          className="btn-primary btn-sm"
                          disabled={isSaving || !isDirty}
                          onClick={handleSave}
                        >
                          {isSaving
                            ? <Icon name="arrow-path" className="w-3.5 h-3.5 animate-spin" />
                            : <Icon name="check" className="w-3.5 h-3.5" />}
                          {isSaving ? "Menyimpan…" : "Simpan"}
                        </button>
                        <button className="btn-secondary btn-sm" onClick={handleCancelEdit}>
                          <Icon name="x-mark" className="w-3.5 h-3.5" />
                          Batal
                        </button>
                      </div>
                    )}
                  </div>
                </div>

                {/* Stock warning banner */}
                {stockWarningCount > 0 && (
                  <div className="mb-4 flex items-start gap-3 rounded-xl bg-amber-50 border border-amber-200 px-4 py-3">
                    <Icon name="exclamation-triangle" className="w-4 h-4 text-amber-600 shrink-0 mt-0.5" />
                    <div>
                      <p className="text-sm font-semibold text-amber-800">Peringatan Stok Gudang</p>
                      <p className="text-xs text-amber-700 mt-0.5 leading-relaxed">
                        {stockWarningCount} produk memiliki Qty Final melebihi stok gudang distributor.
                        Persetujuan tetap dapat dilanjutkan.
                      </p>
                    </div>
                  </div>
                )}

                {visit.items.length === 0 ? (
                  <EmptyState icon="inbox" title="Tidak ada item" description="Tidak ada item order untuk kunjungan ini." />
                ) : (
                  <div className="table-container -mx-1">
                    <table className={`table ${showPriceCol ? "min-w-[920px]" : "min-w-[600px]"}`}>
                      <thead>
                        <tr>
                          <th>Kode SKU</th>
                          <th>Produk</th>
                          <th>Brand</th>
                          <th className="text-right">Qty SE</th>
                          {showFinalQtyCol && (
                            <th className="text-right">
                              Qty Final{fqtyEditing && <Icon name="pencil" className="inline w-3 h-3 ml-1 text-primary-400" />}
                            </th>
                          )}
                          <th className="text-right">Stok Gudang</th>
                          <th className="text-right">Harga Rekomendasi</th>
                          {showPriceCol && (
                            <th className="text-right">
                              Harga Toko / PCS{fqtyEditing && isDistAdm && <Icon name="pencil" className="inline w-3 h-3 ml-1 text-emerald-400" />}
                            </th>
                          )}
                          {showPriceCol && <th className="text-right">Total Harga</th>}
                        </tr>
                      </thead>
                      <tbody>
                        {visit.items.map((item: VisitItem) => {
                          const effQty       = finalQtyMap[item.sku_id] ?? item.final_qty ?? item.qty ?? 0;
                          const recoPrice    = item.stp ?? 0;   // Harga Rekomendasi = STP per PCS
                          const priceVal     = priceMap[item.sku_id] ?? item.price_for_store ?? item.stp ?? 0;
                          const totalPrice   = effQty * priceVal;
                          const changed      = fqtyEditing && (finalQtyMap[item.sku_id] ?? item.qty ?? 0) !== (item.qty ?? 0);
                          const hasStockWarn = item.warehouse_stock_qty != null && effQty > item.warehouse_stock_qty;

                          return (
                            <tr
                              key={item.visit_item_id}
                              className={`transition-colors ${
                                hasStockWarn  ? "bg-amber-50 hover:bg-amber-100/60" :
                                changed       ? "bg-primary-50 hover:bg-primary-50" :
                                "hover:bg-slate-50"
                              }`}
                            >
                              <td className="font-mono text-xs text-slate-500">{item.sku_id}</td>
                              <td className="font-medium text-slate-800">{item.sku_name ?? "—"}</td>
                              <td className="text-slate-500">{item.brand ?? "—"}</td>
                              <td className="text-right font-semibold text-slate-600 tabular-nums">{item.qty ?? 0}</td>

                              {showFinalQtyCol && (
                                <td className="text-right">
                                  {fqtyEditing ? (
                                    <input
                                      type="number" min={0}
                                      aria-label={`Qty Final ${item.sku_name ?? item.sku_id}`}
                                      className={`w-20 text-right border rounded px-2 py-1 text-sm font-semibold tabular-nums focus:outline-none focus:ring-2 ${
                                        hasStockWarn
                                          ? "border-amber-400 bg-amber-50 focus:ring-amber-300"
                                          : "border-slate-300 focus:ring-primary-300"
                                      }`}
                                      value={finalQtyMap[item.sku_id] ?? item.final_qty ?? item.qty ?? 0}
                                      onChange={(e) => {
                                        setFinalQtyMap((m) => ({ ...m, [item.sku_id]: Math.max(0, parseInt(e.target.value) || 0) }));
                                        setFqtyDirty(true);
                                      }}
                                    />
                                  ) : (
                                    <span className={`font-semibold tabular-nums ${
                                      item.final_qty != null && item.final_qty !== item.qty
                                        ? "text-primary-600" : "text-slate-700"
                                    }`}>{effQty}</span>
                                  )}
                                  {hasStockWarn && (
                                    <span
                                      className="ml-1.5 inline-flex items-center justify-center w-4 h-4 text-2xs bg-amber-500 text-white rounded-full cursor-help font-bold leading-none"
                                      role="img"
                                      aria-label={`Peringatan: Qty Final (${effQty}) melebihi stok gudang (${item.warehouse_stock_qty} pcs)`}
                                    >!</span>
                                  )}
                                </td>
                              )}

                              <td className={`text-right tabular-nums ${hasStockWarn ? "text-amber-700 font-medium" : "text-slate-600"}`}>
                                {item.warehouse_stock_qty != null ? item.warehouse_stock_qty : "—"}
                              </td>
                              <td className="text-right font-semibold text-slate-600 tabular-nums">{recoPrice > 0 ? fmtRp(recoPrice) : "—"}</td>

                              {showPriceCol && (
                                <td className="text-right">
                                  {fqtyEditing && isDistAdm ? (
                                    <input
                                      type="number" min={0} step="1"
                                      aria-label={`Harga toko per pcs ${item.sku_name ?? item.sku_id}`}
                                      className="w-28 text-right border border-slate-300 rounded px-2 py-1 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-emerald-300"
                                      value={priceMap[item.sku_id] ?? item.price_for_store ?? item.stp ?? ""}
                                      placeholder={recoPrice > 0 ? String(recoPrice) : "0"}
                                      onChange={(e) => {
                                        setPriceMap((m) => ({ ...m, [item.sku_id]: Math.max(0, parseFloat(e.target.value) || 0) }));
                                        setPriceDirty(true);
                                      }}
                                    />
                                  ) : (
                                    <span className="tabular-nums font-semibold text-emerald-700">
                                      {priceVal > 0 ? fmtRp(priceVal) : "—"}
                                    </span>
                                  )}
                                </td>
                              )}

                              {showPriceCol && (
                                <td className="text-right font-bold tabular-nums text-emerald-700">
                                  {priceVal > 0 ? fmtRp(totalPrice) : "—"}
                                </td>
                              )}
                            </tr>
                          );
                        })}

                        {/* Total row */}
                        <tr className="border-t-2 border-slate-200 bg-slate-50/50 font-bold">
                          <td colSpan={3} className="text-xs text-slate-500 uppercase tracking-wide">Total</td>
                          <td className="text-right text-slate-800 tabular-nums">{totalQty}</td>
                          {showFinalQtyCol && <td className="text-right text-primary-600 tabular-nums">{totalFinalQty}</td>}
                          <td className="text-right text-slate-400">—</td>
                          <td className="text-right text-slate-400">—</td>
                          {showPriceCol && <td className="text-right text-slate-400">—</td>}
                          {showPriceCol && (
                            <td className="text-right text-emerald-700 tabular-nums">
                              {grandTotal > 0 ? fmtRp(grandTotal) : "—"}
                            </td>
                          )}
                        </tr>
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Brand summary */}
                {brandGroups.length > 1 && (
                  <div className="mt-5 pt-4 border-t border-slate-100">
                    <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Ringkasan per Brand</p>
                    <div className="flex flex-wrap gap-3">
                      {brandGroups.map((brand) => {
                        const brandItems  = visit.items.filter((i) => i.brand === brand);
                        const brandDemand = brandItems.reduce(
                          (s, i) => s + (finalQtyMap[i.sku_id] ?? i.final_qty ?? i.qty ?? 0) * (i.stp ?? 0), 0,
                        );
                        return (
                          <div key={brand} className="rail-blue p-3 rounded-xl min-w-[120px]">
                            <p className="font-semibold text-slate-800 text-sm">{brand}</p>
                            <p className="text-xs text-slate-500 mt-0.5">{brandItems.length} SKU · {fmtRp(brandDemand)}</p>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>

              {/* ── Invoice adjustment (Distributor Admin) ─────── */}
              {isDistAdm && (
                <div className="card">
                  <div className="section-heading mb-4">
                    <div>
                      <p className="section-heading-title">Penyesuaian Invoice</p>
                      <p className="section-heading-sub">Biaya kirim, diskon, promo, atau biaya lain</p>
                    </div>
                    {!adjEditing && (
                      <button className="btn-secondary btn-sm" onClick={() => setAdjEditing(true)}>
                        <Icon name="pencil" className="w-3.5 h-3.5" />
                        {(visit.adjustment_amount ?? 0) !== 0 ? "Ubah" : "Tambah"}
                      </button>
                    )}
                  </div>

                  <div className="info-grid">
                    <div className="data-row">
                      <span className="data-label">Subtotal</span>
                      <span className="data-value tabular-nums">{fmtRp(grandTotal)}</span>
                    </div>

                    {adjEditing ? (
                      <div className="space-y-3 py-2">
                        <div>
                          <label className="text-xs text-slate-500 mb-1 block">
                            Nominal penyesuaian (Rp) — gunakan minus untuk pengurangan
                          </label>
                          <input
                            type="number"
                            className="input text-sm tabular-nums"
                            value={adjAmount || ""}
                            placeholder="0"
                            aria-label="Nominal penyesuaian"
                            onChange={(e) => setAdjAmount(parseFloat(e.target.value) || 0)}
                          />
                        </div>
                        <div>
                          <label className="text-xs text-slate-500 mb-1 block">Keterangan</label>
                          <input
                            type="text"
                            className="input text-sm"
                            value={adjNote}
                            placeholder="mis. Ongkos kirim / Diskon promo"
                            aria-label="Keterangan penyesuaian"
                            onChange={(e) => setAdjNote(e.target.value)}
                          />
                        </div>
                        <div className="flex gap-2">
                          <button
                            className="btn-primary btn-sm flex-1"
                            disabled={adjustMut.isPending}
                            onClick={() => adjustMut.mutate()}
                          >
                            {adjustMut.isPending ? "Menyimpan…" : "Simpan"}
                          </button>
                          <button
                            className="btn-secondary btn-sm flex-1"
                            onClick={() => {
                              setAdjEditing(false);
                              setAdjAmount(visit.adjustment_amount ?? 0);
                              setAdjNote(visit.adjustment_note ?? "");
                            }}
                          >
                            Batal
                          </button>
                        </div>
                      </div>
                    ) : adjAmount !== 0 ? (
                      <div className="data-row">
                        <span className="data-label">{adjNote || "Adjustment"}</span>
                        <span className={`data-value tabular-nums ${adjAmount > 0 ? "text-amber-600" : "text-red-600"}`}>
                          {adjAmount > 0 ? "+ " : "− "}{fmtRp(Math.abs(adjAmount))}
                        </span>
                      </div>
                    ) : null}

                    <div className="data-row border-t border-slate-200 mt-1 pt-3">
                      <span className="data-label font-semibold text-slate-700">Final Invoice</span>
                      <span className="data-value tabular-nums font-bold text-emerald-700 text-base">
                        {fmtRp(grandTotal + (adjAmount || 0))}
                      </span>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* ── Right: approval panel ─────────────────────── */}
            <div className="space-y-5">

              {/* Timeline */}
              <div className="card">
                <div className="section-heading mb-5">
                  <p className="section-heading-title">Alur Approval</p>
                </div>
                <ol className="space-y-5">
                  {timelineSteps.map(({ stage, approver, approvedAt, active, done }) => (
                    <li key={stage} className="flex items-start gap-3">
                      <div className={`w-7 h-7 rounded-full shrink-0 flex items-center justify-center mt-0.5 ${
                        done ? "bg-emerald-100 text-emerald-700" :
                        active ? "bg-amber-100 text-amber-700" :
                        "bg-slate-100 text-slate-400"
                      }`}>
                        {done
                          ? <Icon name="check" className="w-3.5 h-3.5" />
                          : active
                          ? <Icon name="clock" className="w-3.5 h-3.5" />
                          : <Icon name="minus" className="w-3.5 h-3.5" />}
                      </div>
                      <div>
                        <p className={`text-sm font-semibold ${
                          done ? "text-emerald-700" : active ? "text-amber-700" : "text-slate-400"
                        }`}>{stage}</p>
                        {approver    && <p className="text-xs text-slate-600 mt-0.5">{approver}</p>}
                        {approvedAt  && <p className="text-xs text-slate-400 mt-0.5">{fmt(approvedAt)}</p>}
                        {active && !approver && <p className="text-xs text-amber-600 mt-0.5">Menunggu persetujuan</p>}
                      </div>
                    </li>
                  ))}
                </ol>

                {visit.approval_status === "REVISION_REQUIRED" && (
                  <div className="mt-5 pt-4 border-t border-slate-100">
                    <div className="rail-red p-3 rounded-xl">
                      <p className="text-xs font-semibold text-red-600 mb-1">Diminta Revisi</p>
                      <p className="text-xs text-red-700 leading-relaxed">{visit.rejection_notes}</p>
                    </div>
                  </div>
                )}
              </div>

              {/* Action buttons */}
              {(canApprove(visit.approval_status, role) || canReject(visit.approval_status, role)) && (
                <div className="card space-y-3">
                  <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Tindakan</p>

                  {canApprove(visit.approval_status, role) && (
                    <button
                      className="btn-primary w-full"
                      disabled={approveMut.isPending}
                      onClick={() => approveMut.mutate()}
                    >
                      {approveMut.isPending
                        ? <Icon name="arrow-path" className="w-4 h-4 animate-spin" />
                        : <Icon name="check-circle" className="w-4 h-4" />}
                      {approveMut.isPending ? "Menyetujui…" : "Setujui Kunjungan"}
                    </button>
                  )}

                  {canReject(visit.approval_status, role) && !rejectOpen && (
                    <button
                      ref={rejectBtnRef}
                      className="btn-secondary w-full text-red-600 border-red-200 hover:bg-red-50"
                      onClick={() => setRejectOpen(true)}
                    >
                      <Icon name="arrow-uturn-left" className="w-4 h-4" />
                      Minta Revisi
                    </button>
                  )}

                  {rejectOpen && (
                    <div className="space-y-2">
                      <textarea
                        className="input text-sm resize-none"
                        rows={3}
                        placeholder="Tulis alasan revisi…"
                        aria-label="Alasan revisi"
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
                          {rejectMut.isPending ? "Mengirim…" : "Kirim Revisi"}
                        </button>
                        <button
                          className="btn-secondary flex-1 text-sm"
                          onClick={() => { setRejectOpen(false); setRejectNotes(""); setTimeout(() => rejectBtnRef.current?.focus(), 0); }}
                        >
                          Batal
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* Dist admin: completed notice */}
              {isDistAdm && visit.approval_status === "COMPLETED" && (
                <div className="card rail-green p-4 rounded-xl">
                  <div className="flex items-center gap-2 mb-1">
                    <Icon name="check-circle" className="w-4 h-4 text-emerald-600" />
                    <p className="text-sm font-semibold text-emerald-700">Kunjungan Selesai</p>
                  </div>
                  <p className="text-xs text-emerald-600 leading-relaxed">
                    Kunjungan telah disetujui penuh. Unduh PDF untuk keperluan distribusi.
                  </p>
                </div>
              )}

              {/* Dist admin: awaiting SPV */}
              {isDistAdm && !["SPV_APPROVED", "COMPLETED"].includes(visit.approval_status ?? "") && (
                <div className="card bg-slate-50 border border-slate-100">
                  <div className="flex items-center gap-2 mb-1">
                    <Icon name="clock" className="w-4 h-4 text-slate-400" />
                    <p className="text-sm font-semibold text-slate-600">Menunggu SPV</p>
                  </div>
                  <p className="text-xs text-slate-500 leading-relaxed">
                    Kunjungan ini belum disetujui SPV. Tindakan tersedia setelah status menjadi Disetujui SPV.
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
