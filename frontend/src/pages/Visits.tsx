import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import TopNav from "@/components/layout/TopNav";
import { listVisits } from "@/api/visit";
import { useAuthStore } from "@/store/authStore";
import type { Visit, VisitApprovalStatus } from "@/types";

const APPROVAL_STATUS_MAP: Record<VisitApprovalStatus, { label: string; cls: string }> = {
  DRAFT:             { label: "Draft",         cls: "badge-gray"   },
  SUBMITTED:         { label: "Submitted",     cls: "badge-yellow" },
  PENDING_SPV:       { label: "Menunggu SPV",  cls: "badge-yellow" },
  SPV_APPROVED:      { label: "SPV ✓",         cls: "badge-blue"   },
  ASM_APPROVED:      { label: "ASM ✓",         cls: "badge-blue"   },
  DDM_APPROVED:      { label: "DDM ✓",         cls: "badge-blue"   },
  REVISION_REQUIRED: { label: "Revisi",        cls: "badge-red"    },
  COMPLETED:         { label: "Selesai",       cls: "badge-green"  },
  REJECTED:          { label: "Ditolak",       cls: "badge-red"    },
};

function ApprovalBadge({ status }: { status: string | null }) {
  const s = (status ?? "DRAFT") as VisitApprovalStatus;
  const { label, cls } = APPROVAL_STATUS_MAP[s] ?? { label: s, cls: "badge-gray" };
  return <span className={cls}>{label}</span>;
}

type TabKey = "waiting" | "all";

const TAB_CONFIG: { key: TabKey; label: string; status?: string }[] = [
  { key: "waiting", label: "Menunggu SPV", status: "PENDING_SPV" },
  { key: "all",     label: "Semua Kunjungan" },
];

export default function Visits() {
  const navigate  = useNavigate();
  const user      = useAuthStore((s) => s.user);
  const isDistAdm = user?.role === "distributor_admin";

  const [tab,         setTab]         = useState<TabKey>(isDistAdm ? "all" : "waiting");
  const [dateFilter,  setDateFilter]  = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [storeSearch, setStoreSearch] = useState("");
  const [page,        setPage]        = useState(1);

  // Resolve active status filter: tab preset wins, unless user picked a specific status
  const activeStatus = tab === "waiting" && !statusFilter
    ? "PENDING_SPV"
    : statusFilter || undefined;

  const { data, isLoading, isFetching } = useQuery({
    queryKey: ["visits-list", tab, dateFilter, statusFilter, storeSearch, page],
    queryFn: () =>
      listVisits({
        visit_date:  dateFilter   || undefined,
        status:      activeStatus,
        store_name:  storeSearch  || undefined,
        page,
        page_size:   50,
      }),
    staleTime: 30_000,
    placeholderData: (prev) => prev,
  });

  const visits     = data?.items ?? [];
  const totalPages = data ? Math.ceil(data.total / 50) : 1;

  const resetFilters = () => {
    setDateFilter("");
    setStatusFilter("");
    setStoreSearch("");
    setPage(1);
  };

  const hasFilters = dateFilter || statusFilter || storeSearch;

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Visit & Demand" />

      <main className="flex-1 overflow-y-auto p-6 space-y-4">

        {/* ── Tabs ───────────────────────────────────────────────── */}
        {!isDistAdm && (
          <div className="flex border-b border-slate-200">
            {TAB_CONFIG.map(({ key, label }) => (
              <button
                key={key}
                onClick={() => { setTab(key); setStatusFilter(""); setPage(1); }}
                className={`px-5 py-3 text-sm font-medium border-b-2 transition-colors ${
                  tab === key
                    ? "border-primary-600 text-primary-600"
                    : "border-transparent text-slate-500 hover:text-slate-700"
                }`}
              >
                {label}
                {key === "waiting" && data && tab === "waiting" && data.total > 0 && (
                  <span className="ml-2 bg-yellow-100 text-yellow-700 text-xs font-bold px-1.5 py-0.5 rounded-full">
                    {data.total}
                  </span>
                )}
              </button>
            ))}
          </div>
        )}

        {/* ── Filters ────────────────────────────────────────────── */}
        <div className="flex gap-3 flex-wrap items-center">
          <input
            type="date"
            className="input w-44 text-sm"
            value={dateFilter}
            onChange={(e) => { setDateFilter(e.target.value); setPage(1); }}
          />

          <input
            type="text"
            className="input w-52 text-sm"
            placeholder="Cari nama toko..."
            value={storeSearch}
            onChange={(e) => { setStoreSearch(e.target.value); setPage(1); }}
          />

          {tab === "all" && (
            <select
              className="input w-52 text-sm"
              value={statusFilter}
              onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
            >
              <option value="">Semua Status Approval</option>
              <option value="PENDING_SPV">Menunggu SPV</option>
              <option value="SPV_APPROVED">SPV Approved</option>
              <option value="ASM_APPROVED">ASM Approved</option>
              <option value="DDM_APPROVED">DDM Approved</option>
              <option value="REVISION_REQUIRED">Perlu Revisi</option>
              <option value="COMPLETED">Selesai</option>
              <option value="REJECTED">Ditolak</option>
            </select>
          )}

          {hasFilters && (
            <button
              className="text-xs text-slate-400 hover:text-slate-600 underline"
              onClick={resetFilters}
            >
              Reset filter
            </button>
          )}

          <div className="ml-auto flex items-center gap-2">
            {isFetching && (
              <span className="text-slate-400 text-xs animate-pulse">●</span>
            )}
            <span className="text-xs text-slate-400">
              {data ? `${data.total} kunjungan` : ""}
            </span>
          </div>
        </div>

        {/* ── Table ──────────────────────────────────────────────── */}
        <div className="card overflow-x-auto">
          {isLoading ? (
            <p className="text-sm text-slate-400 p-4">Memuat data...</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  <th className="text-left py-2 text-xs font-medium text-slate-400 pr-4">Tanggal</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400 pr-4">Salesman</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400 pr-4">Toko</th>
                  <th className="text-right py-2 text-xs font-medium text-slate-400 pr-4">Total Demand</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400 pr-4">EC</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400 pr-4">Durasi</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400">Status Approval</th>
                  <th className="py-2" />
                </tr>
              </thead>
              <tbody>
                {visits.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="py-8 text-center text-slate-400">
                      {tab === "waiting"
                        ? "Tidak ada kunjungan menunggu persetujuan."
                        : "Tidak ada data kunjungan."}
                    </td>
                  </tr>
                ) : (
                  visits.map((v: Visit) => (
                    <tr
                      key={v.visit_id}
                      className="border-b border-slate-50 hover:bg-slate-50 cursor-pointer"
                      onClick={() => navigate(`/visits/${v.visit_id}`)}
                    >
                      <td className="py-3 text-slate-600 pr-4">{v.visit_date}</td>
                      <td className="py-3 font-medium text-slate-700 pr-4">
                        {v.salesman_name ?? v.salesman_sk}
                      </td>
                      <td className="py-3 text-slate-600 pr-4">
                        {v.store_name ?? v.outlet_sk ?? "—"}
                      </td>
                      <td className="py-3 text-right font-medium text-slate-700 pr-4">
                        {v.total_demand != null
                          ? `Rp ${v.total_demand.toLocaleString("id-ID")}`
                          : "—"}
                      </td>
                      <td className="py-3 pr-4">
                        {v.effective_call === "YES" ? (
                          <span className="badge-green">Efektif</span>
                        ) : v.effective_call === "NO" ? (
                          <span className="badge-gray">Tidak</span>
                        ) : (
                          <span className="text-slate-400">—</span>
                        )}
                      </td>
                      <td className="py-3 text-slate-500 pr-4">
                        {v.duration_minutes != null ? `${v.duration_minutes} mnt` : "—"}
                      </td>
                      <td className="py-3">
                        <ApprovalBadge status={v.approval_status} />
                      </td>
                      <td className="py-3 pl-4">
                        <span className="text-xs text-primary-600 hover:underline">Detail →</span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          )}
        </div>

        {/* ── Pagination ─────────────────────────────────────────── */}
        {data && data.total > 50 && (
          <div className="flex justify-center items-center gap-3">
            <button
              className="btn-secondary text-sm"
              disabled={page === 1}
              onClick={() => setPage((p) => p - 1)}
            >
              ← Sebelumnya
            </button>
            <span className="text-sm text-slate-500">Hal. {page} / {totalPages}</span>
            <button
              className="btn-secondary text-sm"
              disabled={!data.has_next}
              onClick={() => setPage((p) => p + 1)}
            >
              Berikutnya →
            </button>
          </div>
        )}
      </main>
    </div>
  );
}
