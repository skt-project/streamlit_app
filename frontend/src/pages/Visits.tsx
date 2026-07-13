import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import TopNav from "@/components/layout/TopNav";
import { Icon, SkeletonTable, EmptyState } from "@/components/ui";
import { listVisits } from "@/api/visit";
import { useDebounce } from "@/hooks/useDebounce";
import type { Visit, VisitApprovalStatus } from "@/types";

const APPROVAL_STATUS_MAP: Record<VisitApprovalStatus, { label: string; cls: string }> = {
  DRAFT:             { label: "Draft",         cls: "badge-gray"   },
  SUBMITTED:         { label: "Submitted",     cls: "badge-yellow" },
  PENDING_SPV:       { label: "Menunggu SPV",  cls: "badge-yellow" },
  SPV_APPROVED:      { label: "SPV Approved",  cls: "badge-blue"   },
  ASM_APPROVED:      { label: "ASM Approved",  cls: "badge-blue"   },
  DDM_APPROVED:      { label: "DDM Approved",  cls: "badge-blue"   },
  REVISION_REQUIRED: { label: "Perlu Revisi",  cls: "badge-red"    },
  COMPLETED:         { label: "Selesai",       cls: "badge-green"  },
  REJECTED:          { label: "Ditolak",       cls: "badge-red"    },
};

function ApprovalBadge({ status }: { status: string | null }) {
  const s = (status ?? "DRAFT") as VisitApprovalStatus;
  const { label, cls } = APPROVAL_STATUS_MAP[s] ?? { label: s, cls: "badge-gray" };
  return <span className={cls}>{label}</span>;
}

type TabKey = "waiting" | "all";

const TAB_CONFIG: { key: TabKey; label: string }[] = [
  { key: "waiting", label: "Menunggu SPV" },
  { key: "all",     label: "Semua Kunjungan" },
];

export default function Visits() {
  const navigate  = useNavigate();
  const [tab,          setTab]          = useState<TabKey>("waiting");
  const [dateFilter,   setDateFilter]   = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [storeSearch,  setStoreSearch]  = useState("");
  const [page,         setPage]         = useState(1);
  const debouncedStoreSearch = useDebounce(storeSearch, 350);

  const activeStatus = tab === "waiting" && !statusFilter
    ? "PENDING_SPV"
    : statusFilter || undefined;

  const { data, isLoading, isFetching } = useQuery({
    queryKey: ["visits-list", tab, dateFilter, statusFilter, debouncedStoreSearch, page],
    queryFn: () =>
      listVisits({
        visit_date:  dateFilter              || undefined,
        status:      activeStatus,
        store_name:  debouncedStoreSearch    || undefined,
        page,
        page_size:   50,
      }),
    staleTime: 30_000,
    placeholderData: (prev) => prev,
  });

  const visits     = data?.items ?? [];
  const totalPages = data ? Math.ceil(data.total / 50) : 1;

  const resetFilters = () => {
    setDateFilter(""); setStatusFilter(""); setStoreSearch(""); setPage(1);
  };
  const hasFilters = dateFilter || statusFilter || storeSearch;

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Visit & Order" />

      <main className="flex-1 overflow-y-auto">
        {/* ── Tabs ── */}
        <div className="tabs px-6" role="tablist" aria-label="Filter kunjungan">
          {TAB_CONFIG.map(({ key, label }) => (
            <button
              key={key}
              role="tab"
              aria-selected={tab === key}
              onClick={() => { setTab(key); setStatusFilter(""); setPage(1); }}
              className={`tab ${tab === key ? "tab-active" : ""}`}
            >
              {label}
              {key === "waiting" && data && tab === "waiting" && data.total > 0 && (
                <span className="ml-2 badge-yellow text-2xs">
                  {data.total}
                </span>
              )}
            </button>
          ))}
        </div>

        {/* ── Filters ── */}
        <div className="filter-bar">
          <div className="search-bar w-52">
            <Icon name="search" />
            <input
              type="text"
              placeholder="Cari nama toko..."
              aria-label="Cari nama toko"
              value={storeSearch}
              onChange={(e) => { setStoreSearch(e.target.value); setPage(1); }}
            />
          </div>

          <input
            type="date"
            className="input w-44"
            aria-label="Filter tanggal kunjungan"
            value={dateFilter}
            onChange={(e) => { setDateFilter(e.target.value); setPage(1); }}
          />

          {tab === "all" && (
            <select
              className="input w-52"
              aria-label="Filter status kunjungan"
              value={statusFilter}
              onChange={(e) => { setStatusFilter(e.target.value); setPage(1); }}
            >
              <option value="">Semua Status</option>
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
              className="btn-ghost btn-sm text-slate-400"
              onClick={resetFilters}
            >
              <Icon name="x-mark" className="w-3.5 h-3.5" />
              Reset
            </button>
          )}

          <div className="ml-auto flex items-center gap-2">
            {isFetching && (
              <Icon name="arrow-path" className="w-4 h-4 text-slate-400 animate-spin" />
            )}
            <span className="text-xs text-slate-400 tabular-nums">
              {data ? `${data.total.toLocaleString()} kunjungan` : ""}
            </span>
          </div>
        </div>

        {/* ── Table ── */}
        <div className="p-6 space-y-4">
          {isLoading ? (
            <SkeletonTable rows={8} cols={7} />
          ) : (
            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    <th>Tanggal</th>
                    <th>Salesman</th>
                    <th>Toko</th>
                    <th className="text-right">Total Order</th>
                    <th>EC</th>
                    <th>Durasi</th>
                    <th>Status</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {visits.length === 0 ? (
                    <tr>
                      <td colSpan={8}>
                        <EmptyState
                          icon={tab === "waiting" ? "check-circle" : "list-bullet"}
                          title={tab === "waiting" ? "Tidak ada kunjungan menunggu persetujuan" : "Tidak ada data kunjungan"}
                          description={hasFilters ? "Coba ubah atau hapus filter yang aktif." : undefined}
                        />
                      </td>
                    </tr>
                  ) : (
                    visits.map((v: Visit) => (
                      <tr
                        key={v.visit_id}
                        className="cursor-pointer group"
                        tabIndex={0}
                        role="link"
                        aria-label={`Detail kunjungan ${v.store_name ?? v.outlet_sk ?? ""} — ${v.visit_date}`}
                        onClick={() => navigate(`/visits/${v.visit_id}`)}
                        onKeyDown={(e) => { if (e.key === "Enter") navigate(`/visits/${v.visit_id}`); }}
                      >
                        <td className="text-slate-500 tabular-nums">{v.visit_date}</td>
                        <td className="font-medium text-slate-800">
                          {v.salesman_name ?? v.salesman_sk}
                        </td>
                        <td className="text-slate-600 max-w-[200px] truncate">
                          {v.store_name ?? v.outlet_sk ?? "—"}
                        </td>
                        <td className="text-right font-medium text-slate-700 tabular-nums">
                          {v.total_demand != null
                            ? `Rp ${v.total_demand.toLocaleString("id-ID")}`
                            : "—"}
                        </td>
                        <td>
                          {v.effective_call === "YES" ? (
                            <span className="badge-green">Efektif</span>
                          ) : v.effective_call === "NO" ? (
                            <span className="badge-gray">Tidak</span>
                          ) : (
                            <span className="text-slate-300">—</span>
                          )}
                        </td>
                        <td className="text-slate-500 tabular-nums">
                          {v.duration_minutes != null ? `${v.duration_minutes} mnt` : "—"}
                        </td>
                        <td>
                          <ApprovalBadge status={v.approval_status} />
                        </td>
                        <td>
                          <Icon name="chevron-right" className="w-4 h-4 text-slate-300 group-hover:text-primary-600" />
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}

          {/* ── Pagination ── */}
          {data && data.total > 50 && (
            <nav className="pagination" aria-label="Navigasi halaman">
              <span>{data.total.toLocaleString()} kunjungan total</span>
              <div className="flex items-center gap-2">
                <button
                  className="pagination-btn"
                  disabled={page === 1}
                  onClick={() => setPage((p) => p - 1)}
                  aria-label="Halaman sebelumnya"
                >
                  <Icon name="chevron-left" className="w-4 h-4" aria-hidden={true} />
                  Sebelumnya
                </button>
                <span className="text-xs text-slate-500 tabular-nums" aria-live="polite" aria-atomic="true">
                  Hal. {page} / {totalPages}
                </span>
                <button
                  className="pagination-btn"
                  disabled={!data.has_next}
                  onClick={() => setPage((p) => p + 1)}
                  aria-label="Halaman berikutnya"
                >
                  Berikutnya
                  <Icon name="chevron-right" className="w-4 h-4" aria-hidden={true} />
                </button>
              </div>
            </nav>
          )}
        </div>
      </main>
    </div>
  );
}
