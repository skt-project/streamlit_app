import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, SkeletonTable } from "@/components/ui";
import { api } from "@/api/client";
import { toast } from "@/store/toastStore";
import { useFocusTrap } from "@/hooks/useFocusTrap";
import type { Salesman } from "@/types";
import { useDebounce } from "@/hooks/useDebounce";

const fetchSalesmen = (search: string, type: string, status: string) =>
  api.get("/salesman/list", {
    params: {
      search:        search || undefined,
      salesman_type: type   || undefined,
      is_active:
        status === "Aktif"
          ? true
          : status === "Non-Aktif"
          ? false
          : undefined,
    },
  }).then((r) => r.data);

export default function MasterDataSalesman() {
  const [searchInput,   setSearchInput]   = useState("");
  const [typeFilter,    setTypeFilter]    = useState("");
  const [statusFilter,  setStatusFilter]  = useState("");
  const [selected,      setSelected]      = useState<Salesman | null>(null);
  const [exporting,     setExporting]     = useState(false);
  const drawerTriggerRef = useRef<Element | null>(null);
  const drawerPanelRef = useRef<HTMLDivElement>(null);
  useFocusTrap(drawerPanelRef, !!selected);
  const closeDrawer = () => {
    setSelected(null);
    setTimeout(() => { (drawerTriggerRef.current as HTMLElement | null)?.focus(); }, 0);
  };

  const debouncedSearch = useDebounce(searchInput, 350);

  const handleExport = async () => {
    setExporting(true);
    try {
      const res = await api.get("/export/salesman", { responseType: "blob" });
      const url = URL.createObjectURL(res.data as Blob);
      const a   = document.createElement("a");
      a.href = url; a.download = "master-salesman.csv"; a.click();
      URL.revokeObjectURL(url);
      toast.success("Export berhasil diunduh.");
    } catch {
      toast.error("Export gagal. Coba lagi.");
    } finally {
      setExporting(false);
    }
  };

  useEffect(() => {
    if (!selected) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeDrawer();
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [selected]);

  const { data, isLoading, isFetching } = useQuery<{ items: Salesman[]; total: number }>({
    queryKey:    ["salesmen-list", debouncedSearch, typeFilter, statusFilter],
    queryFn:     () => fetchSalesmen(debouncedSearch, typeFilter, statusFilter),
    staleTime:   30_000,
    placeholderData: (prev) => prev,
  });

  const salesmen   = data?.items ?? [];
  const showSpinner = isLoading || (isFetching && salesmen.length === 0);

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Master Data Salesman"
        actions={
          <div className="flex gap-2">
            <button className="btn-secondary text-sm" onClick={handleExport} disabled={exporting}>
              {exporting
                ? <Icon name="arrow-path" className="w-3.5 h-3.5 animate-spin" />
                : <Icon name="arrow-down-tray" className="w-3.5 h-3.5" />}
              Export CSV
            </button>
            <button className="btn-primary text-sm">
              <Icon name="plus" className="w-3.5 h-3.5" />
              Tambah Salesman
            </button>
          </div>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-4">
        {/* ── Filters ── */}
        <div className="flex gap-3 flex-wrap items-center">
          <div className="relative">
            <Icon
              name="magnifying-glass"
              className="w-4 h-4 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
            />
            <input
              className="input w-64 text-sm pl-8 pr-8"
              placeholder="Cari nama atau ID..."
              aria-label="Cari nama atau ID salesman"
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
            />
            {isFetching && (
              <Icon
                name="arrow-path"
                className="w-3.5 h-3.5 text-slate-400 absolute right-2.5 top-1/2 -translate-y-1/2 animate-spin"
              />
            )}
          </div>
          <select
            className="input w-36 text-sm"
            aria-label="Filter tipe salesman"
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value)}
          >
            <option value="">Semua Tipe</option>
            <option>GTI</option>
            <option>MIX</option>
            <option>MTI</option>
          </select>
          <select
            className="input w-36 text-sm"
            aria-label="Filter status salesman"
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="">Semua Status</option>
            <option>Aktif</option>
            <option>Non-Aktif</option>
          </select>
          <span className="text-xs text-slate-400 ml-auto">
            {data ? `${data.total} salesman` : ""}
          </span>
        </div>

        {/* ── Table ── */}
        <div className="card">
          {showSpinner ? (
            <SkeletonTable rows={8} cols={9} />
          ) : salesmen.length === 0 ? (
            <EmptyState
              icon="users"
              title="Tidak ada data"
              description="Tidak ada salesman yang cocok dengan filter ini."
            />
          ) : (
            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Nama</th>
                    <th>Tipe</th>
                    <th>Grup</th>
                    <th>Distributor</th>
                    <th>Area</th>
                    <th>SPV</th>
                    <th>Status</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {salesmen.map((s) => (
                    <tr key={s.salesman_sk}>
                      <td className="font-mono text-xs text-slate-500">{s.source_salesman_code}</td>
                      <td className="font-medium">{s.salesman_name}</td>
                      <td>{s.salesman_type}</td>
                      <td>
                        {s.brand_group
                          ? <span className="badge-blue text-xs">{s.brand_group}</span>
                          : <span className="text-slate-400">—</span>}
                      </td>
                      <td>{s.distributor_code ?? "—"}</td>
                      <td>{s.region ?? "—"}</td>
                      <td>{s.spv_name ?? "—"}</td>
                      <td>
                        <span className={s.is_active ? "badge-green" : "badge-gray"}>
                          {s.is_active ? "Aktif" : "Non-Aktif"}
                        </span>
                      </td>
                      <td>
                        <button
                          onClick={() => { drawerTriggerRef.current = document.activeElement; setSelected(s); }}
                          className="text-xs text-primary-600 hover:underline"
                          aria-label={`Detail ${s.salesman_name}`}
                        >
                          Detail
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>

      {/* ── Detail drawer ── */}
      {selected && (
        <>
          <div
            className="fixed inset-0 bg-black/20 z-30"
            onClick={closeDrawer}
            aria-hidden="true"
          />
          <div ref={drawerPanelRef} role="dialog" aria-modal="true" aria-labelledby="salesman-drawer-title" className="fixed inset-y-0 right-0 w-96 bg-white shadow-2xl border-l border-slate-200 flex flex-col z-40">
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 id="salesman-drawer-title" className="font-semibold text-slate-800">Detail Salesman</h3>
              <button
                onClick={closeDrawer}
                className="text-slate-400 hover:text-slate-600 p-1 rounded-lg hover:bg-slate-100 transition-colors"
                aria-label="Tutup"
              >
                <Icon name="x-mark" className="w-5 h-5" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-5 space-y-4 text-sm">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 rounded-full bg-primary-100 text-primary-600 font-bold text-lg flex items-center justify-center shrink-0">
                  {selected.salesman_name[0]}
                </div>
                <div>
                  <p className="font-semibold text-slate-800">{selected.salesman_name}</p>
                  <span className={selected.is_active ? "badge-green" : "badge-gray"}>
                    {selected.is_active ? "Aktif" : "Non-Aktif"}
                  </span>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-3 bg-slate-50 rounded-xl p-4">
                {(
                  [
                    ["ID Salesman", selected.source_salesman_code],
                    ["Tipe",        selected.salesman_type],
                    ["Grup Bisnis", selected.brand_group ?? "—"],
                    ["Distributor", selected.distributor_code ?? "—"],
                    ["Region",      selected.region ?? "—"],
                    ["SPV",         selected.spv_name ?? "—"],
                    ["ASM",         selected.asm_name ?? "—"],
                  ] as [string, string][]
                ).map(([k, v]) => (
                  <div key={k}>
                    <p className="text-xs text-slate-400">{k}</p>
                    <p className="font-medium text-slate-700">{v}</p>
                  </div>
                ))}
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex gap-2">
              <button className="btn-secondary flex-1">Edit</button>
              <button onClick={closeDrawer} className="btn-primary flex-1">
                Tutup
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
