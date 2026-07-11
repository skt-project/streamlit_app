import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, SkeletonTable } from "@/components/ui";
import { api } from "@/api/client";
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

  const debouncedSearch = useDebounce(searchInput, 350);

  useEffect(() => {
    if (!selected) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") setSelected(null);
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
            <button className="btn-secondary text-sm">
              <Icon name="arrow-down-tray" className="w-3.5 h-3.5" />
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
                          onClick={() => setSelected(s)}
                          className="text-xs text-primary-600 hover:underline"
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
            onClick={() => setSelected(null)}
            aria-hidden="true"
          />
          <div className="fixed inset-y-0 right-0 w-96 bg-white shadow-2xl border-l border-slate-200 flex flex-col z-40">
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 className="font-semibold text-slate-800">Detail Salesman</h3>
              <button
                onClick={() => setSelected(null)}
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
              <button onClick={() => setSelected(null)} className="btn-primary flex-1">
                Tutup
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
