import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import type { Salesman } from "@/types";

const fetchSalesmen = (search: string, type: string, status: string) =>
  api.get("/salesman/list", { params: { search, salesman_type: type || undefined, is_active: status === "Aktif" ? true : status === "Non-Aktif" ? false : undefined } }).then((r) => r.data);

export default function MasterDataSalesman() {
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [selected, setSelected] = useState<Salesman | null>(null);

  const { data, isLoading } = useQuery<{ items: Salesman[]; total: number }>({
    queryKey: ["salesmen-list", search, typeFilter, statusFilter],
    queryFn: () => fetchSalesmen(search, typeFilter, statusFilter),
  });

  const salesmen = data?.items ?? [];

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Master Data Salesman"
        actions={
          <div className="flex gap-2">
            <button className="btn-secondary text-sm">Export CSV</button>
            <button className="btn-primary text-sm">+ Tambah Salesman</button>
          </div>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-4">
        {/* Filters */}
        <div className="flex gap-3 flex-wrap">
          <input className="input w-64 text-sm" placeholder="Cari nama atau ID..." value={search} onChange={(e) => setSearch(e.target.value)} />
          <select className="input w-36 text-sm" value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)}>
            <option value="">Semua Tipe</option>
            <option>GTI</option><option>MIX</option><option>MTI</option>
          </select>
          <select className="input w-36 text-sm" value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
            <option value="">Semua Status</option>
            <option>Aktif</option><option>Non-Aktif</option>
          </select>
        </div>

        <div className="card overflow-x-auto">
          {isLoading ? (
            <p className="text-sm text-slate-400 p-4">Memuat data...</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  <th className="text-left py-2 text-xs font-medium text-slate-400">ID</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400">Nama</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400">Tipe</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400">Distributor</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400">Area</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400">SPV</th>
                  <th className="text-left py-2 text-xs font-medium text-slate-400">Status</th>
                  <th className="py-2" />
                </tr>
              </thead>
              <tbody>
                {salesmen.length === 0 ? (
                  <tr><td colSpan={8} className="py-8 text-center text-slate-400">Tidak ada data.</td></tr>
                ) : salesmen.map((s) => (
                  <tr key={s.salesman_sk} className="border-b border-slate-50 hover:bg-slate-50">
                    <td className="py-3 font-mono text-xs text-slate-500">{s.source_salesman_code}</td>
                    <td className="py-3 font-medium text-slate-700">{s.salesman_name}</td>
                    <td className="py-3 text-slate-500">{s.salesman_type}</td>
                    <td className="py-3 text-slate-500">{s.distributor_code ?? "—"}</td>
                    <td className="py-3 text-slate-500">{s.region ?? "—"}</td>
                    <td className="py-3 text-slate-500">{s.spv_name ?? "—"}</td>
                    <td className="py-3">
                      <span className={s.is_active ? "badge-green" : "badge-gray"}>{s.is_active ? "Aktif" : "Non-Aktif"}</span>
                    </td>
                    <td className="py-3">
                      <button onClick={() => setSelected(s)} className="text-xs text-primary-600 hover:underline">Detail</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </main>

      {/* Detail drawer */}
      {selected && (
        <div className="fixed inset-y-0 right-0 w-96 bg-white shadow-2xl border-l border-slate-200 flex flex-col z-40">
          <div className="flex items-center justify-between p-5 border-b border-slate-100">
            <h3 className="font-semibold text-slate-800">Detail Salesman</h3>
            <button onClick={() => setSelected(null)} className="text-slate-400 hover:text-slate-600 text-xl">×</button>
          </div>
          <div className="flex-1 overflow-y-auto p-5 space-y-4 text-sm">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 rounded-full bg-primary-100 text-primary-600 font-bold text-lg flex items-center justify-center">
                {selected.salesman_name[0]}
              </div>
              <div>
                <p className="font-semibold text-slate-800">{selected.salesman_name}</p>
                <span className={selected.is_active ? "badge-green" : "badge-gray"}>{selected.is_active ? "Aktif" : "Non-Aktif"}</span>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3 bg-slate-50 rounded-xl p-4">
              {[
                ["ID Salesman", selected.source_salesman_code],
                ["Tipe", selected.salesman_type],
                ["Distributor", selected.distributor_code ?? "—"],
                ["Region", selected.region ?? "—"],
                ["SPV", selected.spv_name ?? "—"],
                ["ASM", selected.asm_name ?? "—"],
              ].map(([k, v]) => (
                <div key={k}><p className="text-xs text-slate-400">{k}</p><p className="font-medium text-slate-700">{v}</p></div>
              ))}
            </div>
          </div>
          <div className="p-4 border-t border-slate-100 flex gap-2">
            <button className="btn-secondary flex-1">Edit</button>
            <button onClick={() => setSelected(null)} className="btn-primary flex-1">Tutup</button>
          </div>
        </div>
      )}
    </div>
  );
}
