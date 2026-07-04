import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import type { Outlet } from "@/types";

const fetchOutlets = (search: string, unassigned: boolean) =>
  api.get("/outlet/list", { params: { search, unassigned_only: unassigned || undefined } }).then((r) => r.data);
const fetchSalesmenSimple = () =>
  api.get("/salesman/list", { params: { limit: 500 } }).then((r) => r.data?.items ?? []);

export default function OutletSalesman() {
  const qc = useQueryClient();
  const [search, setSearch] = useState("");
  const [unassigned, setUnassigned] = useState(false);
  const [selected, setSelected] = useState<Outlet | null>(null);
  const [reassignSk, setReassignSk] = useState("");

  const { data: outletsData, isLoading } = useQuery({
    queryKey: ["outlets", search, unassigned],
    queryFn: () => fetchOutlets(search, unassigned),
  });
  const { data: salesmen = [] } = useQuery({ queryKey: ["salesmen-simple"], queryFn: fetchSalesmenSimple });

  const outlets: Outlet[] = outletsData?.items ?? [];

  const reassignMutation = useMutation({
    mutationFn: ({ outletId, salesman_sk }: { outletId: string; salesman_sk: string }) =>
      api.post("/outlet/assign", { outlet_id: outletId, salesman_sk }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["outlets"] }); setSelected(null); setReassignSk(""); },
  });

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Outlet – Salesman"
        actions={
          <div className="flex gap-2">
            <button className="btn-secondary text-sm">Export CSV</button>
            <button className="btn-primary text-sm">Bulk Assign</button>
          </div>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-4">
        <div className="flex gap-3 flex-wrap items-center">
          <input className="input w-64 text-sm" placeholder="Cari kode atau nama toko..." value={search} onChange={(e) => setSearch(e.target.value)} />
          <label className="flex items-center gap-2 text-sm text-slate-700 cursor-pointer">
            <input type="checkbox" checked={unassigned} onChange={(e) => setUnassigned(e.target.checked)} />
            Belum memiliki salesman
          </label>
        </div>

        <div className="card overflow-x-auto">
          {isLoading ? (
            <p className="text-sm text-slate-400 p-4">Memuat data...</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  {["Kode Toko", "Nama Toko", "Kecamatan", "Tier", "Salesman", "Kode SE", ""].map((h) => (
                    <th key={h} className="text-left py-2 text-xs font-medium text-slate-400">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {outlets.length === 0 ? (
                  <tr><td colSpan={7} className="py-8 text-center text-slate-400">Tidak ada data.</td></tr>
                ) : outlets.map((o) => (
                  <tr key={o.outlet_id} className="border-b border-slate-50 hover:bg-slate-50">
                    <td className="py-3 font-mono text-xs text-slate-500">{o.source_outlet_code}</td>
                    <td className="py-3 font-medium text-slate-700">{o.store_name}</td>
                    <td className="py-3 text-slate-500">{o.kecamatan ?? "—"}</td>
                    <td className="py-3">
                      <span className="badge-gray text-xs">{o.tier ?? "—"}</span>
                    </td>
                    <td className="py-3 text-slate-700">{o.salesman_name ?? <span className="text-red-400 text-xs">Belum ditugaskan</span>}</td>
                    <td className="py-3 font-mono text-xs text-slate-500">{o.salesman_code ?? "—"}</td>
                    <td className="py-3">
                      <button onClick={() => { setSelected(o); setReassignSk(""); }} className="text-xs text-primary-600 hover:underline">
                        Assign
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </main>

      {selected && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md">
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 className="font-semibold text-slate-800">Assign Toko ke Salesman</h3>
              <button onClick={() => setSelected(null)} className="text-slate-400 hover:text-slate-600 text-xl">×</button>
            </div>
            <div className="p-5 space-y-4 text-sm">
              <div className="bg-slate-50 rounded-xl p-4">
                <p className="font-semibold text-slate-800">{selected.store_name}</p>
                <p className="text-xs text-slate-400 mt-0.5">{selected.source_outlet_code} · {selected.kecamatan ?? "—"}</p>
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Salesman Baru</label>
                <select className="input" value={reassignSk} onChange={(e) => setReassignSk(e.target.value)}>
                  <option value="">— Pilih Salesman —</option>
                  {(salesmen as {salesman_sk: string; salesman_name: string; source_salesman_code: string}[]).map((s) => (
                    <option key={s.salesman_sk} value={s.salesman_sk}>{s.salesman_name} ({s.source_salesman_code})</option>
                  ))}
                </select>
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={() => setSelected(null)} className="btn-secondary">Batal</button>
              <button
                onClick={() => reassignMutation.mutate({ outletId: selected.outlet_id, salesman_sk: reassignSk })}
                className="btn-primary"
                disabled={!reassignSk || reassignMutation.isPending}
              >
                {reassignMutation.isPending ? "Menyimpan..." : "Simpan"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
