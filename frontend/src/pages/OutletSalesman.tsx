import { useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, SkeletonTable, EmptyState } from "@/components/ui";
import { api } from "@/api/client";
import { useDebounce } from "@/hooks/useDebounce";
import { useFocusTrap } from "@/hooks/useFocusTrap";
import type { Outlet } from "@/types";

const fetchOutlets = (search: string, unassigned: boolean) =>
  api.get("/outlet/list", { params: { search, unassigned_only: unassigned || undefined } }).then((r) => r.data);
const fetchSalesmenSimple = () =>
  api.get("/salesman/list", { params: { limit: 500 } }).then((r) => r.data?.items ?? []);

export default function OutletSalesman() {
  const qc = useQueryClient();
  const [searchInput, setSearchInput] = useState("");
  const [unassigned, setUnassigned]   = useState(false);
  const search = useDebounce(searchInput, 350);
  const [selected, setSelected] = useState<Outlet | null>(null);
  const [reassignSk, setReassignSk] = useState("");
  const modalTriggerRef = useRef<Element | null>(null);
  const modalPanelRef = useRef<HTMLDivElement>(null);
  useFocusTrap(modalPanelRef, !!selected);
  const closeModal = () => {
    setSelected(null);
    setTimeout(() => { (modalTriggerRef.current as HTMLElement | null)?.focus(); }, 0);
  };

  const { data: outletsData, isLoading } = useQuery({
    queryKey: ["outlets", search, unassigned],
    queryFn: () => fetchOutlets(search, unassigned),
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  });
  const { data: salesmen = [] } = useQuery({ queryKey: ["salesmen-simple"], queryFn: fetchSalesmenSimple, staleTime: 5 * 60 * 1000 });

  const outlets: Outlet[] = outletsData?.items ?? [];

  const reassignMutation = useMutation({
    mutationFn: ({ outletId, salesman_sk }: { outletId: string; salesman_sk: string }) =>
      api.post("/outlet/assign", { outlet_id: outletId, salesman_sk }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["outlets"] }); closeModal(); setReassignSk(""); },
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
          <div className="relative">
            <Icon name="magnifying-glass" className="w-4 h-4 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
            <input className="input w-64 text-sm pl-8" placeholder="Cari kode atau nama toko..." aria-label="Cari kode atau nama toko" value={searchInput} onChange={(e) => setSearchInput(e.target.value)} />
          </div>
          <label className="flex items-center gap-2 text-sm text-slate-700 cursor-pointer select-none">
            <input type="checkbox" checked={unassigned} onChange={(e) => setUnassigned(e.target.checked)} className="rounded" />
            Belum memiliki salesman
          </label>
        </div>

        <div className="card">
          {isLoading ? (
            <SkeletonTable rows={5} cols={7} />
          ) : outlets.length === 0 ? (
            <EmptyState
              icon="building-storefront"
              title="Tidak ada outlet"
              description="Coba ubah kata kunci pencarian atau filter"
            />
          ) : (
            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    {["Kode Toko", "Nama Toko", "Kecamatan", "Tier", "Salesman", "Kode SE", ""].map((h) => (
                      <th key={h}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {outlets.map((o) => (
                    <tr key={o.outlet_id}>
                      <td className="font-mono text-xs text-slate-500">{o.source_outlet_code}</td>
                      <td className="font-medium text-slate-700">{o.store_name}</td>
                      <td className="text-slate-500">{o.kecamatan ?? "—"}</td>
                      <td><span className="badge-gray text-xs">{o.tier ?? "—"}</span></td>
                      <td>{o.salesman_name ?? <span className="text-red-400 text-xs">Belum ditugaskan</span>}</td>
                      <td className="font-mono text-xs text-slate-500">{o.salesman_code ?? "—"}</td>
                      <td>
                        <button onClick={() => { modalTriggerRef.current = document.activeElement; setSelected(o); setReassignSk(""); }} className="text-xs text-primary-600 hover:underline" aria-label={`Assign ${o.store_name}`}>
                          Assign
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

      {selected && (
        <div
          className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4"
          onClick={(e) => { if (e.target === e.currentTarget) closeModal(); }}
        >
          <div
            ref={modalPanelRef}
            role="dialog"
            aria-modal="true"
            aria-labelledby="outlet-modal-title"
            className="bg-white rounded-2xl shadow-2xl w-full max-w-md"
          >
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 id="outlet-modal-title" className="font-semibold text-slate-800">Assign Toko ke Salesman</h3>
              <button onClick={closeModal} className="text-slate-400 hover:text-slate-600 p-1 rounded-lg hover:bg-slate-100 transition-colors" aria-label="Tutup"><Icon name="x-mark" className="w-5 h-5" /></button>
            </div>
            <div className="p-5 space-y-4 text-sm">
              <div className="bg-slate-50 rounded-xl p-4">
                <p className="font-semibold text-slate-800">{selected.store_name}</p>
                <p className="text-xs text-slate-400 mt-0.5">{selected.source_outlet_code} · {selected.kecamatan ?? "—"}</p>
              </div>
              <div>
                <label htmlFor="outlet-salesman-select" className="block text-sm font-medium text-slate-700 mb-1">Salesman Baru</label>
                <select id="outlet-salesman-select" className="input" value={reassignSk} onChange={(e) => setReassignSk(e.target.value)} autoFocus>
                  <option value="">— Pilih Salesman —</option>
                  {(salesmen as {salesman_sk: string; salesman_name: string; source_salesman_code: string}[]).map((s) => (
                    <option key={s.salesman_sk} value={s.salesman_sk}>{s.salesman_name} ({s.source_salesman_code})</option>
                  ))}
                </select>
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={closeModal} className="btn-secondary">Batal</button>
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
