import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, SkeletonTable } from "@/components/ui";
import { api } from "@/api/client";
import type { TargetComply, SpvTargetRow } from "@/types";
import { format } from "date-fns";

const fetchComply   = () => api.get("/target/comply").then((r) => r.data);
const fetchSpvRows  = (brand: string, period: string) =>
  api.get("/target/spv", { params: { brand, period_month: period } }).then((r) => r.data);

function StatusBadge({ status }: { status: string }) {
  if (status === "Comply")        return <span className="badge-green">{status}</span>;
  if (status === "Over Target")   return <span className="badge-blue">{status}</span>;
  if (status === "Under Comply")  return <span className="badge-red">{status}</span>;
  return <span className="badge-gray">{status}</span>;
}

function ComplyBar({ pct }: { pct: number }) {
  const capped = Math.min(pct, 110);
  const color = pct >= 100 ? "bg-green-500" : pct >= 80 ? "bg-blue-500" : "bg-red-400";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-slate-100 rounded-full h-2">
        <div className={`h-2 rounded-full transition-all ${color}`} style={{ width: `${capped}%` }} />
      </div>
      <span className="text-xs font-medium text-slate-600 w-10 text-right">{pct.toFixed(1)}%</span>
    </div>
  );
}

export default function TargetManagement() {
  const qc = useQueryClient();
  const period = format(new Date(), "yyyy-MM-01");
  const [selectedBrand, setSelectedBrand] = useState<string | null>(null);
  const [simulationDelta, setSimulationDelta] = useState(0);
  const [showSubmitModal, setShowSubmitModal] = useState(false);
  const [editValues, setEditValues] = useState<Record<string, string>>({});

  const { data: comply = [], isLoading } = useQuery<TargetComply[]>({
    queryKey: ["target-comply"],
    queryFn: fetchComply,
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const { data: spvRows = [] } = useQuery<SpvTargetRow[]>({
    queryKey: ["spv-target", selectedBrand, period],
    queryFn: () => fetchSpvRows(selectedBrand!, period),
    enabled: !!selectedBrand,
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const saveMutation = useMutation({
    mutationFn: (rows: { salesman_sk: string; amount: number }[]) =>
      api.post("/target/spv/bulk", {
        rows: rows.map((r) => ({
          salesman_sk:       Number(r.salesman_sk),
          brand:             selectedBrand,
          period_month:      period,
          spv_target_amount: r.amount,
        })),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["target-comply"] });
      qc.invalidateQueries({ queryKey: ["spv-target"] });
      setEditValues({});
    },
  });

  const submitMutation = useMutation({
    mutationFn: () => api.post("/target/spv/submit", { brand: selectedBrand, period_month: period }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["target-comply"] }); setShowSubmitModal(false); },
  });

  const selectedComply = comply.find((c) => c.brand === selectedBrand);
  const simNewSpvTotal = selectedComply
    ? selectedComply.spv_target_total * (1 + simulationDelta / 100)
    : 0;
  const simNewComply = selectedComply?.management_target_total
    ? (simNewSpvTotal / selectedComply.management_target_total) * 100
    : 0;

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Target Management"
        actions={
          <div className="flex gap-2">
            <button
              className="btn-secondary text-sm"
              disabled={!selectedBrand || spvRows.length === 0 || saveMutation.isPending}
              onClick={() => saveMutation.mutate(
                spvRows.map((r) => ({ salesman_sk: r.salesman_sk, amount: Number(editValues[r.salesman_sk] ?? r.spv_target_amount) }))
              )}
            >
              {saveMutation.isPending ? "Menyimpan..." : "Simpan Draft"}
            </button>
            <button className="btn-primary text-sm" onClick={() => setShowSubmitModal(true)} disabled={!selectedBrand}>Submit</button>
          </div>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-6">
        {/* Comply definition */}
        <div className="bg-blue-50 border border-blue-100 rounded-xl p-4 flex gap-3">
          <Icon name="information-circle" className="w-5 h-5 text-blue-500 shrink-0 mt-0.5" />
          <div>
            <p className="text-sm text-blue-800 font-medium">Definisi Comply</p>
            <p className="text-sm text-blue-700 mt-1">
              Comply mengukur apakah total target yang diajukan SPV sesuai dengan target yang ditetapkan Management.
            </p>
            <p className="text-sm font-mono text-blue-600 mt-1">
              Comply % = Total Target SPV ÷ Total Target Management × 100%
            </p>
          </div>
        </div>

        {/* Brand Comply Summary */}
        <div className="card">
          <h2 className="font-semibold text-slate-800 mb-4">Comply per Brand — {format(new Date(), "MMMM yyyy")}</h2>
          {isLoading ? (
            <div className="space-y-3">
              {Array.from({ length: 3 }).map((_, i) => (
                <div key={i} className="rounded-xl border border-slate-100 bg-slate-50 p-4 space-y-2 animate-pulse">
                  <div className="h-4 w-32 bg-slate-200 rounded" />
                  <div className="h-2 w-full bg-slate-200 rounded-full" />
                  <div className="h-3 w-48 bg-slate-100 rounded" />
                </div>
              ))}
            </div>
          ) : comply.length === 0 ? (
            <EmptyState
              icon="clipboard-document-list"
              title="Belum ada data target"
              description="Data target bulan ini belum tersedia"
            />
          ) : (
            <div className="space-y-3">
              {comply.map((c) => (
                <button
                  key={c.brand}
                  onClick={() => setSelectedBrand(c.brand === selectedBrand ? null : c.brand)}
                  aria-pressed={selectedBrand === c.brand}
                  className={`w-full text-left p-4 rounded-xl border transition-all ${
                    selectedBrand === c.brand ? "border-primary-300 bg-primary-50" : "border-slate-100 bg-slate-50 hover:border-slate-200"
                  }`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-3">
                      <p className="font-semibold text-slate-800">{c.brand}</p>
                      <StatusBadge status={c.comply_status} />
                    </div>
                    <span className="text-xs text-slate-400">{spvRows.length > 0 && selectedBrand === c.brand ? `${spvRows.length} salesman` : ""}</span>
                  </div>
                  <ComplyBar pct={c.comply_pct} />
                  <div className="flex gap-6 mt-2 text-xs text-slate-500">
                    <span>Mgmt: <strong>Rp {(c.management_target_total / 1e6).toFixed(1)}M</strong></span>
                    <span>SPV: <strong>Rp {(c.spv_target_total / 1e6).toFixed(1)}M</strong></span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* SPV Distribution Table */}
        {selectedBrand && (
          <div className="card">
            <div className="flex items-center justify-between mb-4">
              <h2 className="font-semibold text-slate-800">Distribusi Target SPV — {selectedBrand}</h2>
              <button className="btn-secondary text-sm">Atur Target Massal</button>
            </div>
            {spvRows.length === 0 ? (
              <EmptyState
                icon="clipboard-document-list"
                title="Belum ada distribusi target"
                description='Klik "Simpan Draft" untuk memulai distribusi target'
              />
            ) : (
              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Salesman</th>
                      <th>Area</th>
                      <th className="text-right">Target (Rp)</th>
                      <th className="text-right">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {spvRows.map((row) => (
                      <tr key={row.salesman_sk}>
                        <td className="font-medium text-slate-700">{row.salesman_name}</td>
                        <td className="text-slate-500">—</td>
                        <td className="text-right">
                          <input
                            type="number"
                            className="input text-right w-36 text-sm"
                            value={editValues[row.salesman_sk] ?? row.spv_target_amount}
                            onChange={(e) => setEditValues((prev) => ({ ...prev, [row.salesman_sk]: e.target.value }))}
                          />
                        </td>
                        <td className="text-right"><StatusBadge status={row.approval_status} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {/* Simulation */}
        {selectedComply && (
          <div className="card">
            <h2 className="font-semibold text-slate-800 mb-4">Simulasi Distribusi Target</h2>
            <p className="text-sm text-slate-500 mb-4">
              Simulasikan kenaikan/penurunan target SPV untuk {selectedBrand} — lihat dampaknya pada Comply % sebelum diterapkan.
            </p>
            <div className="flex items-center gap-4 mb-4">
              <span className="text-sm text-slate-500">−20%</span>
              <input
                type="range" min={-20} max={20} step={1}
                value={simulationDelta}
                onChange={(e) => setSimulationDelta(Number(e.target.value))}
                className="flex-1"
              />
              <span className="text-sm text-slate-500">+20%</span>
              <span className="w-16 text-center text-sm font-semibold text-primary-600">
                {simulationDelta > 0 ? "+" : ""}{simulationDelta}%
              </span>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="bg-slate-50 rounded-xl p-4">
                <p className="text-xs text-slate-500 mb-1">SPV Target Baru (Simulasi)</p>
                <p className="text-xl font-bold text-slate-800">Rp {(simNewSpvTotal / 1e6).toFixed(1)}M</p>
              </div>
              <div className="bg-slate-50 rounded-xl p-4">
                <p className="text-xs text-slate-500 mb-1">Comply % Baru</p>
                <p className={`text-xl font-bold ${simNewComply >= 100 ? "text-green-600" : "text-red-600"}`}>
                  {simNewComply.toFixed(1)}%
                </p>
              </div>
            </div>
          </div>
        )}
      </main>

      {/* Submit Modal */}
      {showSubmitModal && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-sm">
            <div className="p-5 border-b border-slate-100">
              <h3 className="font-semibold text-slate-800">Submit Target {selectedBrand}?</h3>
            </div>
            <div className="p-5">
              <p className="text-sm text-slate-600">
                Target akan melewati alur approval: <strong>SPV → Area Manager → Distributor Manager</strong>. Pastikan distribusi sudah benar.
              </p>
              <textarea
                className="input mt-4 text-sm"
                placeholder="Komentar (opsional)..."
                rows={3}
              />
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={() => setShowSubmitModal(false)} className="btn-secondary">Batal</button>
              <button onClick={() => submitMutation.mutate()} className="btn-primary" disabled={submitMutation.isPending}>
                {submitMutation.isPending ? "Memproses..." : "Submit"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
