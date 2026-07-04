import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";

const fetchPjpSummary = () => api.get("/pjp/summary").then((r) => r.data);
const fetchPjpList    = (search: string) => api.get("/pjp/list", { params: { search } }).then((r) => r.data);

export default function MasterDataPjp() {
  const [activeTab, setActiveTab] = useState<"list" | "upload" | "config">("list");
  const [search, setSearch] = useState("");
  const [dragOver, setDragOver] = useState(false);

  const { data: summary } = useQuery({ queryKey: ["pjp-summary"], queryFn: fetchPjpSummary });
  const { data: pjpList = [], isLoading } = useQuery({
    queryKey: ["pjp-list", search],
    queryFn: () => fetchPjpList(search),
    enabled: activeTab === "list",
  });

  const summaryCards = [
    { label: "Total Toko (Basis DB)", value: summary?.total_stores ?? "—" },
    { label: "Toko dengan PJP", value: summary?.stores_with_pjp ?? "—" },
    { label: "Toko Basis Saja", value: summary?.stores_basis_only ?? "—" },
    { label: "Coverage PJP %", value: summary ? `${summary.coverage_pct?.toFixed(1)}%` : "—" },
  ];

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Master Data PJP" />

      <main className="flex-1 overflow-y-auto p-6 space-y-5">
        {/* Summary cards */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {summaryCards.map((c) => (
            <div key={c.label} className="card text-center">
              <p className="text-2xl font-bold text-primary-600">{c.value}</p>
              <p className="text-xs text-slate-500 mt-1">{c.label}</p>
            </div>
          ))}
        </div>

        {/* Tabs */}
        <div className="flex border-b border-slate-200">
          {([["list", "PJP Efektif"], ["upload", "Upload / Perbarui PJP"], ["config", "Konfigurasi Deadline"]] as const).map(([t, label]) => (
            <button
              key={t}
              onClick={() => setActiveTab(t)}
              className={`px-5 py-3 text-sm font-medium border-b-2 transition-colors ${activeTab === t ? "border-primary-600 text-primary-600" : "border-transparent text-slate-500 hover:text-slate-700"}`}
            >
              {label}
            </button>
          ))}
        </div>

        {activeTab === "list" && (
          <div className="card space-y-4">
            <div className="flex gap-3">
              <input className="input w-64 text-sm" placeholder="Cari toko atau salesman..." value={search} onChange={(e) => setSearch(e.target.value)} />
            </div>
            {isLoading ? <p className="text-sm text-slate-400">Memuat...</p> : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-100">
                    {["Kode Toko", "Nama Toko", "Brand", "Salesman", "Hari", "Frekuensi", "Minggu", "Sumber"].map((h) => (
                      <th key={h} className="text-left py-2 text-xs font-medium text-slate-400">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pjpList.length === 0 ? (
                    <tr><td colSpan={8} className="py-8 text-center text-slate-400">Tidak ada data PJP.</td></tr>
                  ) : pjpList.map((r: Record<string, string>, i: number) => (
                    <tr key={i} className="border-b border-slate-50 hover:bg-slate-50">
                      <td className="py-2 font-mono text-xs text-slate-500">{r.source_outlet_code}</td>
                      <td className="py-2 text-slate-700">{r.store_name}</td>
                      <td className="py-2 text-slate-500">{r.brand ?? "—"}</td>
                      <td className="py-2 text-slate-700">{r.source_salesman_name}</td>
                      <td className="py-2 text-slate-500">{r.visit_day_of_week}</td>
                      <td className="py-2 text-slate-500">{r.visit_frequency_code}</td>
                      <td className="py-2 text-slate-500">{r.visit_week_pattern}</td>
                      <td className="py-2"><span className="badge-gray text-xs">{r.source_system ?? "GT"}</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}

        {activeTab === "upload" && (
          <div className="card max-w-lg space-y-4">
            <p className="text-sm text-slate-600">Upload file Excel PJP sesuai template. Data hanya di-<em>commit</em> setelah konfirmasi eksplisit.</p>
            <div
              onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
              onDragLeave={() => setDragOver(false)}
              onDrop={(e) => { e.preventDefault(); setDragOver(false); }}
              className={`border-2 border-dashed rounded-xl p-10 text-center transition-colors cursor-pointer ${dragOver ? "border-primary-400 bg-primary-50" : "border-slate-200 hover:border-primary-300"}`}
            >
              <p className="text-4xl mb-2">📂</p>
              <p className="text-sm text-slate-500">Drag & drop file Excel di sini, atau</p>
              <label className="mt-3 inline-block btn-secondary text-sm cursor-pointer">
                Pilih File
                <input type="file" accept=".xlsx,.xls,.csv" className="hidden" />
              </label>
            </div>
            <div className="flex gap-2">
              <button className="btn-secondary text-sm">Download Template</button>
            </div>
          </div>
        )}

        {activeTab === "config" && (
          <div className="card max-w-sm space-y-4">
            <div>
              <label className="block text-sm font-medium text-slate-700 mb-1">Deadline Input PJP</label>
              <input type="date" className="input" />
            </div>
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" className="rounded" defaultChecked />
              <span className="text-sm text-slate-700">Periode input sedang terbuka</span>
            </label>
            <button className="btn-primary text-sm">Simpan</button>
          </div>
        )}
      </main>
    </div>
  );
}
