import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";

const REPORT_TYPES = ["Achievement", "Route Compliance", "Sell-In YTD", "Effective Call Rate"];
const PERIODS = ["Bulan Ini", "Bulan Lalu", "Kuartal Ini", "YTD", "Semua"];
const TIERS = ["Semua Tier", "S", "A", "B", "C", "D"];

const fetchReport = (type: string, period: string, tier: string) =>
  api.get("/reports", { params: { type, period, tier } }).then((r) => r.data);

export default function Reports() {
  const [activeReport, setActiveReport] = useState("Achievement");
  const [period, setPeriod] = useState("Bulan Ini");
  const [tier, setTier] = useState("Semua Tier");

  const { data, isLoading } = useQuery({
    queryKey: ["reports", activeReport, period, tier],
    queryFn: () => fetchReport(activeReport, period, tier),
  });

  const rows = data?.rows ?? [];
  const kpis = data?.kpis ?? [];

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Reports"
        actions={
          <div className="flex gap-2">
            <button className="btn-secondary text-sm" onClick={() => api.get("/reports/export", { params: { type: activeReport, period, tier, format: "excel" }, responseType: "blob" })}>
              Export Excel
            </button>
            <button className="btn-secondary text-sm">Export CSV</button>
          </div>
        }
      />

      <div className="flex flex-1 min-h-0">
        {/* Sidebar report list */}
        <aside className="w-52 border-r border-slate-200 bg-white p-3 space-y-1">
          <p className="text-xs font-medium text-slate-400 px-2 py-1">Reports</p>
          {REPORT_TYPES.map((r) => (
            <button
              key={r}
              onClick={() => setActiveReport(r)}
              className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors ${
                activeReport === r ? "bg-primary-50 text-primary-700 font-medium" : "text-slate-600 hover:bg-slate-50"
              }`}
            >
              {r}
            </button>
          ))}
        </aside>

        {/* Main */}
        <main className="flex-1 overflow-y-auto p-6 space-y-5">
          {/* Filter bar */}
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex gap-1">
              {PERIODS.map((p) => (
                <button
                  key={p}
                  onClick={() => setPeriod(p)}
                  className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-colors ${period === p ? "bg-primary-600 text-white" : "bg-white border border-slate-200 text-slate-600 hover:border-primary-300"}`}
                >
                  {p}
                </button>
              ))}
            </div>
            <div className="flex gap-1 ml-2">
              {TIERS.map((t) => (
                <button
                  key={t}
                  onClick={() => setTier(t)}
                  className={`px-2.5 py-1.5 text-xs rounded-lg font-medium transition-colors ${tier === t ? "bg-slate-700 text-white" : "bg-white border border-slate-200 text-slate-600 hover:border-slate-400"}`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>

          {/* KPI row */}
          {kpis.length > 0 && (
            <div className="grid grid-cols-3 gap-4">
              {kpis.map((k: { label: string; value: string }, i: number) => (
                <div key={i} className="card">
                  <p className="text-xs text-slate-400">{k.label}</p>
                  <p className="text-xl font-bold text-slate-800 mt-1">{k.value}</p>
                </div>
              ))}
            </div>
          )}

          {/* Table */}
          <div className="card overflow-x-auto">
            <h2 className="font-semibold text-slate-800 mb-4">{activeReport} — {period}</h2>
            {isLoading ? (
              <p className="text-sm text-slate-400">Memuat data...</p>
            ) : rows.length === 0 ? (
              <p className="text-sm text-slate-400">Belum ada data untuk filter ini.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-100">
                    <th className="text-left py-2 text-xs font-medium text-slate-400">#</th>
                    {Object.keys(rows[0]).filter((k) => k !== "salesman_sk").map((k) => (
                      <th key={k} className="text-left py-2 text-xs font-medium text-slate-400 capitalize">{k.replace(/_/g, " ")}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row: Record<string, unknown>, i: number) => (
                    <tr key={i} className="border-b border-slate-50 hover:bg-slate-50">
                      <td className="py-2 text-slate-400">{i + 1}</td>
                      {Object.entries(row).filter(([k]) => k !== "salesman_sk").map(([k, v]) => (
                        <td key={k} className="py-2 text-slate-700">{String(v ?? "—")}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}
