import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import type { EvaluateTeamRow, EvaluateStoreRow } from "@/types";
import { format, startOfISOWeek, addDays, getISOWeek } from "date-fns";
import { id } from "date-fns/locale";

const fetchTeam   = (week: string) => api.get("/evaluate/team",   { params: { week } }).then((r) => r.data);
const fetchDetail = (salesmanSk: string, week: string) =>
  api.get(`/evaluate/salesman/${salesmanSk}`, { params: { week } }).then((r) => r.data);

function ECBadge({ pct }: { pct: number }) {
  if (pct >= 70) return <span className="badge-green">{pct.toFixed(1)}%</span>;
  if (pct >= 50) return <span className="badge-yellow">{pct.toFixed(1)}%</span>;
  return <span className="badge-red">{pct.toFixed(1)}%</span>;
}

function StatusBadge({ status }: { status: string }) {
  if (status === "OK")              return <span className="badge-green">OK</span>;
  if (status === "Low Conversion")  return <span className="badge-yellow">Low Conversion</span>;
  return <span className="badge-red">Belum Terlaksana</span>;
}

export default function RouteEvaluate() {
  const [weekStart, setWeekStart] = useState(startOfISOWeek(new Date()));
  const [drillSalesman, setDrillSalesman] = useState<EvaluateTeamRow | null>(null);

  const weekKey = format(weekStart, "yyyy-'W'II");
  const weekLabel = `Minggu ${getISOWeek(weekStart)}, ${format(weekStart, "d")}–${format(addDays(weekStart, 5), "d MMM yyyy", { locale: id })}`;

  const { data: team = [], isLoading } = useQuery<EvaluateTeamRow[]>({
    queryKey: ["evaluate-team", weekKey],
    queryFn: () => fetchTeam(weekKey),
  });

  const { data: detail } = useQuery<{ salesman: EvaluateTeamRow; stores: EvaluateStoreRow[] }>({
    queryKey: ["evaluate-detail", drillSalesman?.salesman_sk, weekKey],
    queryFn: () => fetchDetail(drillSalesman!.salesman_sk, weekKey),
    enabled: !!drillSalesman,
  });

  const teamKpis = {
    totalCall: team.reduce((s, r) => s + r.call_count, 0),
    totalEC:   team.reduce((s, r) => s + r.effective_call_count, 0),
    ecRate:    team.length ? team.reduce((s, r) => s + r.ec_rate_pct, 0) / team.length : 0,
    lowConv:   team.filter((r) => r.ec_rate_pct < 50).length,
  };

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Route Evaluate" />

      <main className="flex-1 overflow-y-auto p-6 space-y-6">
        {/* Week nav */}
        <div className="flex items-center gap-3">
          <button onClick={() => setWeekStart((d) => addDays(d, -7))} className="btn-secondary px-3 py-1.5 text-sm">‹</button>
          <span className="text-sm font-semibold text-slate-700">{weekLabel}</span>
          <button onClick={() => setWeekStart((d) => addDays(d, 7))} className="btn-secondary px-3 py-1.5 text-sm">›</button>
        </div>

        {/* Drill-down view */}
        {drillSalesman ? (
          <>
            <button onClick={() => setDrillSalesman(null)} className="text-sm text-primary-600 hover:underline">← Kembali ke Tim</button>

            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="card"><p className="text-xs text-slate-400">Planned Visit</p><p className="text-2xl font-bold text-slate-800">{detail?.stores.length ?? 0}</p></div>
              <div className="card"><p className="text-xs text-slate-400">Call (Terlaksana)</p><p className="text-2xl font-bold text-blue-600">{drillSalesman.call_count}</p></div>
              <div className="card"><p className="text-xs text-slate-400">Effective Call</p><p className="text-2xl font-bold text-green-600">{drillSalesman.effective_call_count}</p></div>
              <div className="card"><p className="text-xs text-slate-400">EC Rate</p><ECBadge pct={drillSalesman.ec_rate_pct} /></div>
            </div>

            <div className="card">
              <h2 className="font-semibold text-slate-800 mb-4">{drillSalesman.salesman_name} — Detail Toko</h2>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-slate-100">
                      <th className="text-left py-2 text-xs font-medium text-slate-400">Toko</th>
                      <th className="text-center py-2 text-xs font-medium text-slate-400">Tier</th>
                      <th className="text-center py-2 text-xs font-medium text-slate-400">Planned</th>
                      <th className="text-center py-2 text-xs font-medium text-slate-400">Call</th>
                      <th className="text-center py-2 text-xs font-medium text-slate-400">Eff. Call</th>
                      <th className="text-right py-2 text-xs font-medium text-slate-400">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(detail?.stores ?? []).map((row) => (
                      <tr key={row.outlet_sk} className="border-b border-slate-50 hover:bg-slate-50">
                        <td className="py-2 font-medium text-slate-700">{row.store_name}</td>
                        <td className="py-2 text-center text-slate-500">{row.store_grade ?? "—"}</td>
                        <td className="py-2 text-center">{row.planned ? "✓" : "—"}</td>
                        <td className="py-2 text-center">{row.is_call ? "✓" : "✗"}</td>
                        <td className="py-2 text-center">{row.is_effective === null ? "—" : row.is_effective ? "✓" : "✗"}</td>
                        <td className="py-2 text-right"><StatusBadge status={row.status} /></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        ) : (
          /* Team Roll-up */
          <>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="card"><p className="text-xs text-slate-400">Total Call</p><p className="text-2xl font-bold text-blue-600">{teamKpis.totalCall}</p></div>
              <div className="card"><p className="text-xs text-slate-400">Effective Call</p><p className="text-2xl font-bold text-green-600">{teamKpis.totalEC}</p></div>
              <div className="card"><p className="text-xs text-slate-400">EC Rate (Tim)</p><ECBadge pct={teamKpis.ecRate} /></div>
              <div className="card"><p className="text-xs text-slate-400">Low Conversion</p><p className="text-2xl font-bold text-amber-600">{teamKpis.lowConv}</p><p className="text-xs text-slate-400">salesman</p></div>
            </div>

            <div className="card">
              <h2 className="font-semibold text-slate-800 mb-4">Performa Tim</h2>
              {isLoading ? (
                <p className="text-sm text-slate-400">Memuat...</p>
              ) : team.length === 0 ? (
                <p className="text-sm text-slate-400">Belum ada data kunjungan minggu ini.</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-slate-100">
                        <th className="text-left py-2 text-xs font-medium text-slate-400">Salesman</th>
                        <th className="text-right py-2 text-xs font-medium text-slate-400">Call</th>
                        <th className="text-right py-2 text-xs font-medium text-slate-400">Eff. Call</th>
                        <th className="text-right py-2 text-xs font-medium text-slate-400">EC Rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {team.map((row) => (
                        <tr
                          key={row.salesman_sk}
                          onClick={() => setDrillSalesman(row)}
                          className="border-b border-slate-50 hover:bg-slate-50 cursor-pointer"
                        >
                          <td className="py-3 font-medium text-slate-700">{row.salesman_name}</td>
                          <td className="py-3 text-right text-slate-600">{row.call_count}</td>
                          <td className="py-3 text-right text-slate-600">{row.effective_call_count}</td>
                          <td className="py-3 text-right"><ECBadge pct={row.ec_rate_pct} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </>
        )}
      </main>
    </div>
  );
}
