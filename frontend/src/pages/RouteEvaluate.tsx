import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, SkeletonTable, SkeletonStatCards } from "@/components/ui";
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
    staleTime: 5 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const { data: detail } = useQuery<{ salesman: EvaluateTeamRow; stores: EvaluateStoreRow[] }>({
    queryKey: ["evaluate-detail", drillSalesman?.salesman_sk, weekKey],
    queryFn: () => fetchDetail(drillSalesman!.salesman_sk, weekKey),
    enabled: !!drillSalesman,
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const teamKpis = useMemo(() => ({
    totalCall: team.reduce((s, r) => s + r.call_count, 0),
    totalEC:   team.reduce((s, r) => s + r.effective_call_count, 0),
    ecRate:    team.length ? team.reduce((s, r) => s + r.ec_rate_pct, 0) / team.length : 0,
    lowConv:   team.filter((r) => r.ec_rate_pct < 50).length,
  }), [team]);

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Route Evaluate" />

      <main className="flex-1 overflow-y-auto p-6 space-y-6">
        {/* Week nav */}
        <div className="flex items-center gap-3">
          <button onClick={() => setWeekStart((d) => addDays(d, -7))} className="btn-secondary p-1.5" aria-label="Minggu sebelumnya"><Icon name="chevron-left" className="w-4 h-4" aria-hidden={true} /></button>
          <span className="text-sm font-semibold text-slate-700" aria-live="polite">{weekLabel}</span>
          <button onClick={() => setWeekStart((d) => addDays(d, 7))} className="btn-secondary p-1.5" aria-label="Minggu berikutnya"><Icon name="chevron-right" className="w-4 h-4" aria-hidden={true} /></button>
        </div>

        {/* Drill-down view */}
        {drillSalesman ? (
          <>
            <button onClick={() => setDrillSalesman(null)} className="btn-ghost btn-sm -ml-2 text-primary-600 hover:text-primary-700">
              <Icon name="arrow-left" className="w-4 h-4" aria-hidden={true} />Kembali ke Tim
            </button>

            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="kpi-tile">
                <span className="icon-badge icon-badge-slate"><Icon name="calendar" className="w-4 h-4" /></span>
                <div className="min-w-0 flex-1">
                  <p className="kpi-tile-value">{detail?.stores.length ?? 0}</p>
                  <p className="kpi-tile-label">Planned Visit</p>
                </div>
              </div>
              <div className="kpi-tile">
                <span className="icon-badge icon-badge-blue"><Icon name="map-pin" className="w-4 h-4" /></span>
                <div className="min-w-0 flex-1">
                  <p className="kpi-tile-value">{drillSalesman.call_count}</p>
                  <p className="kpi-tile-label">Call (Terlaksana)</p>
                </div>
              </div>
              <div className="kpi-tile">
                <span className="icon-badge icon-badge-green"><Icon name="check-circle" className="w-4 h-4" /></span>
                <div className="min-w-0 flex-1">
                  <p className="kpi-tile-value">{drillSalesman.effective_call_count}</p>
                  <p className="kpi-tile-label">Effective Call</p>
                </div>
              </div>
              <div className="kpi-tile">
                <span className="icon-badge icon-badge-amber"><Icon name="chart-pie" className="w-4 h-4" /></span>
                <div className="min-w-0 flex-1">
                  <ECBadge pct={drillSalesman.ec_rate_pct} />
                  <p className="kpi-tile-label">EC Rate</p>
                </div>
              </div>
            </div>

            <div className="card">
              <h2 className="font-semibold text-slate-800 mb-4">{drillSalesman.salesman_name} — Detail Toko</h2>
              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Toko</th>
                      <th className="text-center">Tier</th>
                      <th className="text-center">Planned</th>
                      <th className="text-center">Call</th>
                      <th className="text-center">Eff. Call</th>
                      <th className="text-right">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(detail?.stores ?? []).map((row) => (
                      <tr key={row.outlet_sk}>
                        <td className="font-medium text-slate-700">{row.store_name}</td>
                        <td className="text-center text-slate-500">{row.store_grade ?? "—"}</td>
                        <td className="text-center">{row.planned ? <span className="badge-green text-xs">Ya</span> : "—"}</td>
                        <td className="text-center">{row.is_call ? <span className="badge-green text-xs">Ya</span> : <span className="badge-red text-xs">Tidak</span>}</td>
                        <td className="text-center">{row.is_effective === null ? "—" : row.is_effective ? <span className="badge-green text-xs">Ya</span> : <span className="badge-red text-xs">Tidak</span>}</td>
                        <td className="text-right"><StatusBadge status={row.status} /></td>
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
            {isLoading ? (
              <SkeletonStatCards count={4} />
            ) : (
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                <div className="kpi-tile">
                  <span className="icon-badge icon-badge-blue"><Icon name="map-pin" className="w-4 h-4" /></span>
                  <div className="min-w-0 flex-1">
                    <p className="kpi-tile-value">{teamKpis.totalCall}</p>
                    <p className="kpi-tile-label">Total Call</p>
                  </div>
                </div>
                <div className="kpi-tile">
                  <span className="icon-badge icon-badge-green"><Icon name="check-circle" className="w-4 h-4" /></span>
                  <div className="min-w-0 flex-1">
                    <p className="kpi-tile-value">{teamKpis.totalEC}</p>
                    <p className="kpi-tile-label">Effective Call</p>
                  </div>
                </div>
                <div className="kpi-tile">
                  <span className="icon-badge icon-badge-amber"><Icon name="chart-pie" className="w-4 h-4" /></span>
                  <div className="min-w-0 flex-1">
                    <ECBadge pct={teamKpis.ecRate} />
                    <p className="kpi-tile-label">EC Rate (Tim)</p>
                  </div>
                </div>
                <div className="kpi-tile">
                  <span className="icon-badge icon-badge-purple"><Icon name="exclamation-triangle" className="w-4 h-4" /></span>
                  <div className="min-w-0 flex-1">
                    <p className="kpi-tile-value">{teamKpis.lowConv}</p>
                    <p className="kpi-tile-label">Low Conversion</p>
                  </div>
                </div>
              </div>
            )}

            <div className="card">
              <h2 className="font-semibold text-slate-800 mb-4">Performa Tim</h2>
              {isLoading ? (
                <SkeletonTable rows={5} cols={4} />
              ) : team.length === 0 ? (
                <EmptyState
                  icon="calendar"
                  title="Belum ada data minggu ini"
                  description="Data kunjungan akan muncul setelah salesman melakukan check-in"
                />
              ) : (
                <div className="table-container">
                  <table className="table">
                    <thead>
                      <tr>
                        <th>Salesman</th>
                        <th className="text-right">Call</th>
                        <th className="text-right">Eff. Call</th>
                        <th className="text-right">EC Rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {team.map((row) => (
                        <tr
                          key={row.salesman_sk}
                          role="button"
                          onClick={() => setDrillSalesman(row)}
                          onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setDrillSalesman(row); } }}
                          tabIndex={0}
                          aria-label={`Lihat detail ${row.salesman_name}`}
                          className="cursor-pointer group focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-primary-500"
                        >
                          <td className="font-medium text-slate-700 group-hover:text-primary-700 transition-colors">{row.salesman_name}</td>
                          <td className="text-right text-slate-600 tabular-nums">{row.call_count}</td>
                          <td className="text-right text-slate-600 tabular-nums">{row.effective_call_count}</td>
                          <td className="text-right"><ECBadge pct={row.ec_rate_pct} /></td>
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
