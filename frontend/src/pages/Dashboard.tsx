import { useQuery } from "@tanstack/react-query";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import { useAuthStore } from "@/store/authStore";
import type { ComplyBrand, LeaderboardRow } from "@/types";
import { format } from "date-fns";

// ── API helpers ───────────────────────────────────────────────────────────────
const fetchDashboard = () => api.get("/dashboard/web").then((r) => r.data);

// ── Sub-components ─────────────────────────────────────────────────────────────
function KpiCard({ label, value, sub, color = "blue" }: { label: string; value: string | number; sub?: string; color?: string }) {
  const colors: Record<string, string> = {
    blue:   "bg-blue-50 border-blue-100",
    green:  "bg-green-50 border-green-100",
    yellow: "bg-amber-50 border-amber-100",
    red:    "bg-red-50 border-red-100",
  };
  const textColors: Record<string, string> = {
    blue: "text-blue-700", green: "text-green-700", yellow: "text-amber-700", red: "text-red-700",
  };
  return (
    <div className={`card border ${colors[color]} flex flex-col gap-1`}>
      <p className="text-xs text-slate-500 font-medium">{label}</p>
      <p className={`text-2xl font-bold ${textColors[color]}`}>{value}</p>
      {sub && <p className="text-xs text-slate-400">{sub}</p>}
    </div>
  );
}

function ComplyGauge({ pct, status }: { pct: number; status: string }) {
  const color = status === "Comply" ? "#16a34a" : status === "Over Target" ? "#2563eb" : "#dc2626";
  return (
    <div className="flex flex-col items-center gap-1">
      <div className="relative w-16 h-16">
        <svg viewBox="0 0 36 36" className="w-16 h-16 -rotate-90">
          <circle cx="18" cy="18" r="15.9" fill="none" stroke="#e2e8f0" strokeWidth="3" />
          <circle
            cx="18" cy="18" r="15.9" fill="none"
            stroke={color} strokeWidth="3"
            strokeDasharray={`${Math.min(pct, 100)} 100`}
            strokeLinecap="round"
          />
        </svg>
        <span className="absolute inset-0 flex items-center justify-center text-xs font-bold text-slate-700">
          {pct.toFixed(0)}%
        </span>
      </div>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  if (status === "Comply") return <span className="badge-green">{status}</span>;
  if (status === "Over Target") return <span className="badge-blue">{status}</span>;
  if (status === "Under Comply") return <span className="badge-red">{status}</span>;
  return <span className="badge-gray">{status}</span>;
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const user = useAuthStore((s) => s.user);
  const { data, isLoading } = useQuery({ queryKey: ["dashboard-web"], queryFn: fetchDashboard });

  const comply: ComplyBrand[]     = data?.comply ?? [];
  const leaderboard: LeaderboardRow[] = data?.leaderboard ?? [];
  const routeCompliancePct: number = data?.route_compliance_avg ?? 0;
  const achievementRows: { salesman_name: string; achievement_pct: number }[] = data?.achievement_vs_target ?? [];
  const announcements: { type: string; title: string; body: string; created_at: string }[] = data?.announcements ?? [];
  const kpis = data?.kpis ?? [];

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Dashboard"
        actions={
          <button onClick={() => window.location.reload()} className="btn-secondary text-sm px-3 py-1.5">
            ↻ Muat Ulang
          </button>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-6">
        {isLoading ? (
          <div className="flex items-center justify-center h-48 text-slate-400">Memuat data...</div>
        ) : (
          <>
            {/* KPI Row */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              {kpis.map((k: { label: string; value: string; sub?: string; color?: string }, i: number) => (
                <KpiCard key={i} {...k} />
              ))}
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Comply Target */}
              <div className="card xl:col-span-1">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="font-semibold text-slate-800">Comply Target</h2>
                  <a href="/target-management" className="text-xs text-primary-600 hover:underline">Kelola Target →</a>
                </div>
                <div className="space-y-3">
                  {comply.length === 0 && <p className="text-sm text-slate-400">Belum ada data target.</p>}
                  {comply.map((c) => (
                    <div key={c.brand} className="flex items-center gap-3">
                      <ComplyGauge pct={c.comply_pct} status={c.comply_status} />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between">
                          <p className="text-sm font-medium text-slate-700">{c.brand}</p>
                          <StatusBadge status={c.comply_status} />
                        </div>
                        <p className="text-xs text-slate-400 mt-0.5">
                          Mgmt: Rp {(c.management_target / 1e6).toFixed(1)}M · SPV: Rp {(c.spv_target / 1e6).toFixed(1)}M
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Achievement vs Target */}
              <div className="card xl:col-span-2">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="font-semibold text-slate-800">Achievement vs Target</h2>
                  <a href="/reports" className="text-xs text-primary-600 hover:underline">Lihat Reports →</a>
                </div>
                {achievementRows.length === 0 ? (
                  <p className="text-sm text-slate-400">Belum ada data achievement.</p>
                ) : (
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={achievementRows} layout="vertical" margin={{ left: 80 }}>
                      <XAxis type="number" domain={[0, 120]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} />
                      <YAxis type="category" dataKey="salesman_name" tick={{ fontSize: 11 }} width={80} />
                      <Tooltip formatter={(v: number) => [`${v.toFixed(1)}%`, "Achievement"]} />
                      <Bar dataKey="achievement_pct" radius={[0, 4, 4, 0]}>
                        {achievementRows.map((r, i) => (
                          <Cell key={i} fill={r.achievement_pct >= 100 ? "#16a34a" : r.achievement_pct >= 80 ? "#2563eb" : "#dc2626"} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </div>
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Route Compliance Gauge */}
              <div className="card flex flex-col items-center justify-center gap-3 py-6">
                <h2 className="font-semibold text-slate-800 self-start">Route Compliance</h2>
                <p className="text-xs text-slate-400 self-start">Rata-rata tim minggu ini</p>
                <div className="relative w-32 h-32">
                  <svg viewBox="0 0 36 36" className="w-32 h-32 -rotate-90">
                    <circle cx="18" cy="18" r="15.9" fill="none" stroke="#e2e8f0" strokeWidth="2.5" />
                    <circle
                      cx="18" cy="18" r="15.9" fill="none"
                      stroke={routeCompliancePct >= 80 ? "#16a34a" : routeCompliancePct >= 60 ? "#f59e0b" : "#dc2626"}
                      strokeWidth="2.5"
                      strokeDasharray={`${Math.min(routeCompliancePct, 100)} 100`}
                      strokeLinecap="round"
                    />
                  </svg>
                  <div className="absolute inset-0 flex flex-col items-center justify-center">
                    <span className="text-2xl font-bold text-slate-800">{routeCompliancePct.toFixed(0)}%</span>
                    <span className="text-xs text-slate-400">Compliance</span>
                  </div>
                </div>
                <p className="text-xs text-slate-500 text-center">
                  Kunjungan yang direncanakan vs. yang benar-benar terlaksana
                </p>
              </div>

              {/* Leaderboard */}
              <div className="card xl:col-span-2">
                <h2 className="font-semibold text-slate-800 mb-4">Leaderboard Tim</h2>
                {leaderboard.length === 0 ? (
                  <p className="text-sm text-slate-400">Belum ada data.</p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-slate-100">
                          <th className="text-left py-2 text-xs font-medium text-slate-400 w-8">#</th>
                          <th className="text-left py-2 text-xs font-medium text-slate-400">Salesman</th>
                          <th className="text-right py-2 text-xs font-medium text-slate-400">Achievement</th>
                          <th className="text-right py-2 text-xs font-medium text-slate-400">Compliance</th>
                          <th className="text-right py-2 text-xs font-medium text-slate-400">Coverage</th>
                        </tr>
                      </thead>
                      <tbody>
                        {leaderboard.map((row) => (
                          <tr key={row.salesman_sk} className="border-b border-slate-50 hover:bg-slate-50">
                            <td className="py-2 text-slate-500 font-bold">{row.rank}</td>
                            <td className="py-2 font-medium text-slate-700">{row.salesman_name}</td>
                            <td className="py-2 text-right">
                              <span className={row.achievement_pct >= 100 ? "text-green-600 font-semibold" : "text-slate-600"}>
                                {row.achievement_pct.toFixed(1)}%
                              </span>
                            </td>
                            <td className="py-2 text-right text-slate-600">{row.route_compliance_pct?.toFixed(1) ?? "—"}%</td>
                            <td className="py-2 text-right text-slate-600">{row.coverage_pct?.toFixed(1) ?? "—"}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>

            {/* Announcements Feed */}
            <div className="card">
              <div className="flex items-center justify-between mb-4">
                <h2 className="font-semibold text-slate-800">Feed Pengumuman</h2>
                <a href="/announcements" className="text-xs text-primary-600 hover:underline">Lihat semua →</a>
              </div>
              {announcements.length === 0 ? (
                <p className="text-sm text-slate-400">Belum ada pengumuman.</p>
              ) : (
                <div className="space-y-3">
                  {announcements.slice(0, 3).map((a, i) => (
                    <div key={i} className="flex gap-3 p-3 bg-slate-50 rounded-lg">
                      <span className="badge-blue shrink-0">{a.type}</span>
                      <div className="min-w-0">
                        <p className="text-sm font-medium text-slate-700">{a.title}</p>
                        <p className="text-xs text-slate-500 mt-0.5 line-clamp-2">{a.body}</p>
                        <p className="text-xs text-slate-400 mt-1">{format(new Date(a.created_at), "d MMM yyyy")}</p>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </>
        )}
      </main>
    </div>
  );
}
