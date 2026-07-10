import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import TopNav from "@/components/layout/TopNav";
import { Icon, SkeletonStatCards, Skeleton, EmptyState } from "@/components/ui";
import { api } from "@/api/client";
import { format } from "date-fns";
import type { ComplyBrand, LeaderboardRow } from "@/types";

// ── API ───────────────────────────────────────────────────────────────────────
const fetchDashboard = () => api.get("/dashboard/web").then((r) => r.data);

// ── Derive comply status from pct ──────────────────────────────────────────────
function complyStatus(pct: number): ComplyBrand["comply_status"] {
  if (pct >= 100) return "Over Target";
  if (pct >= 80)  return "Comply";
  if (pct > 0)    return "Under Comply";
  return "No Data";
}

// ── KPI Card ─────────────────────────────────────────────────────────────────
const ACCENT: Record<string, { border: string; value: string }> = {
  blue:   { border: "border-l-blue-500",    value: "text-blue-600"    },
  green:  { border: "border-l-emerald-500", value: "text-emerald-600" },
  yellow: { border: "border-l-amber-500",   value: "text-amber-600"   },
  red:    { border: "border-l-red-500",     value: "text-red-600"     },
};

function KpiCard({
  label,
  value,
  sub,
  color = "blue",
}: {
  label: string;
  value: string | number;
  sub?: string;
  color?: string;
}) {
  const a = ACCENT[color] ?? ACCENT.blue;
  return (
    <div className={`card border-l-4 ${a.border} flex flex-col gap-1`}>
      <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide">{label}</p>
      <p className={`text-3xl font-bold ${a.value} leading-tight`}>{value}</p>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  );
}

// ── Comply Gauge ──────────────────────────────────────────────────────────────
function ComplyGauge({ pct, status }: { pct: number; status: string }) {
  const color =
    status === "Comply"      ? "#10b981" :
    status === "Over Target" ? "#2563eb" : "#ef4444";
  return (
    <div className="relative w-14 h-14 shrink-0">
      <svg viewBox="0 0 36 36" className="w-14 h-14 -rotate-90">
        <circle cx="18" cy="18" r="15.9" fill="none" stroke="#e2e8f0" strokeWidth="3" />
        <circle
          cx="18" cy="18" r="15.9" fill="none"
          stroke={color} strokeWidth="3"
          strokeDasharray={`${Math.min(pct, 100)} 100`}
          strokeLinecap="round"
        />
      </svg>
      <span className="absolute inset-0 flex items-center justify-center text-[10px] font-bold text-slate-700">
        {pct.toFixed(0)}%
      </span>
    </div>
  );
}

// ── Status Badge ──────────────────────────────────────────────────────────────
function StatusBadge({ status }: { status: string }) {
  if (status === "Comply")       return <span className="badge-green">{status}</span>;
  if (status === "Over Target")  return <span className="badge-blue">{status}</span>;
  if (status === "Under Comply") return <span className="badge-red">{status}</span>;
  return <span className="badge-gray">{status}</span>;
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return <h2 className="text-base font-semibold text-slate-900">{children}</h2>;
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const { data, isLoading } = useQuery({
    queryKey: ["dashboard-web"],
    queryFn: fetchDashboard,
    staleTime: 5 * 60 * 1000,
  });

  // Map API response fields → typed values
  const complyBrands: ComplyBrand[] = useMemo(
    () =>
      (data?.comply_brands ?? []).map((r: any) => ({
        brand:             r.brand,
        management_target: r.management_target ?? 0,
        spv_target:        r.spv_target ?? 0,
        comply_pct:        r.comply_pct ?? 0,
        comply_status:     complyStatus(r.comply_pct ?? 0),
      })),
    [data],
  );

  // Leaderboard — API returns visit_mtd + ec_rate; map ec_rate → achievement_pct
  const leaderboard: (LeaderboardRow & { visit_mtd: number; ec_rate: number })[] = useMemo(
    () =>
      (data?.leaderboard ?? []).map((r: any, i: number) => ({
        rank:                 i + 1,
        salesman_sk:          r.salesman_sk,
        salesman_name:        r.salesman_name ?? "—",
        achievement_pct:      r.ec_rate ?? 0,
        route_compliance_pct: 0,
        coverage_pct:         0,
        visit_mtd:            r.visit_mtd ?? 0,
        ec_rate:              r.ec_rate ?? 0,
      })),
    [data],
  );

  const routeCompliancePct: number  = data?.route_comply_pct ?? 0;
  const visitToday: number          = data?.visit_today ?? 0;
  const ecToday: number             = data?.ec_today ?? 0;
  const overallComplyPct: number    = data?.comply_pct ?? 0;

  const announcements: {
    type: string; title: string; body: string; created_at: string;
  }[] = data?.announcements ?? [];

  const kpis = [
    { label: "Kunjungan Hari Ini",  value: visitToday,                         sub: "total visit",          color: "blue"   },
    { label: "EC Hari Ini",         value: ecToday,                             sub: "effective call",       color: "green"  },
    { label: "Comply MTD",          value: `${overallComplyPct.toFixed(1)}%`,   sub: "vs management target", color: overallComplyPct >= 80 ? "green" : "red" },
    { label: "Route Compliance",    value: `${routeCompliancePct.toFixed(1)}%`, sub: "planned vs actual",    color: routeCompliancePct >= 80 ? "green" : "yellow" },
  ];

  const today = format(new Date(), "EEEE, d MMMM yyyy");

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Dashboard"
        subtitle={today}
        actions={
          <button
            onClick={() => window.location.reload()}
            className="btn-secondary btn-sm"
          >
            <Icon name="arrow-path" className="w-4 h-4" />
            Muat Ulang
          </button>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-6">
        {isLoading ? (
          <div className="space-y-6">
            <SkeletonStatCards count={4} />
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              <div className="card space-y-3">
                <Skeleton className="h-5 w-32" />
                {[1,2,3].map(i => <div key={i} className="flex gap-3"><Skeleton className="w-14 h-14 rounded-full" /><div className="flex-1 space-y-2"><Skeleton className="h-4 w-24" /><Skeleton className="h-3 w-32" /></div></div>)}
              </div>
              <div className="card xl:col-span-2"><Skeleton className="h-5 w-40 mb-4" /><Skeleton className="h-52 w-full" /></div>
            </div>
          </div>
        ) : (
          <>
            {/* ── KPI Row ── */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              {kpis.map((k, i) => <KpiCard key={i} {...k} />)}
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Comply Target */}
              <div className="card xl:col-span-1">
                <div className="flex items-start justify-between mb-5">
                  <div>
                    <SectionTitle>Comply Target</SectionTitle>
                    <p className="text-xs text-slate-500 mt-0.5">Bulan berjalan per brand</p>
                  </div>
                  <a href="/target-management"
                    className="text-xs text-primary-600 hover:underline font-medium shrink-0">
                    Kelola →
                  </a>
                </div>
                <div className="space-y-4">
                  {complyBrands.length === 0 && (
                    <EmptyState icon="chart-bar" title="Belum ada data target" description="Tidak ada data target brand bulan ini." />
                  )}
                  {complyBrands.map((c) => (
                    <div key={c.brand} className="flex items-center gap-3">
                      <ComplyGauge pct={c.comply_pct} status={c.comply_status} />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between gap-2">
                          <p className="text-sm font-semibold text-slate-800 truncate">{c.brand}</p>
                          <StatusBadge status={c.comply_status} />
                        </div>
                        <p className="text-xs text-slate-400 mt-1">
                          Mgmt: Rp {(c.management_target / 1e6).toFixed(1)}M &middot; SPV: Rp {(c.spv_target / 1e6).toFixed(1)}M
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* EC Rate chart — leaderboard data, top 8 */}
              <div className="card xl:col-span-2">
                <div className="flex items-start justify-between mb-5">
                  <div>
                    <SectionTitle>EC Rate per Salesman</SectionTitle>
                    <p className="text-xs text-slate-500 mt-0.5">MTD, diurutkan by kunjungan terbanyak</p>
                  </div>
                  <a href="/route-evaluate"
                    className="text-xs text-primary-600 hover:underline font-medium shrink-0">
                    Evaluate →
                  </a>
                </div>
                {leaderboard.length === 0 ? (
                  <EmptyState icon="users" title="Belum ada data" description="Belum ada data kunjungan bulan ini." />
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart
                      data={leaderboard.slice(0, 8)}
                      layout="vertical"
                      margin={{ left: 100, right: 40 }}
                    >
                      <XAxis
                        type="number" domain={[0, 100]}
                        tickFormatter={(v) => `${v}%`}
                        tick={{ fontSize: 11, fill: "#64748b" }}
                        axisLine={false} tickLine={false}
                      />
                      <YAxis
                        type="category" dataKey="salesman_name"
                        tick={{ fontSize: 11, fill: "#475569" }}
                        width={100} axisLine={false} tickLine={false}
                      />
                      <Tooltip
                        formatter={(v: number, _: string, props: any) => [
                          `${v.toFixed(1)}% EC rate (${props.payload.visit_mtd} kunjungan)`,
                          props.payload.salesman_name,
                        ]}
                        contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #e2e8f0" }}
                      />
                      <Bar dataKey="ec_rate" radius={[0, 4, 4, 0]} maxBarSize={20}>
                        {leaderboard.slice(0, 8).map((r, i) => (
                          <Cell key={i}
                            fill={r.ec_rate >= 80 ? "#10b981" : r.ec_rate >= 60 ? "#2563eb" : "#ef4444"}
                          />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </div>
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Route Compliance gauge */}
              <div className="card flex flex-col gap-3 py-6">
                <div>
                  <SectionTitle>Route Compliance</SectionTitle>
                  <p className="text-xs text-slate-500 mt-0.5">MTD — planned vs. actual</p>
                </div>
                <div className="flex flex-col items-center gap-2">
                  <div className="relative w-28 h-28">
                    <svg viewBox="0 0 36 36" className="w-28 h-28 -rotate-90">
                      <circle cx="18" cy="18" r="15.9" fill="none" stroke="#e2e8f0" strokeWidth="2.5" />
                      <circle
                        cx="18" cy="18" r="15.9" fill="none"
                        stroke={routeCompliancePct >= 80 ? "#10b981" : routeCompliancePct >= 60 ? "#f59e0b" : "#ef4444"}
                        strokeWidth="2.5"
                        strokeDasharray={`${Math.min(routeCompliancePct, 100)} 100`}
                        strokeLinecap="round"
                      />
                    </svg>
                    <div className="absolute inset-0 flex flex-col items-center justify-center">
                      <span className="text-2xl font-bold text-slate-900">{routeCompliancePct.toFixed(0)}%</span>
                      <span className="text-xs text-slate-400">Compliance</span>
                    </div>
                  </div>
                  <p className="text-xs text-slate-400 text-center max-w-[180px]">
                    Kunjungan terlaksana dibanding rencana kunjungan bulan ini
                  </p>
                </div>
              </div>

              {/* Leaderboard */}
              <div className="card xl:col-span-2 overflow-x-auto">
                <div className="mb-5">
                  <SectionTitle>Leaderboard Tim</SectionTitle>
                  <p className="text-xs text-slate-500 mt-0.5">Top 10 MTD by jumlah kunjungan</p>
                </div>
                {leaderboard.length === 0 ? (
                  <div className="empty-state">
                    <p className="empty-state-text">Belum ada data kunjungan.</p>
                  </div>
                ) : (
                  <table className="w-full text-sm">
                    <thead>
                      <tr>
                        <th className="text-left pb-3 w-8">#</th>
                        <th className="text-left pb-3">Salesman</th>
                        <th className="text-right pb-3">Kunjungan</th>
                        <th className="text-right pb-3">EC</th>
                        <th className="text-right pb-3">EC Rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {leaderboard.map((row, i) => (
                        <tr key={row.salesman_sk}>
                          <td className="py-2.5">
                            <span className={`text-xs font-bold ${
                              i === 0 ? "text-amber-500" :
                              i === 1 ? "text-slate-400" :
                              i === 2 ? "text-amber-700" : "text-slate-300"
                            }`}>{row.rank}</span>
                          </td>
                          <td className="py-2.5 font-medium text-slate-800">{row.salesman_name}</td>
                          <td className="py-2.5 text-right text-slate-600">{row.visit_mtd}</td>
                          <td className="py-2.5 text-right text-slate-600">{(row as any).ec_mtd ?? "—"}</td>
                          <td className="py-2.5 text-right">
                            <span className={
                              row.ec_rate >= 80 ? "text-emerald-600 font-semibold" :
                              row.ec_rate >= 60 ? "text-primary-600 font-medium" : "text-red-500"
                            }>
                              {row.ec_rate.toFixed(1)}%
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>

            {/* ── Announcements ── */}
            <div className="card">
              <div className="flex items-center justify-between mb-5">
                <SectionTitle>Feed Pengumuman</SectionTitle>
                <a href="/announcements"
                  className="text-xs text-primary-600 hover:underline font-medium">
                  Lihat semua →
                </a>
              </div>
              {announcements.length === 0 ? (
                <div className="empty-state py-8">
                  <p className="empty-state-text">Belum ada pengumuman.</p>
                </div>
              ) : (
                <div className="space-y-3">
                  {announcements.slice(0, 3).map((a, i) => (
                    <div key={i}
                      className="flex gap-3 p-3.5 bg-slate-50 rounded-lg border border-slate-100
                                 hover:border-slate-200 transition-colors duration-150">
                      <span className="badge-blue shrink-0 self-start mt-0.5">{a.type}</span>
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-semibold text-slate-800">{a.title}</p>
                        <p className="text-xs text-slate-500 mt-1 line-clamp-2 leading-relaxed">{a.body}</p>
                        <p className="text-xs text-slate-400 mt-1.5">
                          {format(new Date(a.created_at), "d MMM yyyy")}
                        </p>
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
