import { useQuery } from "@tanstack/react-query";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import { useAuthStore } from "@/store/authStore";
import type { ComplyBrand, LeaderboardRow } from "@/types";
import { format } from "date-fns";

// ── API ───────────────────────────────────────────────────────────────────────
const fetchDashboard = () => api.get("/dashboard/web").then((r) => r.data);

// ── KPI Card ─────────────────────────────────────────────────────────────────
const ACCENT: Record<string, { border: string; value: string; icon: string }> = {
  blue:   { border: "border-l-blue-500",    value: "text-blue-600",    icon: "📊" },
  green:  { border: "border-l-emerald-500", value: "text-emerald-600", icon: "✅" },
  yellow: { border: "border-l-amber-500",   value: "text-amber-600",   icon: "⚡" },
  red:    { border: "border-l-red-500",     value: "text-red-600",     icon: "⚠️" },
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
    status === "Comply"
      ? "#10b981"
      : status === "Over Target"
      ? "#2563eb"
      : "#ef4444";
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

// ── Section title helper ──────────────────────────────────────────────────────
function SectionTitle({ children }: { children: React.ReactNode }) {
  return <h2 className="text-base font-semibold text-slate-900">{children}</h2>;
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const user = useAuthStore((s) => s.user);
  const { data, isLoading } = useQuery({
    queryKey: ["dashboard-web"],
    queryFn: fetchDashboard,
  });

  const comply: ComplyBrand[] = data?.comply ?? [];
  const leaderboard: LeaderboardRow[] = data?.leaderboard ?? [];
  const routeCompliancePct: number = data?.route_compliance_avg ?? 0;
  const achievementRows: { salesman_name: string; achievement_pct: number }[] =
    data?.achievement_vs_target ?? [];
  const announcements: {
    type: string;
    title: string;
    body: string;
    created_at: string;
  }[] = data?.announcements ?? [];
  const kpis = data?.kpis ?? [];

  const today = format(new Date(), "EEEE, d MMMM yyyy");

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Dashboard"
        subtitle={today}
        actions={
          <button
            onClick={() => window.location.reload()}
            className="btn-secondary text-sm px-3 py-1.5"
          >
            ↻ Muat Ulang
          </button>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-6">
        {isLoading ? (
          <div className="flex items-center justify-center h-48 text-slate-400 text-sm">
            Memuat data...
          </div>
        ) : (
          <>
            {/* ── KPI Row ── */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              {kpis.map(
                (
                  k: { label: string; value: string; sub?: string; color?: string },
                  i: number,
                ) => (
                  <KpiCard key={i} {...k} />
                ),
              )}
            </div>

            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Comply Target */}
              <div className="card xl:col-span-1">
                <div className="flex items-start justify-between mb-5">
                  <div>
                    <SectionTitle>Comply Target</SectionTitle>
                    <p className="text-xs text-slate-500 mt-0.5">Bulan berjalan</p>
                  </div>
                  <a
                    href="/target-management"
                    className="text-xs text-primary-600 hover:text-primary-700 hover:underline font-medium shrink-0"
                  >
                    Kelola →
                  </a>
                </div>
                <div className="space-y-4">
                  {comply.length === 0 && (
                    <p className="text-sm text-slate-400 text-center py-4">
                      Belum ada data target.
                    </p>
                  )}
                  {comply.map((c) => (
                    <div key={c.brand} className="flex items-center gap-3">
                      <ComplyGauge pct={c.comply_pct} status={c.comply_status} />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between gap-2">
                          <p className="text-sm font-semibold text-slate-800 truncate">
                            {c.brand}
                          </p>
                          <StatusBadge status={c.comply_status} />
                        </div>
                        <p className="text-xs text-slate-400 mt-1">
                          Mgmt: Rp {(c.management_target / 1e6).toFixed(1)}M &middot; SPV: Rp{" "}
                          {(c.spv_target / 1e6).toFixed(1)}M
                        </p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Achievement vs Target chart */}
              <div className="card xl:col-span-2">
                <div className="flex items-start justify-between mb-5">
                  <div>
                    <SectionTitle>Achievement vs Target</SectionTitle>
                    <p className="text-xs text-slate-500 mt-0.5">Per salesman, bulan berjalan</p>
                  </div>
                  <a
                    href="/reports"
                    className="text-xs text-primary-600 hover:text-primary-700 hover:underline font-medium shrink-0"
                  >
                    Reports →
                  </a>
                </div>
                {achievementRows.length === 0 ? (
                  <div className="empty-state">
                    <p className="empty-state-text">Belum ada data achievement.</p>
                  </div>
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={achievementRows} layout="vertical" margin={{ left: 90, right: 16 }}>
                      <XAxis
                        type="number"
                        domain={[0, 120]}
                        tickFormatter={(v) => `${v}%`}
                        tick={{ fontSize: 11, fill: "#64748b" }}
                        axisLine={false}
                        tickLine={false}
                      />
                      <YAxis
                        type="category"
                        dataKey="salesman_name"
                        tick={{ fontSize: 11, fill: "#475569" }}
                        width={90}
                        axisLine={false}
                        tickLine={false}
                      />
                      <Tooltip
                        formatter={(v: number) => [`${v.toFixed(1)}%`, "Achievement"]}
                        contentStyle={{
                          fontSize: 12,
                          borderRadius: 8,
                          border: "1px solid #e2e8f0",
                        }}
                      />
                      <Bar dataKey="achievement_pct" radius={[0, 4, 4, 0]} maxBarSize={20}>
                        {achievementRows.map((r, i) => (
                          <Cell
                            key={i}
                            fill={
                              r.achievement_pct >= 100
                                ? "#10b981"
                                : r.achievement_pct >= 80
                                ? "#2563eb"
                                : "#ef4444"
                            }
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
              <div className="card flex flex-col items-center justify-center gap-3 py-8">
                <div className="self-stretch flex items-start justify-between">
                  <div>
                    <SectionTitle>Route Compliance</SectionTitle>
                    <p className="text-xs text-slate-500 mt-0.5">Rata-rata tim minggu ini</p>
                  </div>
                </div>
                <div className="relative w-28 h-28">
                  <svg viewBox="0 0 36 36" className="w-28 h-28 -rotate-90">
                    <circle cx="18" cy="18" r="15.9" fill="none" stroke="#e2e8f0" strokeWidth="2.5" />
                    <circle
                      cx="18" cy="18" r="15.9" fill="none"
                      stroke={
                        routeCompliancePct >= 80
                          ? "#10b981"
                          : routeCompliancePct >= 60
                          ? "#f59e0b"
                          : "#ef4444"
                      }
                      strokeWidth="2.5"
                      strokeDasharray={`${Math.min(routeCompliancePct, 100)} 100`}
                      strokeLinecap="round"
                    />
                  </svg>
                  <div className="absolute inset-0 flex flex-col items-center justify-center">
                    <span className="text-2xl font-bold text-slate-900">
                      {routeCompliancePct.toFixed(0)}%
                    </span>
                    <span className="text-xs text-slate-400">Compliance</span>
                  </div>
                </div>
                <p className="text-xs text-slate-400 text-center max-w-[180px]">
                  Kunjungan yang direncanakan vs. yang benar-benar terlaksana
                </p>
              </div>

              {/* Leaderboard */}
              <div className="card xl:col-span-2 overflow-x-auto">
                <div className="flex items-start justify-between mb-5">
                  <SectionTitle>Leaderboard Tim</SectionTitle>
                </div>
                {leaderboard.length === 0 ? (
                  <div className="empty-state">
                    <p className="empty-state-text">Belum ada data leaderboard.</p>
                  </div>
                ) : (
                  <table className="w-full text-sm">
                    <thead>
                      <tr>
                        <th className="text-left pb-3 w-8">#</th>
                        <th className="text-left pb-3">Salesman</th>
                        <th className="text-right pb-3">Achievement</th>
                        <th className="text-right pb-3">Compliance</th>
                        <th className="text-right pb-3">Coverage</th>
                      </tr>
                    </thead>
                    <tbody>
                      {leaderboard.map((row, i) => (
                        <tr key={row.salesman_sk}>
                          <td className="py-2.5">
                            <span
                              className={`text-xs font-bold ${
                                i === 0
                                  ? "text-amber-500"
                                  : i === 1
                                  ? "text-slate-400"
                                  : i === 2
                                  ? "text-amber-700"
                                  : "text-slate-300"
                              }`}
                            >
                              {row.rank}
                            </span>
                          </td>
                          <td className="py-2.5 font-medium text-slate-800">
                            {row.salesman_name}
                          </td>
                          <td className="py-2.5 text-right">
                            <span
                              className={
                                row.achievement_pct >= 100
                                  ? "text-emerald-600 font-semibold"
                                  : row.achievement_pct >= 80
                                  ? "text-primary-600 font-medium"
                                  : "text-red-500"
                              }
                            >
                              {row.achievement_pct.toFixed(1)}%
                            </span>
                          </td>
                          <td className="py-2.5 text-right text-slate-600">
                            {row.route_compliance_pct?.toFixed(1) ?? "—"}%
                          </td>
                          <td className="py-2.5 text-right text-slate-600">
                            {row.coverage_pct?.toFixed(1) ?? "—"}%
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>

            {/* ── Announcements feed ── */}
            <div className="card">
              <div className="flex items-center justify-between mb-5">
                <SectionTitle>Feed Pengumuman</SectionTitle>
                <a
                  href="/announcements"
                  className="text-xs text-primary-600 hover:text-primary-700 hover:underline font-medium"
                >
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
                    <div
                      key={i}
                      className="flex gap-3 p-3.5 bg-slate-50 rounded-lg border border-slate-100
                                 hover:border-slate-200 transition-colors duration-150"
                    >
                      <span className="badge-blue shrink-0 self-start mt-0.5">{a.type}</span>
                      <div className="min-w-0 flex-1">
                        <p className="text-sm font-semibold text-slate-800">{a.title}</p>
                        <p className="text-xs text-slate-500 mt-1 line-clamp-2 leading-relaxed">
                          {a.body}
                        </p>
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
