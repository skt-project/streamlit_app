import { useMemo } from "react";
import { Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import TopNav from "@/components/layout/TopNav";
import { Icon, SkeletonStatCards, Skeleton, EmptyState } from "@/components/ui";
import { api } from "@/api/client";
import { format } from "date-fns";
import { id as idLocale } from "date-fns/locale";
import type { ComplyBrand, LeaderboardRow } from "@/types";

// ── API ───────────────────────────────────────────────────────────────────────
interface DashboardRaw {
  comply_brands?: { brand: string; management_target: number; spv_target: number; comply_pct: number }[];
  leaderboard?:   { salesman_sk: string; salesman_name?: string; visit_mtd: number; ec_rate: number }[];
  route_comply_pct?: number;
  visit_today?:   number;
  ec_today?:      number;
  comply_pct?:    number;
  announcements?: { type: string; title: string; body: string; created_at: string }[];
}

const fetchDashboard = (): Promise<DashboardRaw> => api.get("/dashboard/web").then((r) => r.data);

function complyStatus(pct: number): ComplyBrand["comply_status"] {
  if (pct >= 100) return "Over Target";
  if (pct >= 80)  return "Comply";
  if (pct > 0)    return "Under Comply";
  return "No Data";
}

// ── Announcement type → color mapping ─────────────────────────────────────────
function announcementColors(type: string): { badge: string; rail: string } {
  const t = (type ?? "").toUpperCase();
  if (t === "WARNING" || t === "URGENT")   return { badge: "badge-yellow", rail: "rail-amber"  };
  if (t === "ALERT"   || t === "CRITICAL") return { badge: "badge-red",    rail: "rail-red"    };
  if (t === "SUCCESS" || t === "UPDATE")   return { badge: "badge-green",  rail: "rail-green"  };
  if (t === "PROMO")                       return { badge: "badge-purple", rail: "rail-purple" };
  return { badge: "badge-blue", rail: "rail-blue" };
}

// ── KPI Tile (Gojek service-icon style) ───────────────────────────────────────
interface KpiTileProps {
  label: string;
  value: string | number;
  sub?: string;
  icon: React.ReactNode;
  iconCls?: string;
  trend?: { value: number; label?: string };
}

function KpiTile({ label, value, sub, icon, iconCls = "icon-badge-blue", trend }: KpiTileProps) {
  return (
    <div className="kpi-tile hover-lift">
      <div className={`${iconCls} icon-badge icon-badge-lg shrink-0`}>
        {icon}
      </div>
      <div className="min-w-0 flex-1">
        <p className="kpi-tile-value">{value}</p>
        <p className="kpi-tile-label">{label}</p>
        {trend != null && (
          <p className={trend.value >= 0 ? "kpi-tile-delta-up" : "kpi-tile-delta-down"}>
            <Icon
              name={trend.value >= 0 ? "arrow-trending-up" : "arrow-trending-down"}
              className="w-3.5 h-3.5"
            />
            {Math.abs(trend.value)}%
            {trend.label && <span className="font-normal ml-1 text-slate-400">{trend.label}</span>}
          </p>
        )}
        {sub && !trend && <p className="kpi-tile-delta text-slate-400">{sub}</p>}
      </div>
    </div>
  );
}

// ── Comply Gauge ───────────────────────────────────────────────────────────────
function ComplyGauge({ pct, status }: { pct: number; status: string }) {
  const color =
    status === "Over Target" ? "#2563eb" :
    status === "Comply"      ? "#10b981" : "#ef4444";
  return (
    <div className="relative w-12 h-12 shrink-0" role="img" aria-label={`${pct.toFixed(0)}% ${status}`}>
      <svg viewBox="0 0 36 36" className="w-12 h-12 -rotate-90" aria-hidden="true">
        <circle cx="18" cy="18" r="15.9" fill="none" stroke="#e2e8f0" strokeWidth="3" />
        <circle
          cx="18" cy="18" r="15.9" fill="none"
          stroke={color} strokeWidth="3"
          strokeDasharray={`${Math.min(pct, 100)} 100`}
          strokeLinecap="round"
        />
      </svg>
      <span className="absolute inset-0 flex items-center justify-center text-[9px] font-bold text-slate-700 tabular-nums" aria-hidden="true">
        {pct.toFixed(0)}%
      </span>
    </div>
  );
}

function StatusBadge({ status }: { status: string }) {
  if (status === "Comply")       return <span className="badge-green">{status}</span>;
  if (status === "Over Target")  return <span className="badge-blue">{status}</span>;
  if (status === "Under Comply") return <span className="badge-red">{status}</span>;
  return <span className="badge-gray">{status}</span>;
}

// ── Main ───────────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const { data, isLoading, refetch, isFetching } = useQuery<DashboardRaw>({
    queryKey: ["dashboard-web"],
    queryFn: fetchDashboard,
    staleTime: 5 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const complyBrands: ComplyBrand[] = useMemo(
    () =>
      (data?.comply_brands ?? []).map((r) => ({
        brand:             r.brand,
        management_target: r.management_target ?? 0,
        spv_target:        r.spv_target ?? 0,
        comply_pct:        r.comply_pct ?? 0,
        comply_status:     complyStatus(r.comply_pct ?? 0),
      })),
    [data],
  );

  const leaderboard: (LeaderboardRow & { visit_mtd: number; ec_rate: number })[] = useMemo(
    () =>
      (data?.leaderboard ?? []).map((r, i) => ({
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

  const routeCompliancePct: number = data?.route_comply_pct ?? 0;
  const visitToday: number         = data?.visit_today ?? 0;
  const ecToday: number            = data?.ec_today ?? 0;
  const overallComplyPct: number   = data?.comply_pct ?? 0;

  const announcements: { type: string; title: string; body: string; created_at: string }[] =
    data?.announcements ?? [];

  const today = format(new Date(), "EEEE, d MMMM yyyy", { locale: idLocale });

  return (
    <div className="flex flex-col h-full">
      {/* ── Top Nav ── */}
      <TopNav
        title="Dashboard"
        subtitle={today}
        actions={
          <button
            onClick={() => refetch()}
            className="btn-secondary btn-sm"
            disabled={isFetching}
            aria-busy={isFetching}
            aria-label={isFetching ? "Memuat data..." : "Muat Ulang"}
          >
            <Icon name="arrow-path" className={`w-4 h-4 ${isFetching ? "animate-spin" : ""}`} aria-hidden={true} />
            {isFetching ? "Memuat..." : "Muat Ulang"}
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
                {[1,2,3].map(i => (
                  <div key={i} className="flex gap-3">
                    <Skeleton className="w-12 h-12 rounded-full" />
                    <div className="flex-1 space-y-2"><Skeleton className="h-4 w-24" /><Skeleton className="h-3 w-32" /></div>
                  </div>
                ))}
              </div>
              <div className="card xl:col-span-2"><Skeleton className="h-5 w-40 mb-4" /><Skeleton className="h-52 w-full" /></div>
            </div>
          </div>
        ) : (
          <>
            {/* ── KPI Tiles (Gojek-style) ── */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              <KpiTile
                label="Kunjungan Hari Ini"
                value={visitToday}
                sub="total visit"
                icon={<Icon name="building-storefront" className="w-5 h-5" />}
                iconCls="icon-badge-blue"
              />
              <KpiTile
                label="EC Hari Ini"
                value={ecToday}
                sub="effective call"
                icon={<Icon name="check-circle" className="w-5 h-5" />}
                iconCls="icon-badge-green"
              />
              <KpiTile
                label="Comply MTD"
                value={`${overallComplyPct.toFixed(1)}%`}
                sub="vs management target"
                icon={<Icon name="chart-pie" className="w-5 h-5" />}
                iconCls={overallComplyPct >= 80 ? "icon-badge-green" : "icon-badge-red"}
              />
              <KpiTile
                label="Route Compliance"
                value={`${routeCompliancePct.toFixed(1)}%`}
                sub="planned vs actual"
                icon={<Icon name="map" className="w-5 h-5" />}
                iconCls={routeCompliancePct >= 80 ? "icon-badge-green" : "icon-badge-amber"}
              />
            </div>

            {/* ── Comply + Chart row ── */}
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Comply Target Card */}
              <div className="card">
                <div className="section-heading mb-5">
                  <div>
                    <p className="section-heading-title">Comply Target</p>
                    <p className="section-heading-sub">Bulan berjalan per brand</p>
                  </div>
                  <Link to="/target-management" className="section-heading-action" aria-label="Kelola Target Management">
                    Kelola →
                  </Link>
                </div>

                {complyBrands.length === 0 ? (
                  <EmptyState icon="chart-bar" title="Belum ada target" description="Tidak ada data target brand bulan ini." />
                ) : (
                  <div className="space-y-4">
                    {complyBrands.map((c) => (
                      <div key={c.brand} className="flex items-center gap-3">
                        <ComplyGauge pct={c.comply_pct} status={c.comply_status} />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center justify-between gap-2">
                            <p className="text-sm font-semibold text-slate-800 truncate">{c.brand}</p>
                            <StatusBadge status={c.comply_status} />
                          </div>
                          <div className="progress-track mt-2" aria-hidden="true">
                            <div
                              className={`progress-fill ${c.comply_pct >= 80 ? "progress-fill-green" : c.comply_pct >= 50 ? "progress-fill-amber" : "progress-fill-red"}`}
                              style={{ width: `${Math.min(c.comply_pct, 100)}%` }}
                            />
                          </div>
                          <p className="text-2xs text-slate-400 mt-1">
                            Mgmt: Rp {(c.management_target / 1e6).toFixed(1)}M · SPV: Rp {(c.spv_target / 1e6).toFixed(1)}M
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* EC Rate Chart */}
              <div className="card xl:col-span-2">
                <div className="section-heading mb-5">
                  <div>
                    <p className="section-heading-title">EC Rate per Salesman</p>
                    <p className="section-heading-sub">MTD, diurutkan by kunjungan terbanyak</p>
                  </div>
                  <Link to="/route-evaluate" className="section-heading-action" aria-label="Pergi ke Route Evaluate">Evaluate →</Link>
                </div>

                {leaderboard.length === 0 ? (
                  <EmptyState icon="users" title="Belum ada data" description="Belum ada data kunjungan bulan ini." />
                ) : (
                  <div role="img" aria-label="Grafik EC Rate per salesman MTD, diurutkan berdasarkan jumlah kunjungan terbanyak">
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={leaderboard.slice(0, 8)} layout="vertical" margin={{ left: 100, right: 48 }}>
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
                          `${v.toFixed(1)}% EC (${props.payload.visit_mtd} kunjungan)`,
                          props.payload.salesman_name,
                        ]}
                        contentStyle={{ fontSize: 12, borderRadius: 10, border: "1px solid #e2e8f0", boxShadow: "0 4px 16px rgba(0,0,0,0.06)" }}
                      />
                      <Bar dataKey="ec_rate" radius={[0, 6, 6, 0]} maxBarSize={18}>
                        {leaderboard.slice(0, 8).map((r) => (
                          <Cell key={r.salesman_sk}
                            fill={r.ec_rate >= 80 ? "#10b981" : r.ec_rate >= 60 ? "#2563eb" : "#ef4444"}
                          />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                  </div>
                )}
              </div>
            </div>

            {/* ── Route + Leaderboard row ── */}
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              {/* Route Compliance Gauge */}
              <div className="card flex flex-col items-center gap-4 py-8">
                <div>
                  <p className="section-heading-title text-center">Route Compliance</p>
                  <p className="section-heading-sub text-center mt-0.5">MTD — planned vs. actual</p>
                </div>
                <div className="relative w-32 h-32" role="img" aria-label={`Route compliance ${routeCompliancePct.toFixed(0)}%`}>
                  <svg viewBox="0 0 36 36" className="w-32 h-32 -rotate-90" aria-hidden="true">
                    <circle cx="18" cy="18" r="15.9" fill="none" stroke="#e2e8f0" strokeWidth="2.5" />
                    <circle
                      cx="18" cy="18" r="15.9" fill="none"
                      stroke={routeCompliancePct >= 80 ? "#10b981" : routeCompliancePct >= 60 ? "#f59e0b" : "#ef4444"}
                      strokeWidth="2.5"
                      strokeDasharray={`${Math.min(routeCompliancePct, 100)} 100`}
                      strokeLinecap="round"
                    />
                  </svg>
                  <div className="absolute inset-0 flex flex-col items-center justify-center" aria-hidden="true">
                    <span className="text-3xl font-bold text-slate-900 tabular-nums">{routeCompliancePct.toFixed(0)}%</span>
                    <span className="text-2xs text-slate-400 uppercase tracking-wide">Compliance</span>
                  </div>
                </div>
                <p className="text-xs text-slate-400 text-center max-w-[180px] leading-relaxed">
                  Kunjungan terlaksana dibanding rencana kunjungan bulan ini
                </p>
              </div>

              {/* Leaderboard */}
              <div className="card xl:col-span-2">
                <div className="section-heading mb-4">
                  <div>
                    <p className="section-heading-title">Leaderboard Tim</p>
                    <p className="section-heading-sub">Top MTD by jumlah kunjungan</p>
                  </div>
                </div>
                {leaderboard.length === 0 ? (
                  <EmptyState icon="trophy" title="Belum ada data" description="Belum ada data kunjungan." />
                ) : (
                  <div className="table-container">
                    <table className="table">
                      <thead>
                        <tr>
                          <th className="w-8">#</th>
                          <th>Salesman</th>
                          <th className="text-right">Kunjungan</th>
                          <th className="text-right">EC Rate</th>
                        </tr>
                      </thead>
                      <tbody>
                        {leaderboard.slice(0, 8).map((row, i) => (
                          <tr key={row.salesman_sk}>
                            <td>
                              <span className={`text-xs font-bold tabular-nums ${
                                i === 0 ? "text-amber-500" :
                                i === 1 ? "text-slate-400" :
                                i === 2 ? "text-amber-700" : "text-slate-300"
                              }`}>{row.rank}</span>
                            </td>
                            <td className="font-medium text-slate-800">{row.salesman_name}</td>
                            <td className="text-right text-slate-600 tabular-nums">{row.visit_mtd}</td>
                            <td className="text-right">
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
                  </div>
                )}
              </div>
            </div>

            {/* ── Announcements ── */}
            <div className="card">
              <div className="section-heading mb-4">
                <div>
                  <p className="section-heading-title">Feed Pengumuman</p>
                </div>
                <Link to="/announcements" className="section-heading-action" aria-label="Lihat semua pengumuman">Lihat semua →</Link>
              </div>

              {announcements.length === 0 ? (
                <EmptyState icon="megaphone" title="Belum ada pengumuman" description="Pengumuman terbaru akan muncul di sini." className="py-8" />
              ) : (
                <div className="space-y-3">
                  {announcements.slice(0, 3).map((a) => {
                    const { badge, rail } = announcementColors(a.type);
                    return (
                      <div key={`${a.type}-${a.created_at}`} className={`${rail} hover-lift p-4 rounded-xl`}>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <span className={badge}>{a.type}</span>
                            <span className="text-2xs text-slate-400">
                              {format(new Date(a.created_at), "d MMM yyyy")}
                            </span>
                          </div>
                          <p className="text-sm font-semibold text-slate-800">{a.title}</p>
                          <p className="text-xs text-slate-500 mt-1 line-clamp-2 leading-relaxed">{a.body}</p>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </>
        )}
      </main>
    </div>
  );
}
