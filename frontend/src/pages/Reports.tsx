import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, SkeletonTable } from "@/components/ui";
import { toast } from "@/store/toastStore";
import { api } from "@/api/client";

const REPORT_TYPES = ["Achievement", "Route Compliance", "Sell-In YTD", "Effective Call Rate"];
const PERIODS = ["Bulan Ini", "Bulan Lalu", "Kuartal Ini", "YTD", "Semua"];
const TIERS = ["Semua Tier", "S", "A", "B", "C", "D"];

const fetchReport = (type: string, period: string, tier: string) =>
  api.get("/reports", { params: { type, period, tier } }).then((r) => r.data);

export default function Reports() {
  const [activeReport, setActiveReport] = useState("Achievement");
  const [period, setPeriod]   = useState("Bulan Ini");
  const [tier, setTier]       = useState("Semua Tier");
  const [exporting, setExporting] = useState(false);

  const { data, isLoading } = useQuery({
    queryKey: ["reports", activeReport, period, tier],
    queryFn:  () => fetchReport(activeReport, period, tier),
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const rows = data?.rows ?? [];
  const kpis = data?.kpis ?? [];

  const handleExport = async () => {
    setExporting(true);
    try {
      const res = await api.get("/reports/export.csv", {
        params: { type: activeReport, period, tier },
        responseType: "blob",
      });
      const filename = `${activeReport.replace(/\s+/g, "-").toLowerCase()}-${period}.csv`;
      const url = URL.createObjectURL(res.data as Blob);
      const a   = document.createElement("a");
      a.href = url; a.download = filename; a.click();
      URL.revokeObjectURL(url);
      toast.success(`${activeReport} berhasil diunduh.`);
    } catch {
      toast.error("Gagal mengunduh laporan.");
    } finally {
      setExporting(false);
    }
  };

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Reports"
        actions={
          <button
            className="btn-secondary text-sm"
            onClick={handleExport}
            disabled={exporting}
          >
            {exporting
              ? <Icon name="arrow-path" className="w-3.5 h-3.5 animate-spin" />
              : <Icon name="arrow-down-tray" className="w-3.5 h-3.5" />}
            Export CSV
          </button>
        }
      />

      <div className="flex flex-1 min-h-0">
        {/* ── Sidebar report list ── */}
        <aside className="w-52 border-r border-slate-200 bg-white p-3 space-y-1" aria-label="Daftar laporan">
          <p className="text-xs font-medium text-slate-400 px-2 py-1 uppercase tracking-wide">
            Laporan
          </p>
          {REPORT_TYPES.map((r) => (
            <button
              key={r}
              onClick={() => setActiveReport(r)}
              aria-pressed={activeReport === r}
              className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors ${
                activeReport === r
                  ? "bg-primary-50 text-primary-700 font-medium"
                  : "text-slate-600 hover:bg-slate-50"
              }`}
            >
              {r}
            </button>
          ))}
        </aside>

        {/* ── Main ── */}
        <main className="flex-1 overflow-y-auto p-6 space-y-5">
          {/* Filter bar */}
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex gap-1 flex-wrap">
              {PERIODS.map((p) => (
                <button
                  key={p}
                  onClick={() => setPeriod(p)}
                  className={`chip ${period === p ? "chip-active" : ""}`}
                  aria-pressed={period === p}
                >
                  {p}
                </button>
              ))}
            </div>
            <div className="w-px h-5 bg-slate-200 hidden sm:block" />
            <div className="flex gap-1 flex-wrap">
              {TIERS.map((t) => (
                <button
                  key={t}
                  onClick={() => setTier(t)}
                  className={`chip ${tier === t ? "chip-active" : ""}`}
                  aria-pressed={tier === t}
                  aria-label={t === "Semua Tier" ? t : `Tier ${t}`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>

          {/* KPI row */}
          {kpis.length > 0 && (
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
              {kpis.map((k: { label: string; value: string }) => (
                <div key={k.label} className="kpi-tile">
                  <div className="min-w-0 flex-1">
                    <p className="kpi-tile-value">{k.value}</p>
                    <p className="kpi-tile-label">{k.label}</p>
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Table */}
          <div className="card">
            <h2 className="font-semibold text-slate-800 mb-4">
              {activeReport} — {period}
            </h2>

            {isLoading ? (
              <SkeletonTable rows={7} cols={5} />
            ) : rows.length === 0 ? (
              <EmptyState
                icon="table-cells"
                title="Belum ada data"
                description="Tidak ada data untuk kombinasi filter ini."
              />
            ) : (
              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      <th>#</th>
                      {Object.keys(rows[0])
                        .filter((k) => k !== "salesman_sk")
                        .map((k) => (
                          <th key={k}>{k.replace(/_/g, " ")}</th>
                        ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row: Record<string, unknown>, i: number) => (
                      <tr key={String(row.salesman_sk ?? row.outlet_id ?? i)}>
                        <td className="text-slate-400 tabular-nums">{i + 1}</td>
                        {Object.entries(row)
                          .filter(([k]) => k !== "salesman_sk")
                          .map(([k, v]) => (
                            <td key={k}>{String(v ?? "—")}</td>
                          ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}
