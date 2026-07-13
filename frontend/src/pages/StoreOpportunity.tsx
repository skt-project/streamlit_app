import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, SkeletonTable, SkeletonStatCards } from "@/components/ui";
import { api } from "@/api/client";
import { toast } from "@/store/toastStore";

const fetchOpportunity = (tier: string, brand: string) =>
  api.get("/store-opportunity", { params: { tier: tier || undefined, brand: brand || undefined } }).then((r) => r.data);

export default function StoreOpportunity() {
  const [tier, setTier]   = useState("");
  const [brand, setBrand] = useState("");
  const { data, isLoading } = useQuery({
    queryKey: ["store-opportunity", tier, brand],
    queryFn: () => fetchOpportunity(tier, brand),
    staleTime: 5 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const handleExport = async () => {
    try {
      const res = await (await import("@/api/client")).api.get("/store-opportunity/export", {
        params: { tier: tier || undefined, brand: brand || undefined },
        responseType: "blob",
      });
      const url = URL.createObjectURL(res.data as Blob);
      const a = document.createElement("a");
      a.href = url; a.download = "store_opportunity.csv"; a.click();
      URL.revokeObjectURL(url);
      toast.success("Export berhasil diunduh");
    } catch {
      toast.error("Export gagal. Coba lagi.");
    }
  };

  const rows = data?.rows ?? [];
  const summary = data?.summary ?? {};

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Store Opportunity"
        actions={
          <button className="btn-secondary text-sm flex items-center gap-1.5" onClick={handleExport}>
            <Icon name="arrow-down-tray" className="w-4 h-4" />
            Export CSV
          </button>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-5">
        {/* Summary */}
        {isLoading ? (
          <SkeletonStatCards count={4} />
        ) : (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {[
              { label: "Total Toko Aktif",      value: summary.total_active ?? "—",    icon: "building-storefront" as const, cls: "icon-badge-blue"   },
              { label: "Toko Tier S/A",          value: summary.tier_sa ?? "—",         icon: "star"                as const, cls: "icon-badge-amber"  },
              { label: "Belum Kunjungi MTD",     value: summary.not_visited_mtd ?? "—", icon: "exclamation-circle"  as const, cls: "icon-badge-purple" },
              { label: "Potensi EC Tersisa",     value: summary.potential_ec ?? "—",    icon: "arrow-trending-up"   as const, cls: "icon-badge-green"  },
            ].map((c) => (
              <div key={c.label} className="kpi-tile">
                <span className={`icon-badge ${c.cls}`}>
                  <Icon name={c.icon} className="w-4 h-4" />
                </span>
                <div className="min-w-0 flex-1">
                  <p className="kpi-tile-value">{c.value}</p>
                  <p className="kpi-tile-label">{c.label}</p>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Filters */}
        <div className="flex gap-3 flex-wrap">
          <div className="flex gap-1">
            {["", "S", "A", "B", "C", "D"].map((t) => (
              <button
                key={t || "semua"}
                onClick={() => setTier(t)}
                className={`chip ${tier === t ? "chip-active" : ""}`}
                aria-pressed={tier === t}
                aria-label={t ? `Tier ${t}` : "Semua Tier"}
              >
                {t || "Semua Tier"}
              </button>
            ))}
          </div>
          <div className="flex gap-1">
            {["", "Skintific", "G2G"].map((b) => (
              <button
                key={b || "semua"}
                onClick={() => setBrand(b)}
                className={`chip ${brand === b ? "chip-active" : ""}`}
                aria-pressed={brand === b}
              >
                {b || "Semua Brand"}
              </button>
            ))}
          </div>
        </div>

        {/* Table */}
        <div className="card">
          <h2 className="font-semibold text-slate-800 mb-4">Toko dengan Peluang Tertinggi</h2>
          {isLoading ? (
            <SkeletonTable rows={7} cols={9} />
          ) : rows.length === 0 ? (
            <EmptyState
              icon="building-storefront"
              title="Tidak ada data"
              description="Coba ubah filter tier atau brand"
            />
          ) : (
            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    {["Rank", "Kode Toko", "Nama Toko", "Tier", "Salesman", "Visit MTD", "EC MTD", "Potensi (pcs)", "Gap"].map((h) => (
                      <th key={h}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r: Record<string, string | number>, i: number) => (
                    <tr key={String(r.source_outlet_code ?? i)}>
                      <td className="text-slate-400 text-xs">#{i + 1}</td>
                      <td className="font-mono text-xs text-slate-500">{r.source_outlet_code}</td>
                      <td>{r.store_name}</td>
                      <td><span className="badge-gray text-xs">{r.tier}</span></td>
                      <td>{r.salesman_name}</td>
                      <td className="tabular-nums">{r.visit_mtd}</td>
                      <td className="tabular-nums">{r.ec_mtd}</td>
                      <td className="font-semibold text-primary-600 tabular-nums">{Number(r.potential_demand ?? 0).toLocaleString("id")}</td>
                      <td>
                        <span className={Number(r.gap ?? 0) > 0 ? "badge-red" : "badge-green"}>
                          {Number(r.gap ?? 0) > 0 ? `+${Number(r.gap).toLocaleString("id")}` : "Tercapai"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
