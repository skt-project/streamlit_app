import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";

const fetchOpportunity = (tier: string, brand: string) =>
  api.get("/store-opportunity", { params: { tier: tier || undefined, brand: brand || undefined } }).then((r) => r.data);

export default function StoreOpportunity() {
  const [tier, setTier] = useState("");
  const [brand, setBrand] = useState("");

  const { data, isLoading } = useQuery({
    queryKey: ["store-opportunity", tier, brand],
    queryFn: () => fetchOpportunity(tier, brand),
  });

  const rows = data?.rows ?? [];
  const summary = data?.summary ?? {};

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Store Opportunity"
        actions={<button className="btn-secondary text-sm">Export CSV</button>}
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-5">
        {/* Summary */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {[
            { label: "Total Toko Aktif", value: summary.total_active ?? "—" },
            { label: "Toko Tier S/A", value: summary.tier_sa ?? "—" },
            { label: "Toko Belum Kunjungi MTD", value: summary.not_visited_mtd ?? "—" },
            { label: "Potensi EC Tersisa", value: summary.potential_ec ?? "—" },
          ].map((c) => (
            <div key={c.label} className="card text-center">
              <p className="text-2xl font-bold text-primary-600">{c.value}</p>
              <p className="text-xs text-slate-500 mt-1">{c.label}</p>
            </div>
          ))}
        </div>

        {/* Filters */}
        <div className="flex gap-3 flex-wrap">
          <div className="flex gap-1">
            {["", "S", "A", "B", "C", "D"].map((t) => (
              <button
                key={t || "semua"}
                onClick={() => setTier(t)}
                className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-colors ${tier === t ? "bg-slate-700 text-white" : "bg-white border border-slate-200 text-slate-600 hover:border-slate-400"}`}
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
                className={`px-3 py-1.5 text-xs rounded-lg font-medium transition-colors ${brand === b ? "bg-primary-600 text-white" : "bg-white border border-slate-200 text-slate-600 hover:border-primary-300"}`}
              >
                {b || "Semua Brand"}
              </button>
            ))}
          </div>
        </div>

        {/* Table */}
        <div className="card overflow-x-auto">
          <h2 className="font-semibold text-slate-800 mb-4">Toko dengan Peluang Tertinggi</h2>
          {isLoading ? (
            <p className="text-sm text-slate-400">Memuat data...</p>
          ) : rows.length === 0 ? (
            <p className="text-sm text-slate-400">Tidak ada data.</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  {["Rank", "Kode Toko", "Nama Toko", "Tier", "Salesman", "Visit MTD", "EC MTD", "Potensi (pcs)", "Gap"].map((h) => (
                    <th key={h} className="text-left py-2 text-xs font-medium text-slate-400">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r: Record<string, string | number>, i: number) => (
                  <tr key={i} className="border-b border-slate-50 hover:bg-slate-50">
                    <td className="py-2.5 text-slate-400 text-xs">#{i + 1}</td>
                    <td className="py-2.5 font-mono text-xs text-slate-500">{r.source_outlet_code}</td>
                    <td className="py-2.5 font-medium text-slate-700">{r.store_name}</td>
                    <td className="py-2.5"><span className="badge-gray text-xs">{r.tier}</span></td>
                    <td className="py-2.5 text-slate-500">{r.salesman_name}</td>
                    <td className="py-2.5 text-slate-700">{r.visit_mtd}</td>
                    <td className="py-2.5 text-slate-700">{r.ec_mtd}</td>
                    <td className="py-2.5 font-semibold text-primary-600">{Number(r.potential_demand ?? 0).toLocaleString("id")}</td>
                    <td className="py-2.5">
                      <span className={Number(r.gap ?? 0) > 0 ? "badge-red" : "badge-green"}>
                        {Number(r.gap ?? 0) > 0 ? `+${Number(r.gap).toLocaleString("id")}` : "Tercapai"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </main>
    </div>
  );
}
