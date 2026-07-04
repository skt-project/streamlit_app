import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import { format } from "date-fns";

const searchSalesman = (q: string) =>
  api.get("/salesman/search", { params: { q } }).then((r) => r.data);
const fetchSalesman360 = (sk: string) =>
  api.get(`/salesman/360/${sk}`).then((r) => r.data);

export default function Salesman360() {
  const [query, setQuery] = useState("");
  const [selectedSk, setSelectedSk] = useState<string | null>(null);

  const { data: suggestions = [], isFetching: searching } = useQuery({
    queryKey: ["salesman-search", query],
    queryFn: () => searchSalesman(query),
    enabled: query.length >= 2,
  });

  const { data: d, isLoading } = useQuery({
    queryKey: ["salesman360", selectedSk],
    queryFn: () => fetchSalesman360(selectedSk!),
    enabled: !!selectedSk,
  });

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Salesman 360°" />

      <main className="flex-1 overflow-y-auto p-6 space-y-5">
        {/* Search */}
        <div className="relative max-w-sm">
          <input
            className="input w-full text-sm"
            placeholder="Cari nama atau ID salesman..."
            value={query}
            onChange={(e) => { setQuery(e.target.value); setSelectedSk(null); }}
          />
          {query.length >= 2 && suggestions.length > 0 && (
            <div className="absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-lg border border-slate-200 z-20 max-h-48 overflow-y-auto">
              {(suggestions as {salesman_sk: string; salesman_name: string; source_salesman_code: string}[]).map((s) => (
                <button
                  key={s.salesman_sk}
                  className="w-full text-left px-4 py-3 text-sm hover:bg-slate-50 border-b border-slate-50 last:border-none"
                  onClick={() => { setSelectedSk(s.salesman_sk); setQuery(s.salesman_name); }}
                >
                  <p className="font-medium text-slate-700">{s.salesman_name}</p>
                  <p className="text-xs text-slate-400">{s.source_salesman_code}</p>
                </button>
              ))}
            </div>
          )}
          {searching && <p className="text-xs text-slate-400 mt-1">Mencari...</p>}
        </div>

        {!selectedSk && (
          <div className="text-center py-20 text-slate-400">
            <p className="text-5xl mb-3">👤</p>
            <p>Cari dan pilih salesman untuk melihat profil lengkap</p>
          </div>
        )}

        {isLoading && <p className="text-sm text-slate-400">Memuat...</p>}

        {d && (
          <div className="space-y-5">
            {/* Header */}
            <div className="card flex items-start gap-5">
              <div className="w-14 h-14 rounded-full bg-primary-100 text-primary-600 text-2xl font-bold flex items-center justify-center shrink-0">
                {d.salesman_name?.[0] ?? "S"}
              </div>
              <div className="flex-1">
                <h2 className="text-xl font-bold text-slate-800">{d.salesman_name}</h2>
                <p className="text-sm text-slate-500 mt-0.5">{d.source_salesman_code} · {d.salesman_type}</p>
                <div className="flex items-center gap-2 mt-2">
                  <span className={d.is_active ? "badge-green" : "badge-gray"}>{d.is_active ? "Aktif" : "Non-Aktif"}</span>
                  <span className="badge-gray">{d.region ?? "—"}</span>
                </div>
              </div>
              <div className="text-right text-sm">
                <p className="text-xs text-slate-400">SPV</p>
                <p className="font-medium text-slate-700">{d.spv_name ?? "—"}</p>
                <p className="text-xs text-slate-400 mt-1">ASM</p>
                <p className="font-medium text-slate-700">{d.asm_name ?? "—"}</p>
              </div>
            </div>

            {/* KPI row */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              {[
                { label: "Visit Today", value: d.visit_today ?? 0, color: "text-primary-600" },
                { label: "EC Today", value: d.ec_today ?? 0, color: "text-green-600" },
                { label: "Visit MTD", value: d.visit_mtd ?? 0, color: "text-blue-600" },
                { label: "Route Comply %", value: `${(d.route_comply_pct ?? 0).toFixed(1)}%`, color: "text-indigo-600" },
              ].map((k) => (
                <div key={k.label} className="card text-center">
                  <p className={`text-2xl font-bold ${k.color}`}>{k.value}</p>
                  <p className="text-xs text-slate-500 mt-1">{k.label}</p>
                </div>
              ))}
            </div>

            {/* Today's schedule */}
            <div className="card">
              <h3 className="font-semibold text-slate-800 mb-4">Jadwal Hari Ini</h3>
              {!d.today_schedule?.length ? (
                <p className="text-sm text-slate-400">Tidak ada jadwal hari ini.</p>
              ) : (
                <div className="space-y-2">
                  {d.today_schedule.map((r: Record<string, string>, i: number) => (
                    <div key={i} className="flex items-center justify-between py-2 border-b border-slate-50">
                      <div className="flex items-center gap-3">
                        <div className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold ${r.status === "visited" ? "bg-green-100 text-green-600" : "bg-slate-100 text-slate-400"}`}>
                          {Number(r.sequence_order) || i + 1}
                        </div>
                        <div>
                          <p className="text-sm font-medium text-slate-700">{r.store_name}</p>
                          <p className="text-xs text-slate-400">{r.source_outlet_code}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        {r.checkin_time && <p className="text-xs text-slate-500">In: {format(new Date(r.checkin_time), "HH:mm")}</p>}
                        <span className={r.status === "visited" ? (Number(r.total_demand) > 0 ? "badge-green" : "badge-gray") : "badge-yellow"}>
                          {r.status === "visited" ? (Number(r.total_demand) > 0 ? "EC" : "Kunjungan") : "Belum dikunjungi"}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Outlets assigned */}
            <div className="card">
              <h3 className="font-semibold text-slate-800 mb-2">Toko Terdaftar ({d.total_outlets ?? 0})</h3>
              <p className="text-xs text-slate-400 mb-4">Semua toko yang ditugaskan ke salesman ini</p>
              {!d.outlets?.length ? (
                <p className="text-sm text-slate-400">Belum ada toko terdaftar.</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-slate-100">
                        {["Kode", "Nama Toko", "Kecamatan", "Tier", "Visit MTD", "EC MTD"].map((h) => (
                          <th key={h} className="text-left py-2 text-xs font-medium text-slate-400">{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {d.outlets.map((o: Record<string, string>, i: number) => (
                        <tr key={i} className="border-b border-slate-50 hover:bg-slate-50">
                          <td className="py-2 font-mono text-xs text-slate-500">{o.source_outlet_code}</td>
                          <td className="py-2 text-slate-700">{o.store_name}</td>
                          <td className="py-2 text-slate-500">{o.kecamatan ?? "—"}</td>
                          <td className="py-2"><span className="badge-gray text-xs">{o.tier ?? "—"}</span></td>
                          <td className="py-2 text-slate-700">{o.visit_mtd ?? 0}</td>
                          <td className="py-2 text-slate-700">{o.ec_mtd ?? 0}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
