import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import { format } from "date-fns";

const searchOutlet = (q: string) =>
  api.get("/outlet/search", { params: { q } }).then((r) => r.data);
const fetchStore360 = (id: string) =>
  api.get(`/store/360/${id}`).then((r) => r.data);

export default function Store360() {
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);

  const { data: suggestions = [], isFetching: searching } = useQuery({
    queryKey: ["outlet-search", query],
    queryFn: () => searchOutlet(query),
    enabled: query.length >= 2,
  });

  const { data: storeData, isLoading } = useQuery({
    queryKey: ["store360", selectedId],
    queryFn: () => fetchStore360(selectedId!),
    enabled: !!selectedId,
  });

  const s = storeData;

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Store 360°" />

      <main className="flex-1 overflow-y-auto p-6 space-y-5">
        {/* Search */}
        <div className="relative max-w-sm">
          <input
            className="input w-full text-sm"
            placeholder="Cari toko (min 2 huruf)..."
            value={query}
            onChange={(e) => { setQuery(e.target.value); setSelectedId(null); }}
          />
          {query.length >= 2 && suggestions.length > 0 && (
            <div className="absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-lg border border-slate-200 z-20 max-h-48 overflow-y-auto">
              {(suggestions as {outlet_id: string; store_name: string; source_outlet_code: string}[]).map((s) => (
                <button
                  key={s.outlet_id}
                  className="w-full text-left px-4 py-3 text-sm hover:bg-slate-50 border-b border-slate-50 last:border-none"
                  onClick={() => { setSelectedId(s.outlet_id); setQuery(s.store_name); }}
                >
                  <p className="font-medium text-slate-700">{s.store_name}</p>
                  <p className="text-xs text-slate-400">{s.source_outlet_code}</p>
                </button>
              ))}
            </div>
          )}
          {searching && <p className="text-xs text-slate-400 mt-1">Mencari...</p>}
        </div>

        {!selectedId && (
          <div className="text-center py-20 text-slate-400">
            <p className="text-5xl mb-3">🏪</p>
            <p>Cari dan pilih toko untuk melihat profil lengkap</p>
          </div>
        )}

        {isLoading && <p className="text-sm text-slate-400">Memuat...</p>}

        {s && (
          <div className="space-y-5">
            {/* Header */}
            <div className="card flex items-start gap-5">
              <div className="w-14 h-14 rounded-2xl bg-primary-100 text-primary-600 text-2xl font-bold flex items-center justify-center shrink-0">
                {s.store_name?.[0] ?? "T"}
              </div>
              <div className="flex-1">
                <h2 className="text-xl font-bold text-slate-800">{s.store_name}</h2>
                <p className="text-sm text-slate-500 mt-0.5">{s.source_outlet_code} · {s.kecamatan} · {s.city}</p>
                <div className="flex items-center gap-2 mt-2">
                  <span className="badge-gray">{s.tier ?? "—"}</span>
                  <span className="badge-blue">{s.channel ?? "—"}</span>
                  <span className={s.is_active ? "badge-green" : "badge-gray"}>{s.is_active ? "Aktif" : "Non-Aktif"}</span>
                </div>
              </div>
              <div className="text-right">
                <p className="text-xs text-slate-400">Salesman</p>
                <p className="font-medium text-slate-700">{s.salesman_name ?? "—"}</p>
                <p className="text-xs text-slate-400 mt-1">SPV</p>
                <p className="font-medium text-slate-700">{s.spv_name ?? "—"}</p>
              </div>
            </div>

            {/* KPI row */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              {[
                { label: "Visit MTD", value: s.visit_mtd ?? 0, color: "text-primary-600" },
                { label: "Effective Call MTD", value: s.effective_call_mtd ?? 0, color: "text-green-600" },
                { label: "Sell-In MTD (pcs)", value: (s.sellin_mtd ?? 0).toLocaleString("id"), color: "text-blue-600" },
                { label: "Sell-In YTD (pcs)", value: (s.sellin_ytd ?? 0).toLocaleString("id"), color: "text-indigo-600" },
              ].map((k) => (
                <div key={k.label} className="card text-center">
                  <p className={`text-2xl font-bold ${k.color}`}>{k.value}</p>
                  <p className="text-xs text-slate-500 mt-1">{k.label}</p>
                </div>
              ))}
            </div>

            {/* Visit history */}
            <div className="card">
              <h3 className="font-semibold text-slate-800 mb-4">Riwayat Kunjungan Terakhir</h3>
              {!s.visits?.length ? (
                <p className="text-sm text-slate-400">Belum ada riwayat kunjungan.</p>
              ) : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-slate-100">
                      {["Tanggal", "Salesman", "Check-In", "Check-Out", "Sell-In (pcs)", "Status"].map((h) => (
                        <th key={h} className="text-left py-2 text-xs font-medium text-slate-400">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {s.visits.map((v: Record<string, string>, i: number) => (
                      <tr key={i} className="border-b border-slate-50 hover:bg-slate-50">
                        <td className="py-2 text-slate-700">{v.visit_date}</td>
                        <td className="py-2 text-slate-700">{v.salesman_name}</td>
                        <td className="py-2 text-slate-500">{v.checkin_time ? format(new Date(v.checkin_time), "HH:mm") : "—"}</td>
                        <td className="py-2 text-slate-500">{v.checkout_time ? format(new Date(v.checkout_time), "HH:mm") : "—"}</td>
                        <td className="py-2 text-slate-700">{Number(v.total_demand ?? 0).toLocaleString("id")}</td>
                        <td className="py-2">
                          <span className={Number(v.total_demand) > 0 ? "badge-green" : "badge-gray"}>
                            {Number(v.total_demand) > 0 ? "EC" : "Kunjungan"}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            {/* PJP */}
            {s.pjp_schedule && (
              <div className="card">
                <h3 className="font-semibold text-slate-800 mb-3">Jadwal PJP</h3>
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div><p className="text-xs text-slate-400">Hari Kunjungan</p><p className="font-medium text-slate-700">{s.pjp_schedule.visit_day_of_week}</p></div>
                  <div><p className="text-xs text-slate-400">Frekuensi</p><p className="font-medium text-slate-700">{s.pjp_schedule.visit_frequency_code}</p></div>
                  <div><p className="text-xs text-slate-400">Pola Minggu</p><p className="font-medium text-slate-700">{s.pjp_schedule.visit_week_pattern ?? "Semua"}</p></div>
                </div>
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}
