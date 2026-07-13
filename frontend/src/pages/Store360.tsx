import { useState, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, Skeleton, SkeletonStatCards } from "@/components/ui";
import { api } from "@/api/client";
import { format } from "date-fns";
import { useDebounce } from "@/hooks/useDebounce";

const searchOutlet = (q: string) =>
  api.get("/outlet/search", { params: { q } }).then((r) => r.data);
const fetchStore360 = (id: string) =>
  api.get(`/store/360/${id}`).then((r) => r.data);

function Store360Skeleton() {
  return (
    <div className="space-y-5" aria-hidden="true">
      <div className="card flex items-start gap-5">
        <Skeleton className="w-14 h-14 rounded-2xl shrink-0" />
        <div className="flex-1 space-y-2">
          <Skeleton className="h-5 w-48" />
          <Skeleton className="h-4 w-64" />
          <div className="flex gap-2 mt-2">
            <Skeleton className="h-5 w-14 rounded-full" />
            <Skeleton className="h-5 w-16 rounded-full" />
          </div>
        </div>
      </div>
      <SkeletonStatCards count={4} />
      <div className="card">
        <Skeleton className="h-5 w-40 mb-4" />
        <div className="space-y-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </div>
      </div>
    </div>
  );
}

export default function Store360() {
  const [query, setQuery]           = useState("");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef     = useRef<HTMLInputElement>(null);
  const debouncedQuery = useDebounce(query, 300);

  // Close dropdown when clicking outside the search container
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const { data: suggestions = [], isFetching: searching } = useQuery({
    queryKey: ["outlet-search", debouncedQuery],
    queryFn:  () => searchOutlet(debouncedQuery),
    enabled:  debouncedQuery.length >= 2,
    staleTime: 5 * 60 * 1000,
  });

  const { data: storeData, isLoading } = useQuery({
    queryKey: ["store360", selectedId],
    queryFn:  () => fetchStore360(selectedId!),
    enabled:  !!selectedId,
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const s = storeData;

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Store 360°" />

      <main className="flex-1 overflow-y-auto p-6 space-y-5">
        {/* ── Search ── */}
        <div className="relative max-w-sm" ref={containerRef}>
          <Icon
            name="magnifying-glass"
            className="w-4 h-4 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
            aria-hidden={true}
          />
          <input
            ref={inputRef}
            className="input w-full text-sm pl-8"
            placeholder="Cari toko (min 2 huruf)..."
            aria-label="Cari toko"
            role="combobox"
            aria-expanded={dropdownOpen && debouncedQuery.length >= 2 && suggestions.length > 0}
            aria-autocomplete="list"
            aria-controls="store360-listbox"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setSelectedId(null);
              setDropdownOpen(true);
            }}
            onFocus={() => { if (debouncedQuery.length >= 2) setDropdownOpen(true); }}
            onKeyDown={(e) => { if (e.key === "Escape") { setDropdownOpen(false); inputRef.current?.blur(); } }}
          />
          {searching && (
            <Icon
              name="arrow-path"
              className="w-4 h-4 text-slate-400 absolute right-2.5 top-1/2 -translate-y-1/2 animate-spin"
              aria-hidden={true}
            />
          )}
          {dropdownOpen && debouncedQuery.length >= 2 && suggestions.length > 0 && (
            <div
              id="store360-listbox"
              className="absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-lg border border-slate-200 z-20 max-h-48 overflow-y-auto"
              role="listbox"
              aria-label="Hasil pencarian toko"
            >
              {(
                suggestions as {
                  outlet_id: string;
                  store_name: string;
                  source_outlet_code: string;
                }[]
              ).map((s) => (
                <button
                  key={s.outlet_id}
                  role="option"
                  aria-selected={selectedId === s.outlet_id}
                  className="w-full text-left px-4 py-3 text-sm hover:bg-slate-50 border-b border-slate-50 last:border-none"
                  onClick={() => {
                    setSelectedId(s.outlet_id);
                    setQuery(s.store_name);
                    setDropdownOpen(false);
                  }}
                >
                  <p className="font-medium text-slate-700">{s.store_name}</p>
                  <p className="text-xs text-slate-400">{s.source_outlet_code}</p>
                </button>
              ))}
            </div>
          )}
          {dropdownOpen && debouncedQuery.length >= 2 && !searching && suggestions.length === 0 && (
            <div
              className="absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-lg border border-slate-200 z-20"
              aria-live="polite"
            >
              <p className="px-4 py-3 text-sm text-slate-400 text-center">
                Tidak ada toko ditemukan untuk &ldquo;{debouncedQuery}&rdquo;
              </p>
            </div>
          )}
        </div>

        {/* ── Empty / loading / data states ── */}
        {!selectedId && (
          <EmptyState
            icon="building-storefront"
            title="Cari toko"
            description="Cari dan pilih toko untuk melihat profil lengkap"
          />
        )}

        {isLoading && <Store360Skeleton />}

        {s && (
          <div className="space-y-5">
            {/* Header */}
            <div className="card flex items-start gap-5">
              <div className="w-14 h-14 rounded-2xl bg-primary-100 text-primary-600 text-2xl font-bold flex items-center justify-center shrink-0">
                {s.store_name?.[0] ?? "T"}
              </div>
              <div className="flex-1">
                <h2 className="text-xl font-bold text-slate-800">{s.store_name}</h2>
                <p className="text-sm text-slate-500 mt-0.5">
                  {s.source_outlet_code} · {s.kecamatan} · {s.city}
                </p>
                <div className="flex items-center gap-2 mt-2">
                  <span className="badge-gray">{s.tier ?? "—"}</span>
                  <span className="badge-blue">{s.channel ?? "—"}</span>
                  <span className={s.is_active ? "badge-green" : "badge-gray"}>
                    {s.is_active ? "Aktif" : "Non-Aktif"}
                  </span>
                </div>
              </div>
              <div className="text-right text-sm">
                <p className="text-xs text-slate-400">Salesman</p>
                <p className="font-medium text-slate-700">{s.salesman_name ?? "—"}</p>
                <p className="text-xs text-slate-400 mt-1">SPV</p>
                <p className="font-medium text-slate-700">{s.spv_name ?? "—"}</p>
              </div>
            </div>

            {/* KPI row */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              {[
                { label: "Visit MTD",          value: s.visit_mtd ?? 0,                         icon: "map-pin"      as const, cls: "icon-badge-blue"   },
                { label: "Effective Call MTD",  value: s.effective_call_mtd ?? 0,                icon: "check-circle" as const, cls: "icon-badge-green"  },
                { label: "Sell-In MTD (pcs)",  value: (s.sellin_mtd ?? 0).toLocaleString("id"), icon: "truck"        as const, cls: "icon-badge-indigo" },
                { label: "Sell-In YTD (pcs)",  value: (s.sellin_ytd ?? 0).toLocaleString("id"), icon: "chart-bar"    as const, cls: "icon-badge-purple" },
              ].map((k) => (
                <div key={k.label} className="kpi-tile">
                  <span className={`icon-badge ${k.cls} shrink-0`}>
                    <Icon name={k.icon} className="w-4 h-4" />
                  </span>
                  <div className="min-w-0 flex-1">
                    <p className="kpi-tile-value">{k.value}</p>
                    <p className="kpi-tile-label">{k.label}</p>
                  </div>
                </div>
              ))}
            </div>

            {/* Visit history */}
            <div className="card">
              <h3 className="font-semibold text-slate-800 mb-4">Riwayat Kunjungan Terakhir</h3>
              {!s.visits?.length ? (
                <EmptyState
                  icon="calendar"
                  title="Belum ada riwayat"
                  description="Belum ada kunjungan yang tercatat untuk toko ini."
                />
              ) : (
                <div className="table-container">
                  <table className="table">
                    <thead>
                      <tr>
                        {["Tanggal", "Salesman", "Check-In", "Check-Out", "Sell-In (pcs)", "Status"].map((h) => (
                          <th key={h}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {s.visits.map((v: Record<string, string>) => (
                        <tr key={v.visit_id}>
                          <td>{v.visit_date}</td>
                          <td>{v.salesman_name}</td>
                          <td className="text-slate-500">
                            {v.checkin_time ? format(new Date(v.checkin_time), "HH:mm") : "—"}
                          </td>
                          <td className="text-slate-500">
                            {v.checkout_time ? format(new Date(v.checkout_time), "HH:mm") : "—"}
                          </td>
                          <td className="tabular-nums">
                            {Number(v.total_demand ?? 0).toLocaleString("id")}
                          </td>
                          <td>
                            <span className={Number(v.total_demand) > 0 ? "badge-green" : "badge-gray"}>
                              {Number(v.total_demand) > 0 ? "EC" : "Kunjungan"}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {/* PJP */}
            {s.pjp_schedule && (
              <div className="card">
                <h3 className="font-semibold text-slate-800 mb-3">Jadwal PJP</h3>
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <p className="text-xs text-slate-400">Hari Kunjungan</p>
                    <p className="font-medium text-slate-700">{s.pjp_schedule.visit_day_of_week}</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400">Frekuensi</p>
                    <p className="font-medium text-slate-700">{s.pjp_schedule.visit_frequency_code}</p>
                  </div>
                  <div>
                    <p className="text-xs text-slate-400">Pola Minggu</p>
                    <p className="font-medium text-slate-700">
                      {s.pjp_schedule.visit_week_pattern ?? "Semua"}
                    </p>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}
