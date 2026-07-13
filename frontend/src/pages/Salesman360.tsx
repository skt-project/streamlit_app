import { useState, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, Skeleton, SkeletonStatCards } from "@/components/ui";
import { api } from "@/api/client";
import { format } from "date-fns";
import { useDebounce } from "@/hooks/useDebounce";

const searchSalesman = (q: string) =>
  api.get("/salesman/search", { params: { q } }).then((r) => r.data);
const fetchSalesman360 = (sk: string) =>
  api.get(`/salesman/360/${sk}`).then((r) => r.data);

function Salesman360Skeleton() {
  return (
    <div className="space-y-5" aria-hidden="true">
      <div className="card flex items-start gap-5">
        <Skeleton className="w-14 h-14 rounded-full shrink-0" />
        <div className="flex-1 space-y-2">
          <Skeleton className="h-5 w-44" />
          <Skeleton className="h-4 w-56" />
          <div className="flex gap-2 mt-1">
            <Skeleton className="h-5 w-14 rounded-full" />
            <Skeleton className="h-5 w-20 rounded-full" />
          </div>
        </div>
      </div>
      <SkeletonStatCards count={4} />
    </div>
  );
}

export default function Salesman360() {
  const [query, setQuery]           = useState("");
  const [selectedSk, setSelectedSk] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef     = useRef<HTMLInputElement>(null);
  const debouncedQuery = useDebounce(query, 300);

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
    queryKey: ["salesman-search", debouncedQuery],
    queryFn:  () => searchSalesman(debouncedQuery),
    enabled:  debouncedQuery.length >= 2,
    staleTime: 5 * 60 * 1000,
  });

  const { data: d, isLoading } = useQuery({
    queryKey: ["salesman360", selectedSk],
    queryFn:  () => fetchSalesman360(selectedSk!),
    enabled:  !!selectedSk,
    staleTime: 2 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Salesman 360°" />

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
            placeholder="Cari nama atau ID salesman..."
            aria-label="Cari salesman"
            role="combobox"
            aria-expanded={dropdownOpen && debouncedQuery.length >= 2 && suggestions.length > 0}
            aria-autocomplete="list"
            aria-controls="salesman360-listbox"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setSelectedSk(null);
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
              id="salesman360-listbox"
              className="absolute top-full left-0 right-0 mt-1 bg-white rounded-xl shadow-lg border border-slate-200 z-20 max-h-48 overflow-y-auto"
              role="listbox"
              aria-label="Hasil pencarian salesman"
            >
              {(
                suggestions as {
                  salesman_sk: string;
                  salesman_name: string;
                  source_salesman_code: string;
                }[]
              ).map((s) => (
                <button
                  key={s.salesman_sk}
                  role="option"
                  aria-selected={selectedSk === s.salesman_sk}
                  className="w-full text-left px-4 py-3 text-sm hover:bg-slate-50 border-b border-slate-50 last:border-none"
                  onClick={() => {
                    setSelectedSk(s.salesman_sk);
                    setQuery(s.salesman_name);
                    setDropdownOpen(false);
                  }}
                >
                  <p className="font-medium text-slate-700">{s.salesman_name}</p>
                  <p className="text-xs text-slate-400">{s.source_salesman_code}</p>
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
                Tidak ada salesman ditemukan untuk &ldquo;{debouncedQuery}&rdquo;
              </p>
            </div>
          )}
        </div>

        {/* ── Empty / loading / data states ── */}
        {!selectedSk && (
          <EmptyState
            icon="user"
            title="Cari salesman"
            description="Cari dan pilih salesman untuk melihat profil dan performa lengkapnya"
          />
        )}

        {isLoading && <Salesman360Skeleton />}

        {d && (
          <div className="space-y-5">
            {/* ── Header card ── */}
            <div className="card flex items-start gap-5">
              <div className="w-14 h-14 rounded-full bg-primary-100 text-primary-600 text-2xl font-bold flex items-center justify-center shrink-0">
                {d.salesman_name?.[0] ?? "S"}
              </div>
              <div className="flex-1">
                <h2 className="text-xl font-bold text-slate-800">{d.salesman_name}</h2>
                <p className="text-sm text-slate-500 mt-0.5">
                  {d.source_salesman_code} · {d.salesman_type}
                </p>
                <div className="flex items-center gap-2 mt-2">
                  <span className={d.is_active ? "badge-green" : "badge-gray"}>
                    {d.is_active ? "Aktif" : "Non-Aktif"}
                  </span>
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

            {/* ── KPI row ── */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
              {[
                { label: "Visit Today",    value: d.visit_today ?? 0,                         icon: "map-pin"       as const, cls: "icon-badge-blue"   },
                { label: "EC Today",       value: d.ec_today ?? 0,                            icon: "check-circle"  as const, cls: "icon-badge-green"  },
                { label: "Visit MTD",      value: d.visit_mtd ?? 0,                           icon: "calendar-days" as const, cls: "icon-badge-indigo" },
                { label: "Route Comply %", value: `${(d.route_comply_pct ?? 0).toFixed(1)}%`, icon: "chart-pie"     as const, cls: "icon-badge-purple" },
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

            {/* ── Today's schedule ── */}
            <div className="card">
              <h3 className="font-semibold text-slate-800 mb-4">Jadwal Hari Ini</h3>
              {!d.today_schedule?.length ? (
                <EmptyState icon="calendar" title="Tidak ada jadwal hari ini" />
              ) : (
                <div className="space-y-2">
                  {(d.today_schedule as Record<string, string>[]).map((r, idx) => (
                    <div
                      key={r.route_plan_sk ?? r.outlet_sk}
                      className="flex items-center justify-between py-2 border-b border-slate-50"
                    >
                      <div className="flex items-center gap-3">
                        <div
                          className={`w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold ${
                            r.status === "visited"
                              ? "bg-green-100 text-green-600"
                              : "bg-slate-100 text-slate-400"
                          }`}
                        >
                          {Number(r.sequence_order) || idx + 1}
                        </div>
                        <div>
                          <p className="text-sm font-medium text-slate-700">{r.store_name}</p>
                          <p className="text-xs text-slate-400">{r.source_outlet_code}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        {r.checkin_time && (
                          <p className="text-xs text-slate-500">
                            In: {format(new Date(r.checkin_time), "HH:mm")}
                          </p>
                        )}
                        <span
                          className={
                            r.status === "visited"
                              ? Number(r.total_demand) > 0 ? "badge-green" : "badge-gray"
                              : "badge-yellow"
                          }
                        >
                          {r.status === "visited"
                            ? Number(r.total_demand) > 0 ? "EC" : "Kunjungan"
                            : "Belum dikunjungi"}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* ── Outlets assigned ── */}
            <div className="card">
              <h3 className="font-semibold text-slate-800 mb-1">
                Toko Terdaftar ({d.total_outlets ?? 0})
              </h3>
              <p className="text-xs text-slate-400 mb-4">
                Semua toko yang ditugaskan ke salesman ini
              </p>
              {!d.outlets?.length ? (
                <EmptyState icon="building-storefront" title="Belum ada toko terdaftar" />
              ) : (
                <div className="table-container">
                  <table className="table">
                    <thead>
                      <tr>
                        {["Kode", "Nama Toko", "Kecamatan", "Tier", "Visit MTD", "EC MTD"].map((h) => (
                          <th key={h}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(d.outlets as Record<string, string>[]).map((o) => (
                        <tr key={o.outlet_sk}>
                          <td className="font-mono text-xs text-slate-500">{o.source_outlet_code}</td>
                          <td>{o.store_name}</td>
                          <td>{o.kecamatan ?? "—"}</td>
                          <td><span className="badge-gray text-xs">{o.tier ?? "—"}</span></td>
                          <td className="tabular-nums">{o.visit_mtd ?? 0}</td>
                          <td className="tabular-nums">{o.ec_mtd ?? 0}</td>
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
