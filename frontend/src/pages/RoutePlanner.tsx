import { useRef, useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, Skeleton, EmptyState } from "@/components/ui";
import { toast } from "@/store/toastStore";
import { api } from "@/api/client";
import { useFocusTrap } from "@/hooks/useFocusTrap";
import type { SalesmanRoute, RouteStore, DayId } from "@/types";
import { format, startOfISOWeek, addDays } from "date-fns";
import { id } from "date-fns/locale";
import { useDebounce } from "@/hooks/useDebounce";

interface StoreResult {
  outlet_sk: number;
  source_outlet_code: string;
  store_name: string;
  store_grade: string | null;
  region: string | null;
}

const searchRoutePlannerStores = (q: string): Promise<StoreResult[]> =>
  api.get("/route-planner/stores", { params: { q } }).then((r) => r.data);

const DAYS: DayId[] = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"];

const fetchSalesmenRoutes = (week: string) =>
  api.get("/route-planner/salesmen", { params: { week } }).then((r) => r.data);

function gradeBadge(grade: string | null) {
  if (!grade) return null;
  const map: Record<string, string> = {
    S: "badge-blue", A: "badge-green", B: "badge-yellow", C: "badge-gray", D: "badge-red",
  };
  return <span className={map[grade] ?? "badge-gray"}>Tier {grade}</span>;
}

function StoreCard({ store, onRemove }: { store: RouteStore; onRemove: () => void }) {
  return (
    <div className="flex items-center gap-3 p-3 bg-white border border-slate-100 rounded-lg hover:shadow-sm transition-shadow group">
      <div className="w-6 h-6 rounded-full bg-primary-100 text-primary-600 text-xs font-bold flex items-center justify-center shrink-0">
        {store.sequence_no}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <p className="text-sm font-medium text-slate-700 truncate">{store.store_name}</p>
          {gradeBadge(store.store_grade)}
        </div>
        <p className="text-xs text-slate-400">{store.source_outlet_code}</p>
      </div>
      <button
        onClick={onRemove}
        className="opacity-0 group-hover:opacity-100 focus-visible:opacity-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-400 text-slate-300 hover:text-red-500 transition-all p-1 rounded"
        aria-label={`Hapus ${store.store_name}`}
      >
        <Icon name="x-mark" className="w-4 h-4" />
      </button>
    </div>
  );
}

function SalesmanRailSkeleton() {
  return (
    <div className="divide-y divide-slate-50" aria-hidden="true">
      {Array.from({ length: 7 }).map((_, i) => (
        <div key={i} className="p-3 flex items-center gap-2">
          <Skeleton className="w-8 h-8 rounded-full shrink-0" />
          <div className="flex-1 space-y-1.5">
            <Skeleton className="h-3.5 w-28" />
            <Skeleton className="h-3 w-16" />
          </div>
        </div>
      ))}
    </div>
  );
}

export default function RoutePlanner() {
  const qc = useQueryClient();
  const today = new Date();
  const [weekStart, setWeekStart]   = useState(startOfISOWeek(today));
  const [selectedSalesmanSk, setSelectedSalesmanSk] = useState<string | null>(null);
  const [selectedDay, setSelectedDay]   = useState<DayId>("Senin");
  const [showAddModal, setShowAddModal] = useState(false);
  const modalTriggerRef = useRef<Element | null>(null);
  const modalPanelRef = useRef<HTMLDivElement>(null);
  useFocusTrap(modalPanelRef, showAddModal);
  const closeAddModal = () => {
    setShowAddModal(false);
    setSearchStore("");
    setTimeout(() => { (modalTriggerRef.current as HTMLElement | null)?.focus(); }, 0);
  };
  const [salesmanSearch, setSalesmanSearch] = useState("");
  const [searchStore, setSearchStore]   = useState("");
  const debouncedSearch      = useDebounce(salesmanSearch, 250);
  const debouncedSearchStore = useDebounce(searchStore, 300);

  const weekLabel = format(weekStart, "'Minggu' w, d MMM", { locale: id });
  const weekKey   = format(weekStart, "yyyy-'W'II");

  const { data: salesmen = [], isLoading } = useQuery<SalesmanRoute[]>({
    queryKey: ["route-planner", weekKey],
    queryFn:  () => fetchSalesmenRoutes(weekKey),
    staleTime: 5 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const filteredSalessmen = useMemo(
    () =>
      debouncedSearch
        ? salesmen.filter((s) =>
            s.salesman_name.toLowerCase().includes(debouncedSearch.toLowerCase())
          )
        : salesmen,
    [salesmen, debouncedSearch],
  );

  const selected   = salesmen.find((s) => s.salesman_sk === selectedSalesmanSk) ?? salesmen[0];
  const dayStores: RouteStore[] = selected?.stores_per_day?.[selectedDay] ?? [];

  const prevWeek = () => setWeekStart((d) => addDays(d, -7));
  const nextWeek = () => setWeekStart((d) => addDays(d, 7));

  const { data: storeResults = [], isFetching: searchingStores } = useQuery<StoreResult[]>({
    queryKey: ["route-planner-store-search", debouncedSearchStore],
    queryFn:  () => searchRoutePlannerStores(debouncedSearchStore),
    enabled:  debouncedSearchStore.length >= 2,
    staleTime: 5 * 60 * 1000,
  });

  const removeStoreMutation = useMutation({
    mutationFn: ({ salesmanSk, routePlanSk }: { salesmanSk: string; routePlanSk: string }) =>
      api.delete(`/route-planner/assignment/${routePlanSk}`, { params: { salesman_sk: salesmanSk } }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["route-planner"] });
      toast.success("Toko berhasil dihapus dari rute.");
    },
    onError: () => toast.error("Gagal menghapus toko dari rute."),
  });

  const addStoreMutation = useMutation({
    mutationFn: (store: StoreResult) =>
      api.post("/route-planner/assignment", {
        salesman_sk:     parseInt(selected!.salesman_sk),
        outlet_sk:       store.outlet_sk,
        day_of_week:     selectedDay,
        sequence_order:  (dayStores.length + 1),
      }),
    onSuccess: (_data, store) => {
      qc.invalidateQueries({ queryKey: ["route-planner"] });
      toast.success(`${store.store_name} ditambahkan ke rute ${selectedDay}.`);
      closeAddModal();
    },
    onError: () => toast.error("Gagal menambahkan toko ke rute."),
  });

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Route Planner"
        actions={
          <button onClick={() => { modalTriggerRef.current = document.activeElement; setShowAddModal(true); }} className="btn-primary text-sm" disabled={!selected}>
            <Icon name="plus" className="w-4 h-4" />
            Tambah Store
          </button>
        }
      />

      <div className="flex flex-1 min-h-0">
        {/* ── Left: Salesman Rail ── */}
        <aside className="w-64 border-r border-slate-200 bg-white flex flex-col" aria-label="Daftar salesman">
          <div className="p-3 border-b border-slate-100 shrink-0">
            <div className="relative">
              <Icon
                name="magnifying-glass"
                className="w-4 h-4 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
              />
              <input
                className="input text-sm pl-8"
                placeholder="Cari salesman..."
                aria-label="Cari salesman"
                value={salesmanSearch}
                onChange={(e) => setSalesmanSearch(e.target.value)}
              />
            </div>
          </div>

          <div className="flex-1 overflow-y-auto">
            {isLoading ? (
              <SalesmanRailSkeleton />
            ) : filteredSalessmen.length === 0 ? (
              <p className="p-4 text-sm text-slate-400 text-center">
                {debouncedSearch
                  ? `Tidak ada hasil untuk "${debouncedSearch}"`
                  : "Tidak ada data."}
              </p>
            ) : (
              <div className="divide-y divide-slate-50">
                {filteredSalessmen.map((s) => (
                  <button
                    key={s.salesman_sk}
                    onClick={() => setSelectedSalesmanSk(s.salesman_sk)}
                    aria-current={selected?.salesman_sk === s.salesman_sk ? "true" : undefined}
                    aria-label={`${s.salesman_name}, ${s.total_stores} toko, kepatuhan ${s.compliance_pct?.toFixed(0) ?? "—"}%, pencapaian ${s.achievement_pct?.toFixed(0) ?? "—"}%`}
                    className={`w-full text-left p-3 hover:bg-slate-50 transition-colors ${
                      selected?.salesman_sk === s.salesman_sk
                        ? "bg-primary-50 border-r-2 border-primary-600"
                        : ""
                    }`}
                  >
                    <div className="flex items-center gap-2">
                      <div className="w-8 h-8 rounded-full bg-primary-100 text-primary-600 font-bold text-sm flex items-center justify-center shrink-0">
                        {s.salesman_name[0]}
                      </div>
                      <div className="min-w-0">
                        <p className="text-sm font-medium text-slate-700 truncate">{s.salesman_name}</p>
                        <p className="text-xs text-slate-400">{s.total_stores} toko</p>
                      </div>
                    </div>
                    <div className="flex gap-3 mt-1.5 ml-10 text-xs text-slate-500">
                      <span className="flex items-center gap-1">
                        <Icon name="check-circle" className="w-3.5 h-3.5 text-emerald-500" />
                        {s.compliance_pct?.toFixed(0) ?? "—"}%
                      </span>
                      <span className="flex items-center gap-1">
                        <Icon name="arrow-trending-up" className="w-3.5 h-3.5 text-primary-500" />
                        {s.achievement_pct?.toFixed(0) ?? "—"}%
                      </span>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
        </aside>

        {/* ── Center: Route Board ── */}
        <main className="flex-1 flex flex-col min-w-0 overflow-hidden bg-slate-50">
          {/* Week nav */}
          <div className="bg-white border-b border-slate-200 px-5 py-3 flex items-center gap-3">
            <button
              onClick={prevWeek}
              className="btn-secondary p-1.5"
              aria-label="Minggu sebelumnya"
            >
              <Icon name="chevron-left" className="w-4 h-4" />
            </button>
            <span className="text-sm font-medium text-slate-700 min-w-[160px] text-center" aria-live="polite">
              {weekLabel}
            </span>
            <button
              onClick={nextWeek}
              className="btn-secondary p-1.5"
              aria-label="Minggu berikutnya"
            >
              <Icon name="chevron-right" className="w-4 h-4" />
            </button>
          </div>

          {/* Day tabs */}
          <div role="tablist" className="bg-white border-b border-slate-200 flex">
            {DAYS.map((day) => {
              const count = selected?.stores_per_day?.[day]?.length ?? 0;
              return (
                <button
                  key={day}
                  id={`tab-day-${day}`}
                  role="tab"
                  aria-selected={selectedDay === day}
                  aria-controls="panel-day-stores"
                  onClick={() => setSelectedDay(day)}
                  className={`flex-1 py-3 text-sm font-medium transition-colors border-b-2 ${
                    selectedDay === day
                      ? "border-primary-600 text-primary-600"
                      : "border-transparent text-slate-500 hover:text-slate-700"
                  }`}
                >
                  {day.slice(0, 3)}
                  <span
                    className={`ml-1.5 text-xs px-1.5 py-0.5 rounded-full ${
                      count > 0 ? "bg-primary-100 text-primary-700" : "bg-slate-100 text-slate-400"
                    }`}
                  >
                    {count}
                  </span>
                </button>
              );
            })}
          </div>

          {/* Store list */}
          <div id="panel-day-stores" role="tabpanel" aria-labelledby={`tab-day-${selectedDay}`} className="flex-1 overflow-y-auto p-5">
            {!selected ? (
              <EmptyState
                icon="map"
                title="Pilih salesman"
                description="Pilih salesman di panel kiri untuk melihat rute kunjungannya."
              />
            ) : dayStores.length === 0 ? (
              <EmptyState
                icon="calendar-days"
                title={`Belum ada toko untuk ${selectedDay}`}
                description="Tambahkan toko ke jadwal kunjungan hari ini."
                action={
                  <button onClick={() => setShowAddModal(true)} className="btn-primary">
                    <Icon name="plus" className="w-4 h-4" />
                    Tambah Store
                  </button>
                }
              />
            ) : (
              <div className="space-y-2 max-w-lg">
                {dayStores.map((store) => (
                  <StoreCard
                    key={store.route_plan_sk}
                    store={store}
                    onRemove={() =>
                      removeStoreMutation.mutate({
                        salesmanSk:  selected.salesman_sk,
                        routePlanSk: store.route_plan_sk,
                      })
                    }
                  />
                ))}
              </div>
            )}
          </div>

          {/* Bottom bar */}
          <div className="bg-white border-t border-slate-200 px-5 py-3 flex items-center justify-between">
            <p className="text-sm text-slate-500">
              {selected
                ? `${selected.salesman_name} — ${dayStores.length} toko ${selectedDay}`
                : "—"}
            </p>
            <div className="flex gap-2">
              <button className="btn-secondary text-sm">Simpan Draft</button>
              <button className="btn-primary text-sm">Submit</button>
            </div>
          </div>
        </main>
      </div>

      {/* ── Add Store Modal ── */}
      {showAddModal && (
        <div
          className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4"
          onClick={(e) => { if (e.target === e.currentTarget) closeAddModal(); }}
        >
          <div ref={modalPanelRef} role="dialog" aria-modal="true" aria-labelledby="add-store-modal-title" className="bg-white rounded-2xl shadow-2xl w-full max-w-md">
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 id="add-store-modal-title" className="font-semibold text-slate-800">Tambah Store ke Rute</h3>
              <button
                onClick={closeAddModal}
                className="text-slate-400 hover:text-slate-600 p-1 rounded-lg hover:bg-slate-100 transition-colors"
                aria-label="Tutup"
              >
                <Icon name="x-mark" className="w-5 h-5" />
              </button>
            </div>
            <div className="p-5 space-y-4">
              <div className="relative">
                <Icon
                  name="magnifying-glass"
                  aria-hidden={true}
                  className="w-4 h-4 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
                />
                <input
                  className="input pl-8"
                  placeholder="Cari nama atau kode toko..."
                  aria-label="Cari toko"
                  value={searchStore}
                  onChange={(e) => setSearchStore(e.target.value)}
                  autoFocus
                />
              </div>

              <div className="max-h-64 overflow-y-auto -mx-5 px-5">
                {debouncedSearchStore.length < 2 ? (
                  <EmptyState
                    icon="building-storefront"
                    title="Ketik nama toko"
                    description="Minimal 2 karakter untuk mencari"
                  />
                ) : searchingStores ? (
                  <div className="space-y-2">
                    {[1, 2, 3].map((i) => (
                      <div key={i} className="flex items-center gap-3 p-3 rounded-lg bg-slate-50 animate-pulse">
                        <div className="w-8 h-8 rounded-full bg-slate-200 shrink-0" />
                        <div className="flex-1 space-y-1.5">
                          <div className="h-3.5 bg-slate-200 rounded w-32" />
                          <div className="h-3 bg-slate-200 rounded w-20" />
                        </div>
                      </div>
                    ))}
                  </div>
                ) : storeResults.length === 0 ? (
                  <EmptyState
                    icon="building-storefront"
                    title="Tidak ada hasil"
                    description={`Tidak ada toko untuk "${debouncedSearchStore}"`}
                  />
                ) : (
                  <div className="space-y-1">
                    {storeResults.map((store) => (
                      <button
                        key={store.outlet_sk}
                        onClick={() => addStoreMutation.mutate(store)}
                        disabled={addStoreMutation.isPending}
                        className="w-full text-left flex items-center gap-3 p-3 rounded-lg hover:bg-primary-50 transition-colors group"
                      >
                        <div className="w-8 h-8 rounded-full bg-slate-100 text-slate-500 flex items-center justify-center shrink-0">
                          <Icon name="building-storefront" className="w-4 h-4" aria-hidden={true} />
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium text-slate-700 truncate">{store.store_name}</p>
                          <p className="text-xs text-slate-400">{store.source_outlet_code} · {store.region ?? "—"}</p>
                        </div>
                        {store.store_grade && gradeBadge(store.store_grade)}
                        <Icon name="plus" className="w-4 h-4 text-primary-500 opacity-0 group-hover:opacity-100 shrink-0" aria-hidden={true} />
                      </button>
                    ))}
                  </div>
                )}
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={closeAddModal} className="btn-secondary">
                Batal
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
