import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";
import type { SalesmanRoute, RouteStore, DayId } from "@/types";
import { format, startOfISOWeek, addDays } from "date-fns";
import { id } from "date-fns/locale";

const DAYS: DayId[] = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"];

const fetchSalesmenRoutes = (week: string) =>
  api.get("/route-planner/salesmen", { params: { week } }).then((r) => r.data);

function gradeBadge(grade: string | null) {
  if (!grade) return null;
  const map: Record<string, string> = { S: "badge-blue", A: "badge-green", B: "badge-yellow", C: "badge-gray", D: "badge-red" };
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
        className="opacity-0 group-hover:opacity-100 text-slate-300 hover:text-red-500 transition-all text-lg leading-none"
      >×</button>
    </div>
  );
}

export default function RoutePlanner() {
  const qc = useQueryClient();
  const today = new Date();
  const [weekStart, setWeekStart] = useState(startOfISOWeek(today));
  const [selectedSalesmanSk, setSelectedSalesmanSk] = useState<string | null>(null);
  const [selectedDay, setSelectedDay] = useState<DayId>("Senin");
  const [showAddModal, setShowAddModal] = useState(false);
  const [searchStore, setSearchStore] = useState("");

  const weekLabel = format(weekStart, "'Minggu' w, d MMM", { locale: id });
  const weekKey = format(weekStart, "yyyy-'W'II");

  const { data: salesmen = [], isLoading } = useQuery<SalesmanRoute[]>({
    queryKey: ["route-planner", weekKey],
    queryFn: () => fetchSalesmenRoutes(weekKey),
  });

  const selected = salesmen.find((s) => s.salesman_sk === selectedSalesmanSk) ?? salesmen[0];
  const dayStores: RouteStore[] = selected?.stores_per_day?.[selectedDay] ?? [];

  const prevWeek = () => setWeekStart((d) => addDays(d, -7));
  const nextWeek = () => setWeekStart((d) => addDays(d, 7));

  const removeStoreMutation = useMutation({
    mutationFn: ({ salesmanSk, routePlanSk }: { salesmanSk: string; routePlanSk: string }) =>
      api.delete(`/route-planner/assignment/${routePlanSk}`, { params: { salesman_sk: salesmanSk } }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["route-planner"] }),
  });

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Route Planner"
        actions={
          <button onClick={() => setShowAddModal(true)} className="btn-primary text-sm px-3 py-1.5">
            + Tambah Store
          </button>
        }
      />

      <div className="flex flex-1 min-h-0">
        {/* Left: Salesman Rail */}
        <aside className="w-64 border-r border-slate-200 bg-white overflow-y-auto">
          <div className="p-3 border-b border-slate-100">
            <input
              className="input text-sm"
              placeholder="Cari salesman..."
            />
          </div>
          {isLoading ? (
            <p className="p-4 text-sm text-slate-400">Memuat...</p>
          ) : (
            <div className="divide-y divide-slate-50">
              {salesmen.map((s) => (
                <button
                  key={s.salesman_sk}
                  onClick={() => setSelectedSalesmanSk(s.salesman_sk)}
                  className={`w-full text-left p-3 hover:bg-slate-50 transition-colors ${
                    selected?.salesman_sk === s.salesman_sk ? "bg-primary-50 border-r-2 border-primary-600" : ""
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
                  <div className="flex gap-3 mt-2 ml-10 text-xs text-slate-500">
                    <span>✓ {s.compliance_pct?.toFixed(0) ?? "—"}%</span>
                    <span>🎯 {s.achievement_pct?.toFixed(0) ?? "—"}%</span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </aside>

        {/* Center: Route Board */}
        <main className="flex-1 flex flex-col min-w-0 overflow-hidden bg-slate-50">
          {/* Week nav */}
          <div className="bg-white border-b border-slate-200 px-5 py-3 flex items-center gap-4">
            <button onClick={prevWeek} className="btn-secondary text-sm px-2 py-1">‹</button>
            <span className="text-sm font-medium text-slate-700">{weekLabel}</span>
            <button onClick={nextWeek} className="btn-secondary text-sm px-2 py-1">›</button>
          </div>

          {/* Day tabs */}
          <div className="bg-white border-b border-slate-200 flex">
            {DAYS.map((day) => {
              const count = selected?.stores_per_day?.[day]?.length ?? 0;
              return (
                <button
                  key={day}
                  onClick={() => setSelectedDay(day)}
                  className={`flex-1 py-3 text-sm font-medium transition-colors border-b-2 ${
                    selectedDay === day
                      ? "border-primary-600 text-primary-600"
                      : "border-transparent text-slate-500 hover:text-slate-700"
                  }`}
                >
                  {day.slice(0, 3)}
                  <span className={`ml-1.5 text-xs px-1.5 py-0.5 rounded-full ${count > 0 ? "bg-primary-100 text-primary-700" : "bg-slate-100 text-slate-400"}`}>
                    {count}
                  </span>
                </button>
              );
            })}
          </div>

          {/* Store list */}
          <div className="flex-1 overflow-y-auto p-5">
            {!selected ? (
              <div className="flex items-center justify-center h-48 text-slate-400 text-sm">
                Pilih salesman di kiri
              </div>
            ) : dayStores.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-48 gap-3">
                <p className="text-slate-400 text-sm">Belum ada toko untuk {selectedDay}</p>
                <button onClick={() => setShowAddModal(true)} className="btn-primary text-sm">
                  + Tambah Store
                </button>
              </div>
            ) : (
              <div className="space-y-2 max-w-lg">
                {dayStores.map((store) => (
                  <StoreCard
                    key={store.route_plan_sk}
                    store={store}
                    onRemove={() =>
                      removeStoreMutation.mutate({ salesmanSk: selected.salesman_sk, routePlanSk: store.route_plan_sk })
                    }
                  />
                ))}
              </div>
            )}
          </div>

          {/* Sticky bottom bar */}
          <div className="bg-white border-t border-slate-200 px-5 py-3 flex items-center justify-between">
            <p className="text-sm text-slate-500">
              {selected ? `${selected.salesman_name} — ${dayStores.length} toko ${selectedDay}` : "—"}
            </p>
            <div className="flex gap-2">
              <button className="btn-secondary text-sm">Simpan Draft</button>
              <button className="btn-primary text-sm">Submit</button>
            </div>
          </div>
        </main>
      </div>

      {/* Add Store Modal */}
      {showAddModal && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md">
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 className="font-semibold text-slate-800">Tambah Store ke Rute</h3>
              <button onClick={() => setShowAddModal(false)} className="text-slate-400 hover:text-slate-600 text-xl">×</button>
            </div>
            <div className="p-5 space-y-4">
              <input
                className="input"
                placeholder="Cari nama atau kode toko..."
                value={searchStore}
                onChange={(e) => setSearchStore(e.target.value)}
              />
              <div className="text-sm text-slate-400 text-center py-8">
                Ketik nama toko untuk mencari
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={() => setShowAddModal(false)} className="btn-secondary">Batal</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
