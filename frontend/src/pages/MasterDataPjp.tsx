import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, EmptyState, SkeletonTable, SkeletonStatCards } from "@/components/ui";
import { toast } from "@/store/toastStore";
import { api } from "@/api/client";
import { useDebounce } from "@/hooks/useDebounce";

const fetchPjpSummary = () => api.get("/pjp/summary").then((r) => r.data);
const fetchPjpList    = (search: string) =>
  api.get("/pjp/list", { params: { search } }).then((r) => r.data);

type DragStatus = "idle" | "active" | "done" | "error";

export default function MasterDataPjp() {
  const [activeTab, setActiveTab] = useState<"list" | "upload" | "config">("list");
  const [search, setSearch]       = useState("");
  const [dragStatus, setDragStatus] = useState<DragStatus>("idle");
  const [fileName, setFileName]   = useState<string | null>(null);
  const debouncedSearch = useDebounce(search, 300);

  const { data: summary, isLoading: summaryLoading } = useQuery({
    queryKey: ["pjp-summary"],
    queryFn:  fetchPjpSummary,
    staleTime: 10 * 60 * 1000,  // matches backend 600s cache
    placeholderData: (prev) => prev,
  });

  const { data: pjpList = [], isLoading } = useQuery({
    queryKey: ["pjp-list", debouncedSearch],
    queryFn:  () => fetchPjpList(debouncedSearch),
    enabled:  activeTab === "list",
    staleTime: 5 * 60 * 1000,
    placeholderData: (prev) => prev,
  });

  const summaryCards = [
    { label: "Total Toko (Basis DB)", value: summary?.total_stores ?? "—",          icon: "building-storefront" as const, cls: "icon-badge-blue"   },
    { label: "Toko dengan PJP",       value: summary?.stores_with_pjp ?? "—",       icon: "calendar-days"       as const, cls: "icon-badge-green"  },
    { label: "Toko Basis Saja",       value: summary?.stores_basis_only ?? "—",      icon: "list-bullet"         as const, cls: "icon-badge-amber"  },
    { label: "Coverage PJP",          value: summary ? `${summary.coverage_pct?.toFixed(1)}%` : "—", icon: "chart-pie" as const, cls: "icon-badge-indigo" },
  ];

  const handleFileUpload = async (file: File) => {
    setFileName(file.name);
    setDragStatus("active");
    const fd = new FormData();
    fd.append("file", file);
    try {
      const res = await api.post("/pjp/upload", fd, {
        headers: { "Content-Type": "multipart/form-data" },
      });
      setDragStatus("done");
      toast.success(res.data?.message ?? "Upload PJP berhasil.");
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail
        ?? "Upload gagal.";
      setDragStatus("error");
      toast.error(msg);
    }
  };

  function uploadZoneCls(status: DragStatus): string {
    if (status === "active") return "upload-zone upload-zone-active";
    if (status === "done")   return "upload-zone upload-zone-done";
    if (status === "error")  return "upload-zone upload-zone-error";
    return "upload-zone";
  }

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Master Data PJP" />

      <main className="flex-1 overflow-y-auto p-6 space-y-5">
        {/* ── Summary cards ── */}
        {summaryLoading ? (
          <SkeletonStatCards count={4} />
        ) : (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {summaryCards.map((c) => (
              <div key={c.label} className="kpi-tile">
                <span className={`icon-badge ${c.cls} shrink-0`}>
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

        {/* ── Tabs ── */}
        <div role="tablist" className="flex border-b border-slate-200">
          {(
            [
              ["list",   "PJP Efektif"],
              ["upload", "Upload / Perbarui PJP"],
              ["config", "Konfigurasi Deadline"],
            ] as const
          ).map(([t, label]) => (
            <button
              key={t}
              id={`tab-pjp-${t}`}
              role="tab"
              aria-selected={activeTab === t}
              aria-controls={`panel-pjp-${t}`}
              onClick={() => setActiveTab(t)}
              className={`px-5 py-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === t
                  ? "border-primary-600 text-primary-600"
                  : "border-transparent text-slate-500 hover:text-slate-700"
              }`}
            >
              {label}
            </button>
          ))}
        </div>

        {/* ── List tab ── */}
        {activeTab === "list" && (
          <div id="panel-pjp-list" role="tabpanel" aria-labelledby="tab-pjp-list" className="card space-y-4">
            <div className="flex gap-3 items-center">
              <div className="relative">
                <Icon
                  name="magnifying-glass"
                  className="w-4 h-4 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
                />
                <input
                  className="input w-64 text-sm pl-8"
                  placeholder="Cari toko atau salesman..."
                  aria-label="Cari toko atau salesman"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
              </div>
            </div>

            {isLoading ? (
              <SkeletonTable rows={8} cols={8} />
            ) : pjpList.length === 0 ? (
              <EmptyState
                icon="calendar-days"
                title="Tidak ada data PJP"
                description="Belum ada data PJP untuk filter ini."
              />
            ) : (
              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      {["Kode Toko", "Nama Toko", "Brand", "Salesman", "Hari", "Frekuensi", "Minggu", "Sumber"].map((h) => (
                        <th key={h}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {(pjpList as Record<string, string>[]).map((r) => (
                      <tr key={`${r.source_outlet_code}-${r.brand ?? ""}-${r.visit_day_of_week}`}>
                        <td className="font-mono text-xs text-slate-500">{r.source_outlet_code}</td>
                        <td>{r.store_name}</td>
                        <td>{r.brand ?? "—"}</td>
                        <td>{r.source_salesman_name}</td>
                        <td>{r.visit_day_of_week}</td>
                        <td>{r.visit_frequency_code}</td>
                        <td>{r.visit_week_pattern}</td>
                        <td>
                          <span className="badge-gray text-xs">{r.source_system ?? "GT"}</span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {/* ── Upload tab ── */}
        {activeTab === "upload" && (
          <div id="panel-pjp-upload" role="tabpanel" aria-labelledby="tab-pjp-upload" className="card max-w-lg space-y-4">
            <p className="text-sm text-slate-600">
              Upload file Excel PJP sesuai template. Data hanya di-<em>commit</em> setelah konfirmasi eksplisit.
            </p>

            <div
              onDragOver={(e) => { e.preventDefault(); setDragStatus("active"); }}
              onDragLeave={() => setDragStatus(dragStatus === "active" ? "idle" : dragStatus)}
              onDrop={(e) => {
                e.preventDefault();
                const file = e.dataTransfer.files[0];
                if (file) handleFileUpload(file);
              }}
              className={uploadZoneCls(dragStatus)}
            >
              {dragStatus === "done" ? (
                <>
                  <Icon name="check-circle" className="w-6 h-6 text-emerald-500 mb-2" />
                  <p className="text-xs font-medium text-emerald-700">Upload berhasil</p>
                  <p className="text-2xs text-slate-400 mt-1">{fileName}</p>
                  <button
                    className="mt-2 text-2xs text-primary-500 hover:underline"
                    onClick={() => { setDragStatus("idle"); setFileName(null); }}
                  >
                    Upload lagi
                  </button>
                </>
              ) : dragStatus === "error" ? (
                <>
                  <Icon name="exclamation-circle" className="w-6 h-6 text-red-400 mb-2" />
                  <p className="text-xs font-medium text-red-600">Upload gagal</p>
                  <button
                    className="mt-2 text-2xs text-primary-500 hover:underline"
                    onClick={() => { setDragStatus("idle"); setFileName(null); }}
                  >
                    Coba lagi
                  </button>
                </>
              ) : dragStatus === "active" ? (
                <>
                  <Icon name="arrow-up-tray" className="w-6 h-6 text-primary-400 mb-2" />
                  <p className="text-xs font-medium text-primary-600">Lepaskan file di sini</p>
                </>
              ) : (
                <>
                  <Icon name="arrow-up-tray" className="w-6 h-6 text-slate-300 mb-2" />
                  <p className="text-xs font-medium text-slate-500">Drag & drop file Excel di sini</p>
                  <p className="text-2xs text-slate-400 mt-0.5">atau</p>
                  <label className="mt-2 btn-secondary btn-sm cursor-pointer">
                    Pilih File
                    <input
                      type="file"
                      accept=".xlsx,.xls,.csv"
                      className="hidden"
                      onChange={(e) => {
                        const f = e.target.files?.[0];
                        if (f) handleFileUpload(f);
                        e.target.value = "";
                      }}
                    />
                  </label>
                </>
              )}
            </div>

            <button
              className="w-full flex items-center justify-center gap-2 text-xs text-slate-500 hover:text-primary-600 transition-colors py-1.5 border border-slate-100 rounded-lg hover:border-primary-200 hover:bg-primary-50"
              onClick={() => toast.info("Mengunduh template PJP...")}
            >
              <Icon name="arrow-down-tray" className="w-3.5 h-3.5" />
              Download Template PJP
            </button>
          </div>
        )}

        {/* ── Config tab ── */}
        {activeTab === "config" && (
          <div id="panel-pjp-config" role="tabpanel" aria-labelledby="tab-pjp-config" className="card max-w-sm space-y-4">
            <div>
              <label htmlFor="pjp-deadline" className="block text-sm font-medium text-slate-700 mb-1">
                Deadline Input PJP
              </label>
              <input id="pjp-deadline" type="date" className="input" />
            </div>
            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" className="rounded" defaultChecked />
              <span className="text-sm text-slate-700">Periode input sedang terbuka</span>
            </label>
            <button className="btn-primary text-sm">Simpan</button>
          </div>
        )}
      </main>
    </div>
  );
}
