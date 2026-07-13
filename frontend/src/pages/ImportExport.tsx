import { useState } from "react";
import TopNav from "@/components/layout/TopNav";
import { Icon } from "@/components/ui";
import { toast } from "@/store/toastStore";
import { api } from "@/api/client";
import type { IconName } from "@/components/ui";

type JobStatus = "idle" | "uploading" | "processing" | "done" | "error";
interface Job { name: string; status: JobStatus; message?: string; }

interface ImportZone {
  key: string;
  label: string;
  endpoint: string;
  hint: string;
  icon: IconName;
  iconCls: string;
}

interface ExportItem {
  label: string;
  endpoint: string;
  filename: string;
  icon: IconName;
  description: string;
}

const IMPORT_ZONES: ImportZone[] = [
  {
    key:      "pjp",
    label:    "PJP / Jadwal Kunjungan",
    endpoint: "/pjp/upload",
    hint:     "salesman_sk, outlet_sk, visit_day_of_week, week_number",
    icon:     "calendar-days",
    iconCls:  "icon-badge-blue",
  },
  {
    key:      "salesman",
    label:    "Master Salesman",
    endpoint: "/import/salesman",
    hint:     "salesman_code, name, type, distributor_code, spv_code",
    icon:     "users",
    iconCls:  "icon-badge-purple",
  },
  {
    key:      "outlet",
    label:    "Master Outlet",
    endpoint: "/import/outlet",
    hint:     "outlet_code, name, tier, channel, kecamatan, city",
    icon:     "building-storefront",
    iconCls:  "icon-badge-green",
  },
  {
    key:      "target",
    label:    "Target SPV",
    endpoint: "/import/target",
    hint:     "spv_code, brand, month, target_value",
    icon:     "chart-bar",
    iconCls:  "icon-badge-amber",
  },
];

const EXPORT_ITEMS: ExportItem[] = [
  {
    label:       "Route Compliance (MTD)",
    endpoint:    "/export/route-compliance",
    filename:    "route-compliance.csv",
    icon:        "map",
    description: "Kepatuhan rute kunjungan bulan berjalan",
  },
  {
    label:       "Achievement vs Target",
    endpoint:    "/export/achievement",
    filename:    "achievement.csv",
    icon:        "chart-pie",
    description: "Pencapaian sales vs target per brand",
  },
  {
    label:       "Master Outlet (lengkap)",
    endpoint:    "/export/outlet",
    filename:    "master-outlet.csv",
    icon:        "building-storefront",
    description: "Seluruh data outlet aktif di sistem",
  },
  {
    label:       "Master Salesman (lengkap)",
    endpoint:    "/export/salesman",
    filename:    "master-salesman.csv",
    icon:        "users",
    description: "Seluruh data salesman dan mapping tim",
  },
  {
    label:       "PJP Efektif (semua)",
    endpoint:    "/export/pjp",
    filename:    "pjp-efektif.csv",
    icon:        "calendar-days",
    description: "Jadwal kunjungan efektif seluruh salesman",
  },
  {
    label:       "Visit Log MTD",
    endpoint:    "/export/visits",
    filename:    "visit-log.csv",
    icon:        "clipboard-document-list",
    description: "Log seluruh kunjungan bulan berjalan",
  },
];

function uploadZoneCls(status: JobStatus, dragOver: boolean): string {
  if (dragOver)            return "upload-zone upload-zone-active";
  if (status === "done")   return "upload-zone upload-zone-done";
  if (status === "error")  return "upload-zone upload-zone-error";
  return "upload-zone";
}

function JobStatusIcon({ status }: { status: JobStatus }) {
  if (status === "uploading" || status === "processing")
    return <Icon name="arrow-path" className="w-5 h-5 text-primary-500 animate-spin" aria-hidden={true} />;
  if (status === "done")
    return <Icon name="check-circle" className="w-5 h-5 text-emerald-500" aria-hidden={true} />;
  if (status === "error")
    return <Icon name="exclamation-circle" className="w-5 h-5 text-red-500" aria-hidden={true} />;
  return <Icon name="arrow-up-tray" className="w-5 h-5 text-slate-400" aria-hidden={true} />;
}

export default function ImportExport() {
  const [jobs,     setJobs]     = useState<Record<string, Job>>({});
  const [dragOver, setDragOver] = useState<string | null>(null);
  const [exporting, setExporting] = useState<string | null>(null);

  const setJob = (key: string, patch: Partial<Job>) =>
    setJobs((j) => ({ ...j, [key]: { ...(j[key] ?? { name: "", status: "idle" }), ...patch } }));

  const handleFile = async (key: string, endpoint: string, file: File) => {
    setJob(key, { name: file.name, status: "uploading" });
    const fd = new FormData();
    fd.append("file", file);
    try {
      const res = await api.post(endpoint, fd, { headers: { "Content-Type": "multipart/form-data" } });
      const msg = res.data?.message ?? "Upload berhasil";
      setJob(key, { status: "done", message: msg });
      toast.success(msg);
    } catch (err: unknown) {
      const message = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? "Upload gagal";
      setJob(key, { status: "error", message });
      toast.error(message);
    }
  };

  const handleExport = async (endpoint: string, filename: string, label: string) => {
    setExporting(filename);
    try {
      const res = await api.get(endpoint, { responseType: "blob" });
      const url = URL.createObjectURL(res.data as Blob);
      const a   = document.createElement("a");
      a.href = url; a.download = filename; a.click();
      URL.revokeObjectURL(url);
      toast.success(`${label} berhasil diunduh.`);
    } catch {
      toast.error(`Gagal mengunduh ${label}.`);
    } finally {
      setExporting(null);
    }
  };

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Import / Export" subtitle="Manajemen data massal" />

      <main className="flex-1 overflow-y-auto p-6 space-y-8">

        {/* ── Import ── */}
        <section>
          <div className="section-heading mb-5">
            <div>
              <p className="section-heading-title">Bulk Import</p>
              <p className="section-heading-sub">Upload file .csv untuk memperbarui data master</p>
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            {IMPORT_ZONES.map(({ key, label, endpoint, hint, icon, iconCls }) => {
              const job    = jobs[key];
              const status = job?.status ?? "idle";
              const isOver = dragOver === key;

              return (
                <div key={key} className="card space-y-4 hover-lift">
                  {/* Header */}
                  <div className="flex items-center gap-3">
                    <span className={`icon-badge ${iconCls} shrink-0`}>
                      <Icon name={icon} className="w-4 h-4" />
                    </span>
                    <div className="flex-1 min-w-0">
                      <p className="font-semibold text-slate-800 text-sm">{label}</p>
                      <p className="text-2xs text-slate-400 truncate mt-0.5">{hint}</p>
                    </div>
                    <JobStatusIcon status={status} />
                  </div>

                  {/* Drop zone */}
                  <div
                    className={uploadZoneCls(status, isOver)}
                    aria-label={`Upload zona untuk ${label}`}
                    onDragOver={(e) => { e.preventDefault(); setDragOver(key); }}
                    onDragLeave={() => setDragOver(null)}
                    onDrop={(e) => {
                      e.preventDefault();
                      setDragOver(null);
                      const file = e.dataTransfer.files[0];
                      if (file) handleFile(key, endpoint, file);
                    }}
                  >
                    {status === "uploading" || status === "processing" ? (
                      <>
                        <Icon name="arrow-path" className="w-6 h-6 text-primary-400 animate-spin mb-2" aria-hidden={true} />
                        <p className="text-xs font-medium text-primary-600" role="status">
                          {status === "uploading" ? "Mengunggah…" : "Memproses…"}
                        </p>
                        <p className="text-2xs text-slate-400 mt-1 truncate max-w-full">{job?.name}</p>
                      </>
                    ) : status === "done" ? (
                      <>
                        <Icon name="check-circle" className="w-6 h-6 text-emerald-500 mb-2" aria-hidden={true} />
                        <p className="text-xs font-medium text-emerald-700">{job?.message}</p>
                        <p className="text-2xs text-slate-400 mt-1 truncate max-w-full">{job?.name}</p>
                        <button
                          className="mt-2 text-2xs text-primary-500 hover:underline"
                          onClick={() => setJob(key, { status: "idle", name: "", message: undefined })}
                        >
                          Upload lagi
                        </button>
                      </>
                    ) : status === "error" ? (
                      <>
                        <Icon name="exclamation-circle" className="w-6 h-6 text-red-400 mb-2" aria-hidden={true} />
                        <p className="text-xs font-medium text-red-600">{job?.message ?? "Upload gagal"}</p>
                        <button
                          className="mt-2 text-2xs text-primary-500 hover:underline"
                          onClick={() => setJob(key, { status: "idle", name: "", message: undefined })}
                        >
                          Coba lagi
                        </button>
                      </>
                    ) : (
                      <>
                        <Icon name="arrow-up-tray" className={`w-6 h-6 mb-2 ${isOver ? "text-primary-500" : "text-slate-300"}`} aria-hidden={true} />
                        <p className={`text-xs font-medium ${isOver ? "text-primary-600" : "text-slate-500"}`}>
                          {isOver ? "Lepaskan file di sini" : "Drag & drop file .csv"}
                        </p>
                        <p className="text-2xs text-slate-400 mt-0.5">atau</p>
                        <label className="mt-2 btn-secondary btn-sm cursor-pointer">
                          Pilih File
                          <input
                            type="file" accept=".csv" className="hidden"
                            onChange={(e) => {
                              const f = e.target.files?.[0];
                              if (f) handleFile(key, endpoint, f);
                              e.target.value = "";
                            }}
                          />
                        </label>
                      </>
                    )}
                  </div>

                  {/* Template download */}
                  <button
                    className="w-full flex items-center justify-center gap-2 text-xs text-slate-500 hover:text-primary-600 transition-colors py-1.5 border border-slate-100 rounded-lg hover:border-primary-200 hover:bg-primary-50"
                    aria-label={`Download template ${label}`}
                    onClick={() => handleExport(`/template/${key}`, `template-${key}.csv`, `Template ${label}`)}
                  >
                    <Icon name="arrow-down-tray" className="w-3.5 h-3.5" />
                    Download Template
                  </button>
                </div>
              );
            })}
          </div>
        </section>

        {/* ── Export ── */}
        <section>
          <div className="section-heading mb-5">
            <div>
              <p className="section-heading-title">Export Data</p>
              <p className="section-heading-sub">Unduh laporan dan data master dalam format CSV</p>
            </div>
          </div>

          <div className="card divide-y divide-slate-50">
            {EXPORT_ITEMS.map(({ label, endpoint, filename, icon, description }) => {
              const isLoading = exporting === filename;
              return (
                <div key={label} className="list-item group">
                  <span className="icon-badge icon-badge-slate shrink-0">
                    <Icon name={icon} className="w-4 h-4" />
                  </span>
                  <div className="flex-1 min-w-0">
                    <p className="list-item-title">{label}</p>
                    <p className="list-item-sub">{description}</p>
                  </div>
                  <button
                    onClick={() => handleExport(endpoint, filename, label)}
                    disabled={isLoading}
                    className="btn-secondary btn-sm shrink-0"
                    aria-label={isLoading ? `Mengunduh ${label}` : `Download CSV ${label}`}
                  >
                    {isLoading
                      ? <Icon name="arrow-path" className="w-3.5 h-3.5 animate-spin" />
                      : <Icon name="arrow-down-tray" className="w-3.5 h-3.5" />}
                    {isLoading ? "Mengunduh…" : "Download CSV"}
                  </button>
                </div>
              );
            })}
          </div>
        </section>

      </main>
    </div>
  );
}
