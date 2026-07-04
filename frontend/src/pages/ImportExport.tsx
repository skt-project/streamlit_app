import { useState } from "react";
import TopNav from "@/components/layout/TopNav";
import { api } from "@/api/client";

type JobStatus = "idle" | "uploading" | "processing" | "done" | "error";

interface Job { name: string; status: JobStatus; message?: string; }

export default function ImportExport() {
  const [jobs, setJobs] = useState<Record<string, Job>>({});
  const [dragOver, setDragOver] = useState<string | null>(null);

  const setJob = (key: string, patch: Partial<Job>) =>
    setJobs((j) => ({ ...j, [key]: { ...j[key], ...patch } }));

  const handleFile = async (key: string, endpoint: string, file: File) => {
    setJob(key, { name: file.name, status: "uploading" });
    const fd = new FormData();
    fd.append("file", file);
    try {
      const res = await api.post(endpoint, fd, { headers: { "Content-Type": "multipart/form-data" } });
      setJob(key, { status: "done", message: res.data?.message ?? "Upload berhasil" });
    } catch (err: unknown) {
      const message = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? "Upload gagal";
      setJob(key, { status: "error", message });
    }
  };

  const handleExport = async (endpoint: string, filename: string) => {
    const res = await api.get(endpoint, { responseType: "blob" });
    const url = URL.createObjectURL(res.data as Blob);
    const a = document.createElement("a");
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  };

  const IMPORT_ZONES = [
    { key: "pjp",      label: "PJP / Jadwal Kunjungan",   endpoint: "/import/pjp",       hint: "Format: outlet_code, salesman_code, day, frequency, week_pattern" },
    { key: "salesman", label: "Master Salesman",           endpoint: "/import/salesman",   hint: "Format: salesman_code, name, type, distributor_code, spv_code" },
    { key: "outlet",   label: "Master Outlet",             endpoint: "/import/outlet",     hint: "Format: outlet_code, name, tier, channel, kecamatan, city" },
    { key: "target",   label: "Target SPV",                endpoint: "/import/target",     hint: "Format: spv_code, brand, month, target_value" },
  ];

  const EXPORT_ITEMS = [
    { label: "Route Compliance (MTD)",     endpoint: "/export/route-compliance",  filename: "route-compliance.xlsx" },
    { label: "Achievement vs Target",      endpoint: "/export/achievement",        filename: "achievement.xlsx" },
    { label: "Master Outlet (lengkap)",    endpoint: "/export/outlet",            filename: "master-outlet.xlsx" },
    { label: "Master Salesman (lengkap)",  endpoint: "/export/salesman",          filename: "master-salesman.xlsx" },
    { label: "PJP Efektif (semua)",        endpoint: "/export/pjp",               filename: "pjp-efektif.xlsx" },
    { label: "Visit Log MTD",              endpoint: "/export/visits",            filename: "visit-log.xlsx" },
  ];

  const statusIcon = (s: JobStatus) => ({ idle: "", uploading: "⏳", processing: "⚙️", done: "✅", error: "❌" }[s]);

  return (
    <div className="flex flex-col h-full">
      <TopNav title="Import / Export" />

      <main className="flex-1 overflow-y-auto p-6 space-y-8">
        {/* Import */}
        <section>
          <h2 className="text-base font-semibold text-slate-800 mb-4">Bulk Import</h2>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {IMPORT_ZONES.map(({ key, label, endpoint, hint }) => {
              const job = jobs[key];
              return (
                <div key={key} className="card space-y-3">
                  <div className="flex items-center justify-between">
                    <p className="font-medium text-slate-700">{label}</p>
                    {job && <span className="text-sm">{statusIcon(job.status)}</span>}
                  </div>
                  <p className="text-xs text-slate-400">{hint}</p>
                  <div
                    onDragOver={(e) => { e.preventDefault(); setDragOver(key); }}
                    onDragLeave={() => setDragOver(null)}
                    onDrop={(e) => {
                      e.preventDefault(); setDragOver(null);
                      const file = e.dataTransfer.files[0];
                      if (file) handleFile(key, endpoint, file);
                    }}
                    className={`border-2 border-dashed rounded-xl p-5 text-center transition-colors cursor-pointer ${dragOver === key ? "border-primary-400 bg-primary-50" : "border-slate-200 hover:border-primary-300"}`}
                  >
                    <p className="text-xs text-slate-500 mb-2">Drag & drop .xlsx / .csv</p>
                    <label className="btn-secondary text-xs cursor-pointer">
                      Pilih File
                      <input
                        type="file" accept=".xlsx,.xls,.csv" className="hidden"
                        onChange={(e) => { const f = e.target.files?.[0]; if (f) handleFile(key, endpoint, f); }}
                      />
                    </label>
                  </div>
                  {job?.message && (
                    <p className={`text-xs ${job.status === "error" ? "text-red-500" : "text-green-600"}`}>{job.message}</p>
                  )}
                  {job?.status === "idle" || !job ? null : (
                    <p className="text-xs text-slate-400">{job.name}</p>
                  )}
                  <button className="btn-secondary text-xs w-full" onClick={() => handleExport(`/template/${key}`, `template-${key}.xlsx`)}>
                    Download Template
                  </button>
                </div>
              );
            })}
          </div>
        </section>

        {/* Export */}
        <section>
          <h2 className="text-base font-semibold text-slate-800 mb-4">Export Data</h2>
          <div className="card divide-y divide-slate-50">
            {EXPORT_ITEMS.map(({ label, endpoint, filename }) => (
              <div key={label} className="flex items-center justify-between py-4 first:pt-0 last:pb-0">
                <p className="text-sm text-slate-700">{label}</p>
                <button
                  onClick={() => handleExport(endpoint, filename)}
                  className="btn-secondary text-xs"
                >
                  Download Excel
                </button>
              </div>
            ))}
          </div>
        </section>
      </main>
    </div>
  );
}
