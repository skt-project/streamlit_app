import { useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, SkeletonTable, EmptyState } from "@/components/ui";
import { api } from "@/api/client";
import { useDebounce } from "@/hooks/useDebounce";
import { useFocusTrap } from "@/hooks/useFocusTrap";
import type { User, Role } from "@/types";

const fetchUsers = (search: string, role: string) =>
  api.get("/admin/users", { params: { search, role: role || undefined } }).then((r) => r.data);

const ROLES: Role[] = ["salesman", "spv", "asm", "dm", "ho_admin", "demo"];
const ROLE_LABELS: Record<Role, string> = {
  salesman: "Salesman", spv: "SPV", asm: "ASM", dm: "DM",
  ho_admin: "HO Admin", demo: "Demo",
};
const ROLE_BADGE: Record<Role, string> = {
  salesman: "badge-green", spv: "badge-blue", asm: "badge-purple",
  dm: "badge-yellow", ho_admin: "badge-red", demo: "badge-gray",
};

const EMPTY_FORM = { username: "", full_name: "", role: "spv" as Role, email: "", brand_group: "", salesman_sk: "", password: "" };

export default function Administration() {
  const qc = useQueryClient();
  const [searchInput, setSearchInput] = useState("");
  const [roleFilter, setRoleFilter]   = useState("");
  const search = useDebounce(searchInput, 350);
  const [showModal, setShowModal] = useState(false);
  const [editTarget, setEditTarget] = useState<User | null>(null);
  const [form, setForm] = useState(EMPTY_FORM);
  const modalTriggerRef = useRef<Element | null>(null);
  const modalPanelRef = useRef<HTMLDivElement>(null);
  useFocusTrap(modalPanelRef, showModal);

  const { data: users = [], isLoading } = useQuery<User[]>({
    queryKey: ["admin-users", search, roleFilter],
    queryFn: () => fetchUsers(search, roleFilter),
    staleTime: 30_000,
    placeholderData: (prev) => prev,
  });

  const closeModal = () => {
    setShowModal(false);
    setTimeout(() => { (modalTriggerRef.current as HTMLElement | null)?.focus(); }, 0);
  };

  const createMutation = useMutation({
    mutationFn: () => api.post("/admin/users", form),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["admin-users"] }); closeModal(); setForm(EMPTY_FORM); },
  });

  const updateMutation = useMutation({
    mutationFn: (id: string) => api.put(`/admin/users/${id}`, form),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["admin-users"] }); closeModal(); setEditTarget(null); },
  });

  const toggleActiveMutation = useMutation({
    mutationFn: ({ id, active }: { id: string; active: boolean }) => api.patch(`/admin/users/${id}`, { is_active: active }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["admin-users"] }),
  });

  const openCreate = () => { modalTriggerRef.current = document.activeElement; setForm(EMPTY_FORM); setEditTarget(null); setShowModal(true); };
  const openEdit = (u: User) => {
    modalTriggerRef.current = document.activeElement;
    setForm({ username: u.username, full_name: u.full_name, role: u.role, email: u.email ?? "", brand_group: u.brand_group ?? "", salesman_sk: String(u.salesman_sk ?? ""), password: "" });
    setEditTarget(u);
    setShowModal(true);
  };

  return (
    <div className="flex flex-col h-full">
      <TopNav
        title="Administrasi Pengguna"
        actions={
          <button onClick={openCreate} className="btn-primary text-sm">
            <Icon name="plus" className="w-3.5 h-3.5" />
            Tambah Pengguna
          </button>
        }
      />

      <main className="flex-1 overflow-y-auto p-6 space-y-4">
        <div className="flex gap-3 flex-wrap">
          <div className="relative">
            <Icon
              name="magnifying-glass"
              className="w-4 h-4 text-slate-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
            />
            <input
              className="input w-64 text-sm pl-8"
              placeholder="Cari nama atau username..."
              aria-label="Cari pengguna"
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
            />
          </div>
          <select
            className="input w-36 text-sm"
            aria-label="Filter role"
            value={roleFilter}
            onChange={(e) => setRoleFilter(e.target.value)}
          >
            <option value="">Semua Role</option>
            {ROLES.map((r) => <option key={r} value={r}>{ROLE_LABELS[r]}</option>)}
          </select>
        </div>

        <div className="card">
          {isLoading ? (
            <SkeletonTable rows={5} cols={7} />
          ) : users.length === 0 ? (
            <EmptyState
              icon="users"
              title="Tidak ada pengguna"
              description="Tidak ada pengguna yang cocok dengan filter ini."
            />
          ) : (
            <div className="table-container">
              <table className="table">
                <thead>
                  <tr>
                    {["Username", "Nama", "Role", "Brand Group", "SE Linked", "Status", ""].map((h) => (
                      <th key={h}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {users.map((u) => (
                    <tr key={u.user_id}>
                      <td className="font-mono text-xs text-slate-500">{u.username}</td>
                      <td>{u.full_name}</td>
                      <td><span className={`${ROLE_BADGE[u.role] ?? "badge-gray"} text-xs`}>{ROLE_LABELS[u.role]}</span></td>
                      <td>{u.brand_group ?? "—"}</td>
                      <td>{u.salesman_sk ? "Ya" : <span className="text-slate-300">Tidak</span>}</td>
                      <td><span className={u.is_active ? "badge-green" : "badge-gray"}>{u.is_active ? "Aktif" : "Non-Aktif"}</span></td>
                      <td>
                        <div className="flex items-center gap-3">
                          <button onClick={() => openEdit(u)} className="text-xs text-primary-600 hover:underline" aria-label={`Edit ${u.full_name}`}>Edit</button>
                          <button
                            onClick={() => toggleActiveMutation.mutate({ id: u.user_id, active: !u.is_active })}
                            className="text-xs text-slate-400 hover:text-slate-600"
                            aria-label={u.is_active ? `Nonaktifkan ${u.full_name}` : `Aktifkan ${u.full_name}`}
                          >
                            {u.is_active ? "Nonaktifkan" : "Aktifkan"}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>

      {showModal && (
        <div
          className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4"
          onClick={(e) => { if (e.target === e.currentTarget) closeModal(); }}
        >
          <div
            ref={modalPanelRef}
            role="dialog"
            aria-modal="true"
            aria-labelledby="admin-modal-title"
            className="bg-white rounded-2xl shadow-2xl w-full max-w-md max-h-[90vh] flex flex-col"
          >
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 id="admin-modal-title" className="font-semibold text-slate-800">{editTarget ? "Edit Pengguna" : "Tambah Pengguna Baru"}</h3>
              <button onClick={closeModal} className="text-slate-400 hover:text-slate-600 p-1 rounded-lg hover:bg-slate-100 transition-colors" aria-label="Tutup">
                <Icon name="x-mark" className="w-5 h-5" />
              </button>
            </div>
            <div className="flex-1 overflow-y-auto p-5 space-y-4">
              {[
                { label: "Username", key: "username", type: "text", placeholder: "e.g. spv_jakarta01" },
                { label: "Nama Lengkap", key: "full_name", type: "text", placeholder: "e.g. Budi Santoso" },
                { label: "Email", key: "email", type: "email", placeholder: "optional" },
                { label: "Password", key: "password", type: "password", placeholder: editTarget ? "Kosongkan jika tidak diubah" : "Min 8 karakter" },
                { label: "Salesman SK (opsional, untuk SE/SPV)", key: "salesman_sk", type: "text", placeholder: "integer SK" },
              ].map(({ label, key, type, placeholder }, idx) => (
                <div key={key}>
                  <label htmlFor={`admin-field-${key}`} className="block text-sm font-medium text-slate-700 mb-1">{label}</label>
                  <input
                    id={`admin-field-${key}`}
                    type={type}
                    className="input"
                    placeholder={placeholder}
                    value={(form as Record<string, string>)[key]}
                    onChange={(e) => setForm((f) => ({ ...f, [key]: e.target.value }))}
                    autoFocus={idx === 0}
                  />
                </div>
              ))}
              <div>
                <label htmlFor="admin-role" className="block text-sm font-medium text-slate-700 mb-1">Role</label>
                <select id="admin-role" className="input" value={form.role} onChange={(e) => setForm((f) => ({ ...f, role: e.target.value as Role }))}>
                  {ROLES.map((r) => <option key={r} value={r}>{ROLE_LABELS[r]}</option>)}
                </select>
              </div>
              <div>
                <label htmlFor="admin-brand-group" className="block text-sm font-medium text-slate-700 mb-1">Brand Group (opsional)</label>
                <select id="admin-brand-group" className="input" value={form.brand_group} onChange={(e) => setForm((f) => ({ ...f, brand_group: e.target.value }))}>
                  <option value="">Semua (HO Admin)</option>
                  <option>SKT</option><option>G2G</option>
                </select>
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={closeModal} className="btn-secondary">Batal</button>
              <button
                className="btn-primary"
                disabled={!form.username || !form.full_name || (!editTarget && !form.password) || createMutation.isPending || updateMutation.isPending}
                onClick={() => editTarget ? updateMutation.mutate(editTarget.user_id) : createMutation.mutate()}
              >
                {(createMutation.isPending || updateMutation.isPending) ? "Menyimpan..." : "Simpan"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
