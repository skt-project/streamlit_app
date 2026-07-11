import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import TopNav from "@/components/layout/TopNav";
import { Icon, SkeletonTable, EmptyState } from "@/components/ui";
import { api } from "@/api/client";
import { useDebounce } from "@/hooks/useDebounce";
import type { User, Role } from "@/types";

const fetchUsers = (search: string, role: string) =>
  api.get("/admin/users", { params: { search, role: role || undefined } }).then((r) => r.data);

const ROLES: Role[] = ["spv", "asm", "ddm", "dm", "rsm", "ho_admin", "distributor_admin"];
const ROLE_LABELS: Record<Role, string> = {
  se: "SE", spv: "SPV", asm: "ASM", ddm: "DDM", dm: "DM", rsm: "RSM",
  ho_admin: "HO Admin", distributor_admin: "Distributor Admin",
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

  const { data: users = [], isLoading } = useQuery<User[]>({
    queryKey: ["admin-users", search, roleFilter],
    queryFn: () => fetchUsers(search, roleFilter),
    placeholderData: (prev) => prev,
  });

  const createMutation = useMutation({
    mutationFn: () => api.post("/admin/users", form),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["admin-users"] }); setShowModal(false); setForm(EMPTY_FORM); },
  });

  const updateMutation = useMutation({
    mutationFn: (id: string) => api.put(`/admin/users/${id}`, form),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ["admin-users"] }); setShowModal(false); setEditTarget(null); },
  });

  const toggleActiveMutation = useMutation({
    mutationFn: ({ id, active }: { id: string; active: boolean }) => api.patch(`/admin/users/${id}`, { is_active: active }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["admin-users"] }),
  });

  const openCreate = () => { setForm(EMPTY_FORM); setEditTarget(null); setShowModal(true); };
  const openEdit = (u: User) => {
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
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
            />
          </div>
          <select className="input w-36 text-sm" value={roleFilter} onChange={(e) => setRoleFilter(e.target.value)}>
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
                      <td><span className="badge-blue text-xs">{ROLE_LABELS[u.role]}</span></td>
                      <td>{u.brand_group ?? "—"}</td>
                      <td>{u.salesman_sk ? "Ya" : <span className="text-slate-300">Tidak</span>}</td>
                      <td><span className={u.is_active ? "badge-green" : "badge-gray"}>{u.is_active ? "Aktif" : "Non-Aktif"}</span></td>
                      <td>
                        <div className="flex items-center gap-3">
                          <button onClick={() => openEdit(u)} className="text-xs text-primary-600 hover:underline">Edit</button>
                          <button
                            onClick={() => toggleActiveMutation.mutate({ id: u.user_id, active: !u.is_active })}
                            className="text-xs text-slate-400 hover:text-slate-600"
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
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md max-h-[90vh] flex flex-col">
            <div className="flex items-center justify-between p-5 border-b border-slate-100">
              <h3 className="font-semibold text-slate-800">{editTarget ? "Edit Pengguna" : "Tambah Pengguna Baru"}</h3>
              <button onClick={() => setShowModal(false)} className="text-slate-400 hover:text-slate-600">
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
              ].map(({ label, key, type, placeholder }) => (
                <div key={key}>
                  <label className="block text-sm font-medium text-slate-700 mb-1">{label}</label>
                  <input
                    type={type}
                    className="input"
                    placeholder={placeholder}
                    value={(form as Record<string, string>)[key]}
                    onChange={(e) => setForm((f) => ({ ...f, [key]: e.target.value }))}
                  />
                </div>
              ))}
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Role</label>
                <select className="input" value={form.role} onChange={(e) => setForm((f) => ({ ...f, role: e.target.value as Role }))}>
                  {ROLES.map((r) => <option key={r} value={r}>{ROLE_LABELS[r]}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1">Brand Group (opsional)</label>
                <select className="input" value={form.brand_group} onChange={(e) => setForm((f) => ({ ...f, brand_group: e.target.value }))}>
                  <option value="">Semua (HO Admin)</option>
                  <option>SKT</option><option>G2G</option>
                </select>
              </div>
            </div>
            <div className="p-4 border-t border-slate-100 flex justify-end gap-2">
              <button onClick={() => setShowModal(false)} className="btn-secondary">Batal</button>
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
