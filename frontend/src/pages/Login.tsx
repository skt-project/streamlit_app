import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "@/api/client";
import { useAuthStore } from "@/store/authStore";

export default function Login() {
  const navigate = useNavigate();
  const login = useAuthStore((s) => s.login);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const { data } = await api.post("/auth/login", { username, password });
      login(data.access_token, data.user);
      navigate("/dashboard");
    } catch {
      setError("Username atau password salah.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-white flex items-center justify-center p-4">
      <div className="w-full max-w-sm">

        {/* Skintific logo */}
        <div className="flex justify-center mb-8">
          <img
            src="/skintific-logo.png"
            alt="Skintific"
            className="h-8 w-auto object-contain"
          />
        </div>

        {/* STEP heading */}
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-slate-900 tracking-tight">STEP</h1>
          <p className="text-slate-500 text-sm mt-1">Territory &amp; Execution Platform</p>
          <p className="text-slate-400 text-xs mt-1">Hanya untuk Penggunaan Internal</p>
        </div>

        {/* Login card */}
        <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-8">
          <h2 className="text-slate-800 font-semibold text-lg mb-6">Masuk</h2>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="form-label">Username</label>
              <input
                className="input"
                placeholder="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                autoComplete="username"
                required
              />
            </div>
            <div>
              <label className="form-label">Password</label>
              <input
                type="password"
                className="input"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="current-password"
                required
              />
            </div>

            {error && (
              <p className="text-sm text-red-600 bg-red-50 px-3 py-2 rounded-lg">{error}</p>
            )}

            <button type="submit" className="btn-primary w-full py-2.5" disabled={loading}>
              {loading ? "Memuat..." : "Masuk →"}
            </button>
          </form>

          <p className="text-xs text-slate-400 text-center mt-6">
            Hubungi HO Admin jika akun belum dibuat
          </p>
        </div>

        <p className="text-slate-400 text-xs text-center mt-4">STEP v1.0</p>
      </div>
    </div>
  );
}
