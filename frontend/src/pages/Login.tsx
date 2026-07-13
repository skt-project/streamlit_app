import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "@/api/client";
import { useAuthStore } from "@/store/authStore";
import { Icon } from "@/components/ui";

export default function Login() {
  const navigate = useNavigate();
  const login = useAuthStore((s) => s.login);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPw, setShowPw] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    document.title = "Masuk — STEP";
    return () => { document.title = "STEP — Territory & Execution Platform"; };
  }, []);

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
    <div className="min-h-screen bg-slate-50 flex items-center justify-center p-4">
      <main className="w-full max-w-sm">
        {/* Brand mark */}
        <div className="flex flex-col items-center mb-8 gap-3">
          <div className="w-12 h-12 rounded-2xl bg-primary-600 flex items-center justify-center shadow-primary">
            <Icon name="map" className="w-6 h-6 text-white" />
          </div>
          <div className="text-center">
            <p className="text-slate-900 font-bold text-xl tracking-tight">STEP</p>
            <p className="text-slate-400 text-xs mt-0.5">Territory &amp; Execution Platform</p>
          </div>
        </div>

        {/* Login card */}
        <div className="card p-8 shadow-card-md">
          <h2 className="text-slate-800 font-semibold text-base mb-6">Masuk ke akun Anda</h2>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label htmlFor="username" className="form-label">Username</label>
              <input
                id="username"
                className="input"
                placeholder="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                autoComplete="username"
                autoFocus
                required
              />
            </div>

            <div>
              <label htmlFor="password" className="form-label">Password</label>
              <div className="relative">
                <input
                  id="password"
                  type={showPw ? "text" : "password"}
                  className="input pr-10"
                  placeholder="••••••••"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  autoComplete="current-password"
                  required
                />
                <button
                  type="button"
                  tabIndex={-1}
                  onClick={() => setShowPw((v) => !v)}
                  className="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
                  aria-label={showPw ? "Sembunyikan password" : "Tampilkan password"}
                >
                  <Icon name={showPw ? "eye-slash" : "eye"} className="w-4 h-4" />
                </button>
              </div>
            </div>

            {error && (
              <div role="alert" className="alert-danger text-sm py-2.5">
                <Icon name="exclamation-circle" className="w-4 h-4 shrink-0" aria-hidden={true} />
                {error}
              </div>
            )}

            <button
              type="submit"
              className="btn-primary w-full py-2.5 mt-2"
              disabled={loading}
            >
              {loading ? (
                <span className="flex items-center gap-2 justify-center">
                  <Icon name="arrow-path" className="w-4 h-4 animate-spin" />
                  Memuat...
                </span>
              ) : (
                <span className="flex items-center gap-1.5 justify-center">
                  Masuk
                  <Icon name="chevron-right" className="w-4 h-4" />
                </span>
              )}
            </button>
          </form>

          <p className="text-xs text-slate-400 text-center mt-6">
            Hubungi HO Admin jika akun belum dibuat
          </p>
        </div>

        <p className="text-slate-400 text-xs text-center mt-4">Hanya untuk Penggunaan Internal · v1.0</p>
      </main>
    </div>
  );
}
