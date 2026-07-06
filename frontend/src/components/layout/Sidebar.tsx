import { NavLink, useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { useAuthStore } from "@/store/authStore";
import type { Role } from "@/types";

interface NavItem {
  to: string;
  label: string;
  icon: string;
  roles: Role[];
}

const NAV: NavItem[] = [
  { to: "/dashboard",            label: "Dashboard",          icon: "📊", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/route-planner",        label: "Route Planner",      icon: "🗺️", roles: ["spv","asm","dm","ho_admin"] },
  { to: "/route-evaluate",       label: "Route Evaluate",     icon: "📈", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/visits",               label: "Visit & Demand",     icon: "📦", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/target-management",    label: "Target Management",  icon: "🎯", roles: ["spv","asm","dm","ho_admin"] },
  { to: "/approvals",            label: "Approvals",          icon: "✅", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/outlet-salesman",      label: "Outlet & Salesman",  icon: "🏬", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/reports",              label: "Reports",            icon: "📋", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/announcements",        label: "Announcements",      icon: "📢", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/master-data-pjp",      label: "Master Data PJP",   icon: "📌", roles: ["asm","dm","ho_admin"] },
  { to: "/master-data-salesman", label: "Master Salesman",   icon: "👥", roles: ["asm","dm","ho_admin"] },
  { to: "/store-opportunity",    label: "Store Opportunity",  icon: "💡", roles: ["asm","dm","rsm","ho_admin"] },
  { to: "/store360",             label: "Store 360°",         icon: "🔍", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/salesman360",          label: "Salesman 360°",      icon: "👤", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/import-export",        label: "Import & Export",    icon: "⇅",  roles: ["dm","ho_admin"] },
  { to: "/administration",       label: "Administration",     icon: "⚙️", roles: ["ho_admin"] },
  { to: "/notifications",        label: "Notifikasi",         icon: "🔔", roles: ["spv","asm","dm","rsm","ho_admin"] },
];

export default function Sidebar() {
  const { user, logout } = useAuthStore();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const role = user?.role as Role;

  const visible = NAV.filter((n) => n.roles.includes(role));

  return (
    <aside className="w-60 min-h-screen bg-slate-900 flex flex-col shrink-0">
      {/* ── Brand ── */}
      <div className="px-5 py-5 border-b border-white/10">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary-600 flex items-center justify-center shrink-0">
            <span className="text-sm leading-none">🗺️</span>
          </div>
          <div>
            <p className="text-white font-bold text-sm leading-tight tracking-wide">STEP</p>
            <p className="text-slate-500 text-xs mt-0.5">Territory &amp; Execution</p>
          </div>
        </div>
      </div>

      {/* ── Navigation ── */}
      <nav className="flex-1 px-3 py-4 space-y-0.5 overflow-y-auto">
        {visible.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-150 ${
                isActive
                  ? "bg-primary-600 text-white shadow-sm"
                  : "text-slate-400 hover:bg-white/[0.07] hover:text-slate-100"
              }`
            }
          >
            <span className="text-base leading-none shrink-0">{item.icon}</span>
            <span className="truncate">{item.label}</span>
          </NavLink>
        ))}
      </nav>

      {/* ── User footer ── */}
      <div className="px-4 py-4 border-t border-white/10">
        <div className="flex items-center gap-3 mb-3">
          <div className="w-8 h-8 rounded-full bg-primary-600 flex items-center justify-center text-white text-sm font-bold shrink-0">
            {user?.username?.[0]?.toUpperCase()}
          </div>
          <div className="min-w-0 flex-1">
            <p className="text-white text-sm font-medium truncate">{user?.username}</p>
            <p className="text-slate-500 text-xs uppercase tracking-wide mt-0.5">
              {user?.role}
            </p>
          </div>
        </div>
        <button
          onClick={() => {
            qc.clear();
            logout();
            navigate("/login", { replace: true });
          }}
          className="w-full text-left text-slate-500 hover:text-slate-200 text-xs py-1.5
                     transition-colors duration-150 flex items-center gap-1.5 group"
        >
          <span className="group-hover:translate-x-0.5 transition-transform duration-150">→</span>
          <span>Keluar</span>
        </button>
      </div>
    </aside>
  );
}
