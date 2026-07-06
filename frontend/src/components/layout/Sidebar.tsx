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
  { to: "/dashboard",           label: "Dashboard",           icon: "📊", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/route-planner",       label: "Route Planner",       icon: "🗺️", roles: ["spv","asm","dm","ho_admin"] },
  { to: "/route-evaluate",      label: "Route Evaluate",      icon: "📈", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/target-management",   label: "Target Management",   icon: "🎯", roles: ["spv","asm","dm","ho_admin"] },
  { to: "/approvals",           label: "Approvals",           icon: "✅", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/outlet-salesman",     label: "Outlet & Salesman",   icon: "🏬", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/reports",             label: "Reports",             icon: "📋", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/announcements",       label: "Announcements",       icon: "📢", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/master-data-pjp",     label: "Master Data PJP",     icon: "📌", roles: ["asm","dm","ho_admin"] },
  { to: "/master-data-salesman",label: "Master Salesman",     icon: "👥", roles: ["asm","dm","ho_admin"] },
  { to: "/store-opportunity",   label: "Store Opportunity",   icon: "💡", roles: ["asm","dm","rsm","ho_admin"] },
  { to: "/store360",            label: "Store 360°",          icon: "🔍", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/salesman360",         label: "Salesman 360°",       icon: "👤", roles: ["spv","asm","dm","rsm","ho_admin"] },
  { to: "/import-export",       label: "Import & Export",     icon: "⇅",  roles: ["dm","ho_admin"] },
  { to: "/administration",      label: "Administration",      icon: "⚙️", roles: ["ho_admin"] },
  { to: "/notifications",       label: "Notifikasi",          icon: "🔔", roles: ["spv","asm","dm","rsm","ho_admin"] },
];

export default function Sidebar() {
  const { user, logout } = useAuthStore();
  const navigate = useNavigate();
  const qc = useQueryClient();
  const role = user?.role as Role;

  const visible = NAV.filter((n) => n.roles.includes(role));

  return (
    <aside className="w-60 min-h-screen bg-primary-800 flex flex-col">
      {/* Brand */}
      <div className="px-5 py-5 border-b border-primary-700">
        <div className="flex items-center gap-2">
          <span className="text-2xl">🗺️</span>
          <div>
            <p className="text-white font-bold text-sm leading-tight">STEP</p>
            <p className="text-primary-300 text-xs">Territory &amp; Execution</p>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-3 py-4 space-y-0.5 overflow-y-auto">
        {visible.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                isActive
                  ? "bg-primary-600 text-white"
                  : "text-primary-200 hover:bg-primary-700 hover:text-white"
              }`
            }
          >
            <span className="text-base">{item.icon}</span>
            {item.label}
          </NavLink>
        ))}
      </nav>

      {/* User */}
      <div className="px-4 py-4 border-t border-primary-700">
        <div className="flex items-center gap-3 mb-3">
          <div className="w-8 h-8 rounded-full bg-primary-500 flex items-center justify-center text-white text-sm font-bold">
            {user?.username?.[0]?.toUpperCase()}
          </div>
          <div className="min-w-0">
            <p className="text-white text-sm font-medium truncate">{user?.username}</p>
            <p className="text-primary-300 text-xs uppercase">{user?.role}</p>
          </div>
        </div>
        <button
          onClick={() => {
            qc.clear();                           // purge all cached API data
            logout();                             // clear token + Zustand state
            navigate("/login", { replace: true }); // replace history so Back can't return
          }}
          className="w-full text-left text-primary-300 hover:text-white text-xs py-1 transition-colors"
        >
          → Keluar
        </button>
      </div>
    </aside>
  );
}
