import { useState, useEffect } from "react";
import { NavLink, useNavigate, useLocation } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { useAuthStore } from "@/store/authStore";
import type { Role } from "@/types";

// ── Types ──────────────────────────────────────────────────────────────────────
interface NavLeaf {
  to: string;
  label: string;
  roles: Role[];
}

interface NavGroup {
  kind: "group";
  id: string;
  label: string;
  icon: string;
  children: NavLeaf[];
}

interface NavSingle {
  kind: "single";
  to: string;
  label: string;
  icon: string;
  roles: Role[];
}

type NavItem = NavGroup | NavSingle;

// ── Navigation tree ───────────────────────────────────────────────────────────
// RBAC source of truth: roles[] on each leaf/single item.
// Parent groups appear only when ≥1 child is visible for the current role.
// URLs are 1-to-1 with existing routes in App.tsx — nothing renamed.
const NAV_TREE: NavItem[] = [
  {
    kind: "single",
    to: "/dashboard",
    label: "Dashboard",
    icon: "📊",
    roles: ["spv", "asm", "dm", "rsm", "ho_admin"],
  },
  {
    kind: "group",
    id: "master-data",
    label: "Master Data",
    icon: "🗂️",
    children: [
      { to: "/route-planner",        label: "Route Planner",     roles: ["spv", "asm", "dm", "ho_admin"] },
      { to: "/master-data-pjp",      label: "Master Data PJP",   roles: ["asm", "dm", "ho_admin"] },
      { to: "/master-data-salesman", label: "Master Salesman",   roles: ["asm", "dm", "ho_admin"] },
      { to: "/target-management",    label: "Target Management", roles: ["spv", "asm", "dm", "ho_admin"] },
      { to: "/outlet-salesman",      label: "Outlet & Salesman", roles: ["spv", "asm", "dm", "rsm", "ho_admin"] },
    ],
  },
  {
    kind: "group",
    id: "reports",
    label: "Reports",
    icon: "📈",
    children: [
      { to: "/route-evaluate",    label: "Route Evaluate",    roles: ["spv", "asm", "dm", "rsm", "ho_admin"] },
      { to: "/visits",            label: "Visit & Demand",    roles: ["spv", "asm", "dm", "rsm", "ho_admin"] },
      { to: "/store-opportunity", label: "Store Opportunity", roles: ["asm", "dm", "rsm", "ho_admin"] },
      { to: "/store360",          label: "Store 360°",        roles: ["spv", "asm", "dm", "rsm", "ho_admin"] },
      { to: "/salesman360",       label: "Salesman 360°",     roles: ["spv", "asm", "dm", "rsm", "ho_admin"] },
    ],
  },
  {
    kind: "single",
    to: "/approvals",
    label: "Approvals",
    icon: "✅",
    roles: ["spv", "asm", "dm", "rsm", "ho_admin"],
  },
  {
    kind: "single",
    to: "/import-export",
    label: "Import & Export",
    icon: "⇅",
    roles: ["dm", "ho_admin"],
  },
  {
    kind: "single",
    to: "/announcements",
    label: "Announcements",
    icon: "📢",
    roles: ["spv", "asm", "dm", "rsm", "ho_admin"],
  },
  {
    kind: "single",
    to: "/administration",
    label: "Administration",
    icon: "⚙️",
    roles: ["ho_admin"],
  },
  {
    kind: "single",
    to: "/notifications",
    label: "Notifikasi",
    icon: "🔔",
    roles: ["spv", "asm", "dm", "rsm", "ho_admin"],
  },
];

// ── Helpers ───────────────────────────────────────────────────────────────────
function isGroup(item: NavItem): item is NavGroup {
  return item.kind === "group";
}

function canSee(item: NavItem, role: Role): boolean {
  if (isGroup(item)) return item.children.some((c) => c.roles.includes(role));
  return item.roles.includes(role);
}

function groupIsActive(group: NavGroup, pathname: string): boolean {
  return group.children.some(
    (c) => pathname === c.to || pathname.startsWith(c.to + "/"),
  );
}

// ── Chevron SVG ───────────────────────────────────────────────────────────────
function Chevron({ open }: { open: boolean }) {
  return (
    <svg
      viewBox="0 0 20 20"
      fill="currentColor"
      className={`w-3.5 h-3.5 shrink-0 transition-transform duration-200 ${open ? "rotate-90" : ""}`}
    >
      <path
        fillRule="evenodd"
        d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1
           0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z"
        clipRule="evenodd"
      />
    </svg>
  );
}

// ── Sidebar ───────────────────────────────────────────────────────────────────
export default function Sidebar() {
  const { user, logout } = useAuthStore();
  const navigate = useNavigate();
  const location = useLocation();
  const qc = useQueryClient();
  const role = user?.role as Role;

  // Set of group IDs that are currently expanded
  const [openGroups, setOpenGroups] = useState<Set<string>>(() => {
    // Pre-expand whichever group contains the initial route
    const initial = new Set<string>();
    for (const item of NAV_TREE) {
      if (isGroup(item) && groupIsActive(item, location.pathname)) {
        initial.add(item.id);
      }
    }
    return initial;
  });

  // When the route changes, ensure the owning group is expanded (back/forward,
  // direct URL access, programmatic navigation).
  useEffect(() => {
    for (const item of NAV_TREE) {
      if (isGroup(item) && groupIsActive(item, location.pathname)) {
        setOpenGroups((prev) => {
          if (prev.has(item.id)) return prev;
          return new Set([...prev, item.id]);
        });
        break;
      }
    }
  }, [location.pathname]);

  const toggleGroup = (id: string) =>
    setOpenGroups((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  return (
    <aside className="w-60 min-h-screen bg-slate-900 flex flex-col shrink-0">
      {/* ── Brand ── */}
      <div className="px-5 py-5 border-b border-white/10 shrink-0">
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
      <nav className="flex-1 px-3 py-4 overflow-y-auto space-y-0.5">
        {NAV_TREE.filter((item) => canSee(item, role)).map((item) => {
          if (isGroup(item)) {
            const open    = openGroups.has(item.id);
            const active  = groupIsActive(item, location.pathname);
            const visible = item.children.filter((c) => c.roles.includes(role));

            return (
              <div key={item.id}>
                {/* Parent button */}
                <button
                  onClick={() => toggleGroup(item.id)}
                  className={`
                    w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm
                    font-medium transition-all duration-150 select-none
                    ${active
                      ? "bg-white/10 text-white"
                      : "text-slate-400 hover:bg-white/[0.07] hover:text-slate-100"}
                  `}
                >
                  <span className="text-base leading-none shrink-0">{item.icon}</span>
                  <span className="flex-1 text-left truncate">{item.label}</span>
                  <Chevron open={open} />
                </button>

                {/* Children — max-height animation; no JS animation library needed */}
                <div
                  style={{ maxHeight: open ? `${visible.length * 48}px` : "0px" }}
                  className="overflow-hidden transition-[max-height] duration-200 ease-in-out"
                >
                  <div className="mt-0.5 ml-3 pl-3 border-l border-white/10 space-y-0.5 pb-1">
                    {visible.map((child) => (
                      <NavLink
                        key={child.to}
                        to={child.to}
                        className={({ isActive }) =>
                          `flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm
                           transition-all duration-150
                           ${isActive
                             ? "bg-primary-600 text-white font-semibold shadow-sm"
                             : "text-slate-400 hover:bg-white/[0.07] hover:text-slate-100 font-normal"
                           }`
                        }
                      >
                        {/* Bullet — small filled circle, visually lighter than an icon */}
                        <span className="w-1.5 h-1.5 rounded-full bg-current opacity-50 shrink-0 mt-px" />
                        <span className="truncate">{child.label}</span>
                      </NavLink>
                    ))}
                  </div>
                </div>
              </div>
            );
          }

          // ── Single leaf item ──────────────────────────────────────────────
          return (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium
                 transition-all duration-150
                 ${isActive
                   ? "bg-primary-600 text-white shadow-sm"
                   : "text-slate-400 hover:bg-white/[0.07] hover:text-slate-100"
                 }`
              }
            >
              <span className="text-base leading-none shrink-0">{item.icon}</span>
              <span className="truncate">{item.label}</span>
            </NavLink>
          );
        })}
      </nav>

      {/* ── User footer ── */}
      <div className="px-4 py-4 border-t border-white/10 shrink-0">
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
