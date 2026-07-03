# Information Architecture & Sitemap
## Skintific Territory & Execution Platform (STEP)

## 1. Sitemap

```
STEP
├── Login (SSO)
├── Dashboard                                   [all roles, content varies]
│   ├── Achievement vs Target
│   ├── Route Compliance
│   ├── Outlet Coverage
│   ├── Pending Actions
│   ├── Notifications widget
│   ├── Critical Alerts
│   ├── Leaderboards / Top Performing Areas / Areas Need Attention
│   └── Announcement Feed
│
├── Route Planner                                [SPV primary; AM/DM/RS/HO read-only drill-in]
│   ├── Calendar View
│   ├── Kanban View
│   ├── Route Template Library
│   ├── Planner Wizard (Select Salesman → Week → Outlets → Review → Save)
│   └── Recommendation Panel (Critical / Recommended / Optional)
│
├── Outlet & Salesman                            [combined module]
│   ├── Outlet List → Outlet 360
│   │     (Basic Info, Commercial Info, Visit Info, Target Info, Timeline, Attachments, Comments)
│   ├── Salesman List → Salesman 360
│   │     (Profile, Assigned Outlets, Coverage, Route Compliance, Sell-In, Achievement, Timeline, Attachments, Comments)
│   └── Outlet-Salesman Mapping
│
├── Target Management                            [SPV create/allocate; AM/DM approve; RS/HO view]
│   ├── Area Target (top-down, Head Office/Admin set)
│   ├── Outlet Target Allocation (SPV distributes)
│   ├── What-if Simulator / Scenario Planning
│   ├── Bulk Adjustment
│   └── Version History (effective-dated)
│
├── Approvals (Inbox)                            [SPV submits; AM = L1; DM = L2; RS/HO view]
│   ├── Target Adjustment requests
│   ├── Tier Override requests
│   └── Request Reopen requests
│
├── Reports                                      [all roles, scoped to territory]
│   ├── Sell-In YTD / Avg Sell-In YTD
│   ├── Route Compliance / Coverage / Productivity
│   ├── Outlet Performance / Salesman Performance / Achievement
│   ├── Leaderboards
│   └── Exception Reports
│
├── Notification Center                          [all roles]
├── Announcement Center                          [Head Office/Admin author; all roles consume]
├── Import & Export Center                       [SPV + Admin, scoped by permission]
│   └── Upload Wizard (Upload → Preview → Validate → Confirm → Commit)
├── Exception Center                             [AM/DM/HO primary]
├── Data Quality Dashboard                       [HO Admin primary, AM/DM view]
│
└── Administration                                [Head Office Admin only]
    ├── User Management
    ├── Role Management
    ├── Master Data
    ├── Recommendation Rules (weight configuration)
    ├── Approval Matrix Configuration
    ├── Notification Settings
    ├── Announcement Center (authoring)
    ├── SFA Integration Monitor
    ├── Scheduled Jobs Monitor
    ├── System Health Dashboard
    ├── Feature Flags
    ├── Environment Banner
    ├── Audit Logs
    └── Read-only Impersonation
```

## 2. Primary Sidebar (role-adaptive)

Per the brief, the sidebar always shows these 6 top-level groups; items inside a group are filtered by RBAC + territory scope:

1. **Dashboard**
2. **Route Planner**
3. **Outlet & Salesman**
4. **Target Management**
5. **Reports**
6. **Administration** *(Head Office Admin only — hidden entirely for other roles)*

Notifications and Announcements live in the top bar (bell icon + megaphone icon), not the primary sidebar, since they're cross-cutting rather than a "module." Approvals surface as a top-bar inbox icon with an unread-count badge for AM/DM (and as a "My Submissions" tab for SPV), in addition to being reachable from Dashboard's Pending Action widget.

## 3. RBAC × Navigation Matrix

| Sidebar item | SPV | Area Manager | Distributor Manager (HO) | Regional Sales (HO) | Head Office Admin |
|---|---|---|---|---|---|
| Dashboard | ✅ own territory | ✅ own area | ✅ national | ✅ national | ✅ national |
| Route Planner | ✅ full edit | 👁 view team routes | 👁 view routes nationally | 👁 view only | 👁 view only |
| Outlet & Salesman | ✅ edit (own territory) | ✅ approve tier/mapping changes | ✅ approve tier/mapping changes (national) | 👁 view only | ✅ full (master data) |
| Target Management | ✅ allocate (own territory) | ✅ approve L1 | ✅ approve L2 (national) | 👁 view only | ✅ set area targets (top-down) |
| Approvals Inbox | 📤 submit / track | ✅ act as L1 approver | ✅ act as L2 approver (national) | 👁 monitor only | 👁 monitor only |
| Reports | ✅ own territory | ✅ own area | ✅ national | ✅ national | ✅ national |
| Notifications | ✅ | ✅ | ✅ | ✅ | ✅ (+ national broadcast) |
| Announcement Center (consume) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Announcement Center (author) | ❌ | ❌ | ❌ | ❌ | ✅ |
| Import & Export Center | ✅ scoped (routes, own targets) | 👁 view jobs | 👁 view jobs | ❌ | ✅ all entities |
| Exception Center | 📤 raise exception | ✅ assign/dismiss | ✅ assign/dismiss (national) | 👁 view | ✅ assign/dismiss |
| Data Quality Dashboard | ❌ | 👁 own area | 👁 national | 👁 view | ✅ full |
| Administration (all sub-items) | ❌ | ❌ | ❌ | ❌ | ✅ |

Legend: ✅ full access · 👁 read-only · 📤 submitter role (creates requests, doesn't approve) · ❌ not visible

**RBAC enforcement rule (FR-1 from PRD):** this table governs UI visibility *and* must be re-enforced server-side on every API call — the frontend hiding a menu item is a UX convenience, never the security boundary. See [05-api-recommendation.md](05-api-recommendation.md#security).

## 4. Territory Scoping Model

**Correction from initial draft:** Distributor Manager and Regional Sales are **Head Office officers**, not territory-embedded roles — they get the **national** view by default, the same way Head Office Admin does. Only SPV and Area Manager are genuinely territory-bound. This changes the approval chain's character: SPV (local) → Area Manager (regional) → Distributor Manager (national, Head Office) is a local→regional→national escalation, not three peers each owning a different patch of the map.

```
National
 └── Region
      └── Area
           └── Distributor / Branch
                └── Route / Salesman
```

- **SPV** → scoped to one or more Areas/Branches they supervise.
- **Area Manager** → scoped to one Area (approves everything submitted within it).
- **Distributor Manager** → Head Office officer, **national scope** — acts as the L2 approver across all areas/distributors nationally, not one distributor specifically.
- **Regional Sales** → Head Office officer, **national scope by default** (the brief's "multi-area or national view depending on access" — national is the default access level since the role sits at Head Office) — view-only regardless of breadth.
- **Head Office Admin** → unscoped (National + configuration access).

A user can hold the same role across multiple territory nodes (e.g., an SPV covering two Areas); the UI exposes a **territory switcher** in the top bar whenever a user's scope includes more than one node. Distributor Manager, Regional Sales, and Head Office Admin don't need this switcher — their scope is already national.

## 5. Cross-Cutting Surfaces (not in sidebar, reachable from anywhere)

- **Global Search** (top bar) — outlets, salesmen, routes, approvals; ≤ 2s budget per PRD §9.1.
- **Notification Center** — bell icon, badge count, deep-links into the originating record.
- **Approvals Inbox** — inbox icon, badge count for approvers.
- **Help Center / Product Tour / Release Notes** — "?" icon, available everywhere, never gated by role.
- **Environment Banner** — shown to Admin in non-production environments (UAT/staging) so configuration changes are never mistaken for production.

## 6. Related Documents

[01-PRD.md](01-PRD.md) · [03-ux-flows.md](03-ux-flows.md) · [07-screen-specifications.md](07-screen-specifications.md)
