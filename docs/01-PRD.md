# Product Requirement Document
## Skintific Territory & Execution Platform (STEP)
**Tagline:** Plan. Execute. Monitor.

| | |
|---|---|
| **Doc owner** | Product / Design / Architecture (combined PRD) |
| **Status** | Draft v1.0 — for stakeholder review |
| **Audience** | Executive sponsors, Sales leadership, Engineering, SFA team |

---

## 1. Executive Summary

STEP is the internal enterprise system of record for **territory planning, route execution, target governance, and sales performance monitoring** across Skintific's field sales organization. It sits upstream of the existing Sales Force Automation (SFA) handheld apps (`SFA-Handheldv2`, `SFA-Portal`): STEP is where supervisors *plan* the work (routes, targets, outlet classification), while the SFA handheld is where reps *execute* the work in the field. STEP closes the loop by monitoring execution against plan and governing every change through an auditable approval chain.

Today, route planning, target allocation, and outlet/salesman data live across spreadsheets, ad-hoc Streamlit tools, and tribal knowledge. There is no single planning system, no formal approval trail for target or tier changes, and no unified view of route compliance vs. coverage vs. achievement. STEP consolidates this into one card-first, consumer-grade web application.

## 2. Problem Statement

| Problem today | Consequence |
|---|---|
| Route planning is manual/offline (spreadsheets, WhatsApp) | No visibility into coverage gaps until the month is over |
| Target changes are overwritten in place | No audit trail; disputes over "what was the target on date X" |
| Outlet tier classification is informal | Inconsistent service levels, no governance over high-value outlet reassignment |
| Approvals happen over chat/email | No SLA tracking, no accountability, slow turnaround |
| SFA sync issues are discovered reactively | Field reps' work silently fails to land in the system of record |
| No unified dashboard | Each role re-derives status from different spreadsheets |

## 3. Goals & Success Metrics

| Goal | Metric | Target |
|---|---|---|
| Single source of truth for territory plans | % of routes created in STEP vs. offline | ≥ 95% by end of Phase 1 rollout |
| Faster, auditable approvals | Median approval turnaround | ≤ 24h (from ≥ 3 days today) |
| Better route compliance | Route Compliance % (planned visits executed) | +10pp within 2 quarters |
| Reduce coverage gaps | Outlets with no visit > 30 days | -50% within 2 quarters |
| Trustworthy target history | Disputed target queries to Head Office | -80% (audit trail self-serves the answer) |
| Adoption by non-tech-savvy users | Task completion without support ticket | ≥ 90% of SPVs complete weekly route plan unassisted |

## 4. Non-Goals (Phase 1)

- STEP does **not** replace the SFA handheld apps — it does not do field check-in/check-out, photo capture, or order taking. That remains in `SFA-Handheldv2`.
- STEP does **not** automate route assignment. The Recommendation Engine *advises*; a human always commits the route (see §9).
- No Power BI embed in Phase 1 (reserved for Phase 3, only if in-app reporting proves insufficient — see [10-roadmap-and-backlog.md](10-roadmap-and-backlog.md)).
- No native mobile app — mobile is responsive web, not iOS/Android binaries.

## 5. Personas & Roles

| Role | Primary job | Device | Frequency |
|---|---|---|---|
| **SPV** (Supervisor) | Plans routes, allocates targets, proposes adjustments, reviews recommendations | Desktop (planning) + mobile (monitoring) | Daily |
| **Area Manager** | Approval Level 1, monitors team performance | Desktop + mobile | Daily |
| **Distributor Manager** *(Head Office)* | Approval Level 2 — national, monitors distributor relationships across the whole network, not one distributor | Desktop + mobile | Daily |
| **Regional Sales** *(Head Office)* | View-only oversight, national by default | Desktop + mobile | Weekly, ad hoc |
| **Head Office Admin** | Configures system, manages users/master data, monitors SFA + system health | Desktop | Daily (admin), continuous (monitoring) |

**Note:** Distributor Manager and Regional Sales are both Head Office roles, not territory-embedded ones — only SPV and Area Manager are genuinely bound to a single territory node. This matters for the approval chain's shape: SPV (local) → Area Manager (regional) → Distributor Manager (national) is a local→regional→national escalation, not three peers each owning a different patch of the map. Full permission matrix in [02-information-architecture.md](02-information-architecture.md).

## 6. Design Principles (non-negotiable)

1. **Enterprise grade + consumer-grade experience** — Salesforce-level capability, Notion/Linear-level polish.
2. **Card-first** — every primary entity (route, outlet, approval, target) renders as a card before it renders as a table.
3. **≤ 3 clicks** for any primary action (create route, approve, search outlet).
4. **Progressive disclosure** — list → card → detail panel → full record. Never dump every field on first view.
5. **Mobile responsive, touch-friendly** — see §11.
6. **Explainable, not black-box automation** — every system suggestion shows its reasoning (see §9).
7. **Built for non-tech-savvy sales users** — plain language, guided wizards over free-form forms, beautiful empty states that teach rather than just inform.

## 7. Scope by Module

### 7.1 Dashboard
Role-specific landing experience. KPI cards, achievement vs. target, route compliance, outlet coverage, pending actions, critical alerts, leaderboards, announcement feed. Always shows **Data As Of: DD MMM YYYY HH:mm WIB**. Supports cross-filter, drill-down, saved views, personalization/favorites. Full spec: [07-screen-specifications.md](07-screen-specifications.md#dashboard).

### 7.2 Route Planner
Primary SPV workspace. Calendar + Kanban views, daily/weekly planning, templates, copy-previous-week, manual drag-and-drop assignment, compliance/density/capacity/coverage-gap indicators, smart search, autosave + unsaved-change protection + offline draft persistence, guided wizard (Select Salesman → Week → Outlets → Review → Save). Full spec: [07-screen-specifications.md](07-screen-specifications.md#route-planner).

### 7.3 Route Recommendation Engine
Explainable, rule-based. Assignment stays manual — the engine only advises. Scoring factors: Last Visit Days, Sell-In Growth, Outlet Tier, Potential Score, New Outlet flag, Route Proximity. Categories: Critical / Recommended / Optional, each with a human-readable reasoning list. Rule weights configurable by Admin without a deployment. Full design: [07-screen-specifications.md](07-screen-specifications.md#recommendation-engine).

### 7.4 Outlet & Salesman (combined module)
Outlet 360 and Salesman 360 profiles, outlet-salesman mapping, sell-in/visit history, tier info with automatic rule-engine assignment + manual override (gated by the same 3-step approval chain as target adjustments), timeline, notes, attachments. All changes audited.

### 7.5 Target Management
Top-down area targets → SPV-distributed outlet allocation, constrained so **sum(outlet allocations) = area target**. What-if simulator, scenario planning, bulk adjustment, draft + copy-previous-month. Mid-month changes are **versioned, never overwritten** — historical achievement freezes at the version boundary, remaining target recalculates forward, full audit history retained.

### 7.6 Approval Governance
Three workflows — Target Adjustment, Tier Override, Request Reopen — all routed SPV → Area Manager → Distributor Manager. Inbox-style experience with comments, SLA indicator, and timeline. Rejection at any stage bounces back to the submitter (SPV), never to a dead end — same bounce-back principle validated in the PO Portal approval chain.

### 7.7 Reports & Analytics
Sell-In YTD, Average Sell-In YTD, Route Compliance, Coverage, Productivity, Outlet/Salesman Performance, Achievement, Leaderboards, Exception Reports. Advanced + saved filters, favorites, drill-down, export to Excel/CSV/PDF. Power BI embed reserved as a Phase 3 escape hatch if in-app reporting can't keep pace with data volume.

### 7.8 Administration
User/role management, master data, recommendation rule weights, approval matrix configuration, notification settings, announcement center, SFA integration monitor, scheduled jobs monitor, system health dashboard, feature flags, environment banner, audit logs, read-only impersonation.

### 7.9 Notification & Announcement Center
Categories: Approval, Routing, Target, System, Announcement. In-app + email + push, preferences, mark read/all read, deep links. Announcement Center lets Head Office/Admin broadcast Campaign / Policy / Meeting / Distributor Announcement / Training.

### 7.10 Import & Export Center
Upload → Preview → Validation → Confirm → Commit, with template download, inline error highlighting, downloadable error file, background processing + progress indicator. Bulk targets: Route Assignment, Outlet Classification Override, Outlet Mapping, Target Allocation, User Management.

### 7.11 Exception Center & Data Quality Dashboard
Surfaces missing routes, under-coverage, duplicate assignment, failed sync, missing mapping, underperforming areas. Actions: Assign / Dismiss / Request Exception.

### 7.12 SFA Integration Monitor
Last Sync, Success Rate, Failed Transactions, Retry Queue, Sync Status (Healthy / Partial Sync / Failed). Architecture detail: [09-sfa-integration-architecture.md](09-sfa-integration-architecture.md).

## 8. Functional Requirements Summary

See per-module detail above and screen specs. Cross-cutting functional requirements:

- **FR-1**: Every menu item and action is gated by RBAC + assigned territory (a user never sees data outside their scope).
- **FR-2**: Every mutating action (route save, target change, tier override, approval decision, user/master-data edit) writes an audit log entry with actor, timestamp, before/after value, and reason where applicable.
- **FR-3**: Target changes are append-only versions with effective dates; no in-place overwrite is permitted anywhere in the system, including Admin tooling.
- **FR-4**: Bulk import always runs Preview → Validate → Confirm before Commit; nothing commits without an explicit confirm step showing affected-record counts.
- **FR-5**: All list/detail screens provide a designed empty state with a clear next action — never a blank table.
- **FR-6**: Recommendation Engine output always pairs a score with plain-language reasoning; it never auto-commits a route.

## 9. Non-Functional Requirements

### 9.1 Performance budgets

| Interaction | Budget |
|---|---|
| Dashboard initial render | ≤ 3s |
| Search (outlet/salesman/global) | ≤ 2s |
| Approval action (approve/reject submit) | ≤ 1s |
| Save Route | ≤ 2s |
| Bulk upload | Background job + progress indicator (no hard budget — must not block UI) |
| Mobile Lighthouse Performance score | ≥ 90 |

### 9.2 Security

- SSO-ready (OIDC/SAML), session timeout, configurable password policy, RBAC enforced server-side (not just UI hiding), HTTPS-only, encryption at rest + in transit, IP-allowlist-ready, full audit trail. Detail: [05-api-recommendation.md](05-api-recommendation.md#security).

### 9.3 Reliability & data integrity

- Autosave + unsaved-change protection in Route Planner; offline draft persistence so a lost connection never loses planning work.
- SFA sync failures are visible (not silent) via the Integration Monitor and feed the Exception Center.

### 9.4 Accessibility & device support

- Desktop-first for planning-heavy screens (Route Planner, Target Management); full tablet responsiveness; mobile covers Dashboard, Approvals, Notifications, Monitoring, and light editing (see §11).
- Touch targets sized for field/warehouse use, not just office desks.

## 10. Branding & Visual Direction

- **Primary:** Skintific Light Blue · **Secondary:** Soft White · **Dark accent:** Navy · full Dark Mode support.
- 12–16px rounded corners, soft shadows, generous spacing, modern iconography, smooth transitions, large touch targets.
- Reference feel: Monday.com (board clarity), Notion (calm information density), Linear (speed + keyboard-friendly precision), Trello (drag-and-drop intuitiveness), Salesforce Lightning (enterprise data depth).
- Logo placeholder: `/assets/skintific-logo.png`.
- Full token spec: [06-design-system.md](06-design-system.md).

## 11. Platform & Responsive Behavior

| Tier | Scope |
|---|---|
| **Desktop** | Full command center — all modules, complex planning (Route Planner, Target Management, Administration) |
| **Tablet** | Responsive card layouts for all modules; planning remains usable but desktop is recommended |
| **Mobile** | Dashboard, Approvals, Notifications, Monitoring, light editing. Bottom navigation, sticky primary actions, deep links, touch-friendly controls |

## 12. Onboarding & Support

Interactive product tour, in-app Help Center, Release Notes feed, Demo Mode (sandboxed data for training without touching real territories).

## 13. Open Questions for Stakeholder Review

1. **Identity provider** — which SSO/IdP does Head Office IT standardize on (Azure AD / Google Workspace / other)? Drives §9.2 implementation.
2. **System of record boundary** — does STEP own outlet/salesman master data, or does it mirror master data still maintained in BigQuery `gt_schema`/`mt_schema`/`sadata`? (Affects [04-database-erd.md](04-database-erd.md) sync direction.)
3. **SFA contract ownership** — STEP's integration assumes `SFA-Handheldv2`/`SFA-Portal` as the field-execution source; confirm whether `SFA-Portal`'s Google Apps Script/Sheets backend is being retired before STEP integration, or STEP must integrate with that interim backend too.
4. **Approval matrix exceptions** — since Distributor Manager is a Head Office role (always exists, national scope), the more likely exception is the *Area Manager* step: are there small/flat territories with no separate Area Manager, where SPV submissions go straight to the Head Office Distributor Manager (collapsing 3 steps to 2)? Needs to be configurable, not hardcoded — see [08-approval-workflow.md](08-approval-workflow.md).
5. **Rollout sequencing** — pilot region/team before national rollout, or big-bang launch?

## 14. Related Documents

- [02-information-architecture.md](02-information-architecture.md) — IA, sitemap, RBAC nav matrix
- [03-ux-flows.md](03-ux-flows.md) — user flow diagrams
- [04-database-erd.md](04-database-erd.md) — entity diagram + DB recommendation
- [05-api-recommendation.md](05-api-recommendation.md) — API design + security
- [06-design-system.md](06-design-system.md) — design tokens + component library
- [07-screen-specifications.md](07-screen-specifications.md) — screen-by-screen spec incl. Dashboard, Route Planner, Recommendation Engine
- [08-approval-workflow.md](08-approval-workflow.md) — approval state machines
- [09-sfa-integration-architecture.md](09-sfa-integration-architecture.md) — SFA sync architecture
- [10-roadmap-and-backlog.md](10-roadmap-and-backlog.md) — phase roadmap + future backlog
- [`../prototype/`](../prototype/index.html) — clickable high-fidelity prototype
