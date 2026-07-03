# Screen-by-Screen Specifications
## Skintific Territory & Execution Platform (STEP)

Each spec below maps directly to a page in [`../prototype/`](../prototype/index.html). Where behavior differs by role, the prototype's role switcher demonstrates the variant live.

---

## Dashboard

**File:** `prototype/dashboard.html`

### Layout
- Top: Data freshness strip — `Data As Of: 25 Jun 2026 14:32 WIB` + manual refresh icon.
- Row 1: KPI cards (4-up desktop / 2-up tablet / 1-up mobile) — content varies by role (see below).
- Row 2: two-column — left = Achievement vs Target chart + Route Compliance gauge; right = Pending Action widget + Critical Alerts.
- Row 3: Leaderboards / Top Performing Areas / Areas Need Attention (tabbed within one widget card).
- Row 4: Announcement Feed (latest 3, "View all" → Announcement Center).
- All cards support a "⋮" menu: **Pin to favorites**, **Drill down**, **Save as view**.

### Role variants

| Role | KPI cards show | Pending Action widget shows |
|---|---|---|
| SPV | My Team Achievement %, Route Compliance %, Outlets Visited Today, Coverage Gap Count | Incomplete target allocation, Missing routes, Missing visits |
| Area Manager | Area Achievement %, Pending Approvals, Underperforming SPVs, Route Compliance (area) | Pending approvals queue, Underperforming teams |
| Distributor Manager *(Head Office, national)* | National Distributor Achievement %, Pending Approvals (L2, national), Active Exceptions (national), Sell-In YTD (national) | Pending approvals queue (national), Underperforming areas |
| Regional Sales *(Head Office, national by default)* | National Achievement %, Areas On-Track vs At-Risk, Avg Route Compliance, Top Region | (read-only — no action widget, shows "Approval Monitoring" summary instead) |
| Head Office Admin | National Achievement %, SFA Sync Health %, Active Data Quality Issues, Open Exceptions | National issues, Sync failures, Data quality issues |

### Capabilities
- **Cross-filter:** selecting a territory in the top-bar switcher re-filters every widget on the page without reload.
- **Drill-down:** clicking any KPI number navigates to the relevant module pre-filtered (e.g., "Coverage Gap Count" → Outlet & Salesman list filtered to `last_visit > 30d`).
- **Saved Views / Personalization:** "Save current layout as a view," widget reorder (drag), per-user favorite widgets pinned to top.
- **Performance:** initial render ≤ 3s (NFR) — achieved via one composed `/dashboard` payload (see [05-api-recommendation.md](05-api-recommendation.md#5-performance-considerations-mapped-to-nfrs)), skeleton-loading cards rather than a blocking spinner.

### Empty states
- No pending actions → "You're all caught up." illustration + secondary link to Reports.
- No announcements → hidden entirely (not an empty card) to avoid clutter on a dashboard.

---

## Route Planner

**File:** `prototype/route-planner.html`

### Layout
- Top: week navigator (prev/next + label) + view toggle (Calendar / Kanban) + "+ Plan Route" wizard entry.
- Salesman search bar (full-width, above the 3-column layout) — search by **Salesman Name, Salesman ID, Area, or Distributor** — plus a **Filters** button (badge shows active filter count) opening a panel of chips: **Region, Distributor, Area, SPV, Status**. Needed because the platform operates at national scale — dozens of salesmen, not a handful — so the flat list from earlier drafts no longer scales. Desktop: filter panel opens as a right-side drawer. Mobile: filter panel opens as a bottom sheet; the search bar is sticky just below the top bar.
- Left: salesman rail, filtered live by the search/chips above, scrollable independently of the board.
- Center: **Calendar view** = 7-day grid, each day cell shows outlet count (density dots), a capacity bar, and a coverage-gap flag if applicable. **Kanban view** = one column per day, draggable outlet cards; each card shows visit sequence number, outlet code, outlet name, tier, planned visit time, last sell-in, and last visit date — not just name + tier. A small "+" on each day header opens the **Add Outlet** panel (below) scoped to that day.
- Right (persistent, collapsible): **Recommendation Panel** — Critical / Recommended / Optional tabs, each a scrollable list of recommendation cards (see Recommendation Engine below). Never modal — always visible alongside the planner per PRD FR-6.

### Default route behavior (weekly continuity)
Business rule: a weekly route generally stays the same unless someone deliberately changes it. Concretely:
- Every salesman has a real baseline weekly route (Monday–Saturday, 10–12 outlets/day) rather than starting from a blank board — see [04-database-erd.md](04-database-erd.md) for how this maps to `ROUTE`/`ROUTE_ITEM`.
- Navigating to a week that hasn't been visited yet **automatically copies the previous week's route**, preserving outlet order and visit sequence, and shows a banner: *"This week's route is copied from previous week."* with three actions — **Edit Route** (acknowledge and start editing), **Reset to Previous Week** (re-copy, discarding edits made this session), **Apply Route Template** (regenerate the canonical default route for this salesman).
- If no previous route exists at all, the planner starts empty with a "No Route Assigned Yet" empty state (Create Route / Apply Template).

### Add Outlet panel (search & add)
A dedicated search-and-add flow, distinct from the curated Recommendation Panel: search the outlet master by **Outlet Code, Outlet Name, Tier, or Area**; results show Code, Name, Tier, Area, Last Sell-In, and Last Visit, each with an **Add** action that places the outlet on the selected day. Desktop: searchable modal with a results table. Mobile: the same panel renders edge-to-edge (full-screen) with larger touch targets and a sticky search field.

### Planner Wizard (guided flow, modal stepper)
1. **Select Salesman** (now a small search-filtered list, not a flat radio list — necessary at national scale)
2. **Select Week** (with "Copy Previous Week" and "Apply Template" shortcuts on this step)
3. **Add Outlets** (quick-add Critical recommendations; full search/drag-and-drop continues after closing the wizard)
4. **Review** (density/capacity/coverage-gap summary, warnings if any day is over capacity)
5. **Save** (autosave already happened throughout; this is the explicit "Submit" / commit-to-team-visible state)

### Indicators
- **Route Density** — dot cluster per day cell, color shifts at a configurable threshold (e.g. >12 outlets/day = amber).
- **Route Capacity** — thin progress bar under each day, red when over the salesman's configured daily capacity.
- **Coverage Gap** — small flag icon on outlets not visited in > 30 days, surfaced both on the outlet card and aggregated as a Dashboard KPI.

### Save behavior
- **Autosave**: every change persists to a local draft every few seconds and on blur (NFR-adjacent: protects planning work, no DB round-trip required for every keystroke).
- **Unsaved Change Protection**: navigating away with an uncommitted draft triggers a confirm dialog.
- **Offline Draft Persistence**: draft state lives in browser storage keyed by salesman+week, so a lost connection or accidental tab close never loses work — resumes exactly where left off.
- **Explicit Save**: ≤ 2s budget (NFR), shows inline success toast, not a page reload.

### Mobile behavior
Mobile planning is genuinely editable, not read-only — consistent with the platform's general mobile principle of ≤3 taps for a primary action:
- The weekly board becomes horizontally scrollable day cards (swipe between days) rather than a forced 7-column squeeze; within a day, route cards stack vertically.
- Native drag-and-drop doesn't translate to touch, so each card exposes a "⋮" action sheet — **Move to day** / **Remove** — as the touch-friendly equivalent of dragging.
- Sticky action bar pinned above the bottom navigation: **Save Draft** / **Submit**, always reachable without scrolling.

---

## Recommendation Engine

**File:** rendered inside `prototype/route-planner.html` (Recommendation Panel) + configured in `prototype/administration.html` (Recommendation Rules tab).

### Principle
**Assignment stays manual.** The engine never writes a route — it only ranks and explains. This is the same explainability bar as the design system's general principle: a non-tech-savvy SPV must be able to read *why* a card is flagged without training.

### Scoring factors (configurable weights)

| Factor | Signal | Default weight |
|---|---|---|
| Last Visit Days | days since last visit, normalized | 25% |
| Sell-In Growth | trailing 4-week sell-in trend (negative = higher score) | 20% |
| Outlet Tier | S/A outlets weighted higher | 20% |
| Potential Score | from outlet master data (category potential index) | 15% |
| New Outlet | binary boost for outlets onboarded < 30 days ago | 10% |
| Route Proximity | distance from other outlets already in the day's route | 10% |

`score = Σ (factor_normalized × weight)`, computed per outlet per candidate week, recalculated nightly (or on-demand for the visible week).

### Categories
- **Critical** (score ≥ 0.75) — red ribbon, e.g. "18 days unvisited + Tier S + Sell-In -32%."
- **Recommended** (0.45–0.74) — amber ribbon.
- **Optional** (< 0.45) — neutral ribbon, collapsed by default.

### Card content (always all three)
1. **Score** (numeric + category ribbon)
2. **Reasoning** — plain-language checklist, e.g.:
   ```
   ✓ Last visit 18 days ago
   ✓ Sell-In down 32%
   ✓ Tier S outlet
   ✓ High potential
   ```
3. **Suggested action** — "Add to Tuesday" (pre-fills the least-dense eligible day) — one click, lands the outlet on the planner; SPV still drags/confirms placement.

### Admin rule management (`administration.html` → Recommendation Rules)
- Sliders for each factor's weight (must sum to 100%, UI enforces this live).
- Threshold inputs for Critical/Recommended/Optional cutoffs.
- **No deployment required** — weight changes take effect on next score computation, satisfying the brief's "modify rule weights without deployment."
- Every weight change is audit-logged (who, when, old → new).

---

## Related Documents

[01-PRD.md](01-PRD.md) · [02-information-architecture.md](02-information-architecture.md) · [06-design-system.md](06-design-system.md) · [`../prototype/`](../prototype/index.html)
