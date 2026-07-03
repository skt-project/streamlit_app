# Design System & Component Library
## Skintific Territory & Execution Platform (STEP)

This is the token + component spec implemented literally in [`../prototype/assets/css/step.css`](../prototype/assets/css/step.css) — treat this doc and that file as the same source of truth; if they ever drift, the CSS file wins and this doc should be updated to match.

## 1. Color Tokens

### Light mode

| Token | Value | Usage |
|---|---|---|
| `--primary-50` | `#EAF6FE` | tinted backgrounds, hover states |
| `--primary-100` | `#D2ECFC` | selected nav item background |
| `--primary-300` | `#7FC8F2` | chart accents |
| `--primary-500` | `#2BA6E0` | **Skintific Light Blue** — primary actions, links, active states |
| `--primary-600` | `#1B8AC4` | primary button hover |
| `--primary-700` | `#146B9C` | primary text-on-light emphasis |
| `--navy-900` | `#0B1F33` | **Dark accent** — sidebar background, headings on light, dark-mode surface |
| `--navy-700` | `#16314D` | dark-mode card surface |
| `--soft-white` | `#F6F9FC` | **Secondary** — app canvas background |
| `--surface` | `#FFFFFF` | card/panel surface |
| `--ink` | `#10202F` | primary text |
| `--ink-soft` | `#54677A` | secondary text |
| `--ink-faint` | `#8B9AA8` | placeholder/disabled text |
| `--line` | `#E3EAF0` | borders, dividers |
| `--success` / `--success-bg` | `#1C9A6C` / `#E6F6EF` | approved, healthy, on-track |
| `--warning` / `--warning-bg` | `#C2780C` / `#FBF1DD` | at-risk, partial sync, pending |
| `--danger` / `--danger-bg` | `#D03B3B` / `#FBEAEA` | rejected, failed, breached SLA |
| `--info` / `--info-bg` | `#2B6FE0` / `#EAF1FE` | informational badges |

### Dark mode (`[data-theme="dark"]`)

| Token | Value |
|---|---|
| `--soft-white` (canvas) | `#0B1620` |
| `--surface` | `#102233` |
| `--ink` | `#EAF2FA` |
| `--ink-soft` | `#9FB3C4` |
| `--line` | `#1E3349` |
| `--navy-900` (sidebar) | `#081420` |
| `--primary-500` | `#4FB8EE` *(brightened for contrast on dark)* |

Semantic success/warning/danger backgrounds shift to ~12% opacity tints of the same hue rather than the light-mode pastel fills.

## 2. Typography

- **Font:** `Inter, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif` — chosen for enterprise SaaS legibility at small sizes (Linear/Notion reference) and wide numeral support (KPI-heavy dashboards).
- **Scale:** `32/24/20/16/14/13/12px`, weights 700 (display/headings), 600 (labels/buttons), 400 (body).
- Numerals in KPI cards use `font-variant-numeric: tabular-nums` so achievement percentages don't jitter on update.

## 3. Geometry & Elevation

| Token | Value |
|---|---|
| `--radius-sm` | 10px — inputs, small badges |
| `--radius-md` | 14px — cards, buttons |
| `--radius-lg` | 18px — modals, large panels |
| `--radius-pill` | 999px — status pills, avatar |
| `--shadow-sm` | `0 1px 2px rgba(11,31,51,.06)` |
| `--shadow-md` | `0 8px 24px rgba(11,31,51,.08), 0 2px 6px rgba(11,31,51,.05)` |
| `--shadow-lg` | `0 24px 56px rgba(11,31,51,.16)` |

Spacing scale: 4px base — `4 8 12 16 20 24 32 40 48 64`.

## 4. Component Inventory

| Component | Notes |
|---|---|
| **Sidebar nav** | Navy background, 6 top-level items, active item = primary-500 left bar + tinted background, role-filtered |
| **Top bar** | Global search, territory switcher, notification bell (badge), approvals inbox icon (badge), dark-mode toggle, avatar menu |
| **KPI card** | Label, big number (tabular-nums), trend chip (▲/▼/flat with semantic color), optional sparkline |
| **Widget card** | Generic container for dashboard widgets — header with title + "View all" link, body, optional empty state |
| **Status badge / pill** | Maps 1:1 to entity statuses: `draft / submitted / approved / rejected / locked / archived`, sync `healthy / partial / failed`, tier `S / A / B / C / D` |
| **SLA indicator** | Color-coded countdown chip: green (on track) → amber (at risk, <25% time left) → red (breached) |
| **Progress bar** | Used in Target Allocation (allocated vs. remaining) and Import jobs (% complete) |
| **Kanban card** | Route Planner — outlet name, tier badge, last-visit chip, drag handle |
| **Calendar cell** | Route Planner — day cell with density indicator (dot count) and capacity warning state |
| **Recommendation card** | Category ribbon (Critical/Recommended/Optional), score, reasoning checklist, "Add to route" action |
| **Approval inbox row** | Requester avatar, type badge, SLA indicator, expands to detail panel (slide-in from right) |
| **Detail panel / drawer** | Right-side slide-in, used for Outlet 360 quick view, Approval detail, Notification deep-link target |
| **Wizard stepper** | Horizontal steps with current/complete/upcoming states — Route Planner wizard, Import wizard |
| **Empty state** | Icon + headline + one-line support text + primary action button — never a bare "No data" |
| **Toast** | Bottom-right (desktop) / bottom-above-nav (mobile), auto-dismiss, used for autosave confirmation |
| **Bottom navigation (mobile)** | 5 icons: Dashboard, Planner, Notifications, Reports, More |
| **Role switcher** *(prototype-only utility)* | Not a production component — lets this clickable mockup demo all 5 roles from one session; production app derives role from SSO, never a UI toggle |
| **Filter chip** | Pill-shaped multi-select toggle, grouped by category (e.g. Region/Distributor/Area/SPV/Status); active state fills solid primary |
| **Filter drawer / bottom sheet** | Same filter content, two shells — right-side drawer on desktop/tablet, bottom sheet on mobile. Used by Route Planner's salesman filters and Reports' mobile filter panel |
| **Bottom sheet** | Mobile-only slide-up panel (rounded top corners, drag handle), used for filters and quick actions (e.g. "Move outlet to day") where a side drawer would feel cramped on a phone |
| **Accordion item** | Collapsed header (label + key value) expands to an editable body — used for Target Management's per-outlet allocation cards on mobile, replacing a dense table row |
| **Table ↔ card pair** | Every data table renders both a `<table>` (desktop/tablet) and a parallel card list (mobile) from the same row data, toggled by breakpoint — not a JS-driven re-render, so there's no resize flicker |
| **Sticky summary bar** | Desktop: pinned to the top of the scroll area (e.g. Target Management's allocation summary). Mobile: pinned above the bottom nav instead, condensed to the 1-2 numbers that matter most |

## 5. Responsive Breakpoints

| Breakpoint | Width | Behavior |
|---|---|---|
| Desktop | ≥ 1280px | Full sidebar + multi-column dashboard/planner |
| Tablet | 768–1279px | Sidebar collapses to icon rail; cards reflow to 2-column |
| Mobile | ≤ 767px | Sidebar replaced by bottom nav; single-column; dense tables collapse to cards/accordions; touch targets ≥ 44px; sticky search and summary/action bars replace desktop-only panels |

## 6. Interaction & Motion

- Transitions: 150–200ms ease for hover/active states, 220ms for panel slide-in, no motion longer than 300ms (per "fast and clean interaction" principle).
- All primary actions reachable in ≤ 3 clicks (PRD principle) — verified per-screen in [07-screen-specifications.md](07-screen-specifications.md).
- Drag-and-drop (Route Planner) shows a ghost card + drop-target highlight; invalid drops (e.g., over-capacity day) shake briefly + show inline reason rather than silently rejecting.

## 7. Related Documents

[01-PRD.md](01-PRD.md) · [07-screen-specifications.md](07-screen-specifications.md) · [`../prototype/`](../prototype/index.html)
