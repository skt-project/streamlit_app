# Skintific Territory & Execution Platform (STEP)
**Plan. Execute. Monitor.**

Internal enterprise web application for territory planning, route execution, target governance, and sales performance monitoring — upstream of the SFA handheld apps. This repo contains the full design/architecture deliverable set plus a clickable high-fidelity prototype.

## Contents

### Documentation (`docs/`)

| Doc | Covers |
|---|---|
| [01-PRD.md](docs/01-PRD.md) | Product Requirement Document — vision, personas, scope, functional + non-functional requirements |
| [02-information-architecture.md](docs/02-information-architecture.md) | Sitemap, RBAC × navigation matrix, territory scoping model |
| [03-ux-flows.md](docs/03-ux-flows.md) | User flow diagrams — route planning, target approval, tier override, import/export, onboarding |
| [04-database-erd.md](docs/04-database-erd.md) | Entity diagram + database recommendation (Postgres OLTP + BigQuery analytics) |
| [05-api-recommendation.md](docs/05-api-recommendation.md) | REST API design, security, SFA integration endpoints |
| [06-design-system.md](docs/06-design-system.md) | Color/type/spacing tokens, component library, responsive breakpoints |
| [07-screen-specifications.md](docs/07-screen-specifications.md) | Dashboard spec, Route Planner spec, Recommendation Engine design |
| [08-approval-workflow.md](docs/08-approval-workflow.md) | Approval state machine, SLA model, configurable approval matrix |
| [09-sfa-integration-architecture.md](docs/09-sfa-integration-architecture.md) | Sync architecture, retry queue, status taxonomy, contract boundary with SFA-Handheldv2 |
| [10-roadmap-and-backlog.md](docs/10-roadmap-and-backlog.md) | Phase 1/2/3 roadmap + future enhancement backlog |

### Clickable Prototype (`prototype/`)

Static HTML/CSS/JS, no build step — open `prototype/index.html` directly or serve locally:

```
cd prototype
python -m http.server 8765
# open http://localhost:8765
```

Use the **role switcher** in the top bar to demo the same data from all 5 roles (SPV, Area Manager, Distributor Manager, Regional Sales, Head Office Admin) without logging in separately. This switcher is a prototype-only convenience — the production app derives role from SSO (see [05-api-recommendation.md](docs/05-api-recommendation.md#2-authentication--security)).

| Page | Module |
|---|---|
| `index.html` | Login |
| `dashboard.html` | Dashboard (role-aware) |
| `route-planner.html` | Route Planner + Recommendation Engine |
| `outlet-salesman.html` | Outlet & Salesman list |
| `outlet-360.html` | Outlet 360 |
| `salesman-360.html` | Salesman 360 |
| `target-management.html` | Target Management |
| `approvals.html` | Approvals Inbox |
| `reports.html` | Reports |
| `notifications.html` | Notification Center |
| `announcements.html` | Announcement Center |
| `import-export.html` | Import & Export Center |
| `administration.html` | Administration (Head Office Admin) |

## Status

Design/architecture phase — clickable prototype + full doc suite for stakeholder review, scoped to Phase 1 MVP (see [10-roadmap-and-backlog.md](docs/10-roadmap-and-backlog.md)). Not yet implemented as a production application.
