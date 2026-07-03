# API Recommendation
## Skintific Territory & Execution Platform (STEP)

## 1. Style & Conventions

- **REST over HTTPS**, JSON bodies, resource-oriented URLs (`/api/v1/...`).
- Versioned from day one (`/api/v1/`) since SFA integration and mobile clients will depend on contract stability.
- Pagination: cursor-based (`?cursor=...&limit=...`) for any list endpoint that can exceed ~200 rows (routes, outlets, audit log).
- Idempotency keys (`Idempotency-Key` header) required on all POST endpoints that create approval requests or commit imports — bulk upload and flaky mobile networks make retries common.
- Errors follow a single envelope: `{ "error": { "code", "message", "field_errors": [...] } }` so the frontend can render inline validation consistently across every form (Target Allocation, Tier Override, Bulk Upload preview, etc.).

## 2. Authentication & Security

| Concern | Recommendation |
|---|---|
| Identity | SSO via OIDC (Azure AD / Google Workspace — see PRD open question #1). API issues short-lived JWT access tokens + refresh tokens after IdP handoff. |
| Session | Configurable idle timeout (NFR §9.2), refresh token revocation list for forced logout (e.g. offboarded user). |
| RBAC | Enforced in an API middleware layer reading `role` + `territory_scope` claims — **every** endpoint re-checks scope server-side; the IA's RBAC matrix ([02-information-architecture.md](02-information-architecture.md#3-rbac--navigation-matrix)) is the source of truth for what each role/endpoint combination allows. |
| Transport | HTTPS-only (HSTS), TLS 1.2+. |
| At-rest encryption | Cloud SQL + BigQuery default encryption; service-account-scoped access, least privilege (mirrors the existing read-only BQ service account pattern already in use). |
| IP allowlisting | Reverse-proxy/load-balancer level, optional per-environment (ready for Head Office IT to enable, not mandatory for Phase 1 pilot). |
| Audit | Every mutating endpoint writes to `AUDIT_LOG` (actor, before/after, reason) as part of the same transaction — not a best-effort side effect. |

## 3. Resource Endpoints by Module

```
# Auth
POST   /api/v1/auth/sso/callback
POST   /api/v1/auth/refresh
POST   /api/v1/auth/logout

# Dashboard
GET    /api/v1/dashboard?role={role}&territory_id={id}          # composed widget payload
GET    /api/v1/dashboard/data-freshness

# Territory & RBAC
GET    /api/v1/territories
GET    /api/v1/me                                                # role + scoped territories

# Route Planner
GET    /api/v1/routes?salesman_id&week_start
POST   /api/v1/routes                                            # create/save draft
PATCH  /api/v1/routes/{id}
POST   /api/v1/routes/{id}/items                                 # add outlet to route
DELETE /api/v1/routes/{id}/items/{item_id}
POST   /api/v1/routes/{id}/submit
GET    /api/v1/route-templates
POST   /api/v1/route-templates
POST   /api/v1/routes/copy-previous-week
GET    /api/v1/recommendations?salesman_id&week_start             # Critical/Recommended/Optional + reasoning

# Outlet & Salesman
GET    /api/v1/outlets?territory_id&search=
GET    /api/v1/outlets/{id}                                       # Outlet 360 payload
PATCH  /api/v1/outlets/{id}/tier                                   # creates an APPROVAL_REQUEST, not a direct write
GET    /api/v1/salesmen/{id}                                       # Salesman 360 payload
GET    /api/v1/outlet-salesman-map

# Target Management
GET    /api/v1/area-targets?territory_id&period
POST   /api/v1/area-targets                                        # Head Office sets top-down target
GET    /api/v1/outlet-targets?area_target_id
POST   /api/v1/outlet-targets/allocate                             # bulk allocation, validated sum = area target
POST   /api/v1/outlet-targets/{id}/versions                        # creates a TARGET_VERSION via approval flow
GET    /api/v1/outlet-targets/{id}/versions                        # full version history

# Approvals
GET    /api/v1/approvals?status=pending&type=
GET    /api/v1/approvals/{id}
POST   /api/v1/approvals/{id}/decision                             # { decision: approve|reject, comment }

# Reports
GET    /api/v1/reports/{report_code}?filters=...                   # proxies to BigQuery step_schema
POST   /api/v1/reports/{report_code}/export                        # async job -> Excel/CSV/PDF

# Notifications & Announcements
GET    /api/v1/notifications?unread=true
POST   /api/v1/notifications/{id}/read
POST   /api/v1/notifications/read-all
GET    /api/v1/announcements
POST   /api/v1/announcements                                       # Admin only

# Import & Export
POST   /api/v1/imports/{entity}/upload                             # returns job_id
GET    /api/v1/imports/jobs/{job_id}                                # status + preview + error file link
POST   /api/v1/imports/jobs/{job_id}/commit
GET    /api/v1/exports/templates/{entity}

# Exception & Data Quality
GET    /api/v1/exceptions?status=open
POST   /api/v1/exceptions/{id}/assign
POST   /api/v1/exceptions/{id}/dismiss
GET    /api/v1/data-quality/summary

# Administration
GET/POST/PATCH  /api/v1/admin/users
GET/POST/PATCH  /api/v1/admin/roles
GET/POST/PATCH  /api/v1/admin/master-data/{entity}
GET/PATCH       /api/v1/admin/recommendation-rules
GET/PATCH       /api/v1/admin/approval-matrix
GET             /api/v1/admin/audit-logs?entity_type&date_range
GET             /api/v1/admin/sfa-sync-status
GET             /api/v1/admin/system-health
GET/PATCH       /api/v1/admin/feature-flags
POST            /api/v1/admin/impersonate/{user_id}                 # read-only session, logged
```

## 4. SFA Integration Endpoints (consumed by `SFA-Handheldv2` / produced for STEP)

```
POST   /api/v1/sfa/sync/visits          # SFA pushes completed visit checkpoints
POST   /api/v1/sfa/sync/orders          # SFA pushes order/sell-in events
GET    /api/v1/sfa/sync/status          # STEP exposes health for the Integration Monitor widget
POST   /api/v1/sfa/sync/retry/{batch_id}
```

Full sync architecture, retry-queue design, and status taxonomy: [09-sfa-integration-architecture.md](09-sfa-integration-architecture.md).

## 5. Performance Considerations Mapped to NFRs

| NFR budget | API implication |
|---|---|
| Dashboard ≤ 3s | `/dashboard` endpoint is a single composed/cached payload (server-side widget aggregation), not N client-side calls per widget |
| Search ≤ 2s | Outlet/salesman/route search backed by a dedicated search index (e.g. Postgres trigram/GIN or an external search service) rather than ad hoc `LIKE` queries |
| Approval ≤ 1s | `/approvals/{id}/decision` is a single transactional write (decision + audit log + notification fan-out queued async, not inline) |
| Save Route ≤ 2s | Route save batches `ROUTE_ITEM` upserts in one transaction; autosave debounced client-side to avoid hammering the endpoint |
| Bulk upload | Always `202 Accepted` + `job_id`, polled or pushed via websocket/SSE for progress — never a synchronous request held open for a large file |

## 6. Related Documents

[01-PRD.md](01-PRD.md) · [04-database-erd.md](04-database-erd.md) · [09-sfa-integration-architecture.md](09-sfa-integration-architecture.md)
