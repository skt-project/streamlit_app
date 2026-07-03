# Phase Roadmap & Future Enhancement Backlog
## Skintific Territory & Execution Platform (STEP)

## Phase 1 — MVP (Production Ready)

Goal: replace spreadsheet/offline planning with a governed, auditable system for one pilot region before national rollout (see PRD open question #5).

- Authentication & RBAC (SSO-ready)
- Dashboard (role-specific)
- Route Planner (Calendar + Kanban, manual drag-and-drop, templates, copy-previous-week)
- Recommendation Engine (rule-based, configurable weights)
- Outlet & Salesman 360
- Target Management (allocation + versioning + approval-gated changes)
- Approval Workflow (Target Adjustment, Tier Override, Request Reopen)
- Notifications (in-app + email)
- Reports (core set: Sell-In YTD, Route Compliance, Coverage, Achievement, Leaderboards)
- Announcement Center
- Import & Export Center + Bulk Upload + Download Template
- Audit Timeline
- Data Freshness indicator
- SFA Integration Monitor
- Mobile Responsive (Dashboard, Approvals, Notifications, light editing)

**Exit criteria:** pilot region SPVs plan a full week without leaving STEP; AM/DM approve without email/chat fallback; route compliance numbers in STEP match field reality within an agreed tolerance.

## Phase 2

- Heatmap visualization (territory coverage density)
- Coverage Gap Analytics (trend, not just point-in-time count)
- Route Redistribution Suggestions (engine recommends rebalancing across salesmen, still manual commit)
- Advanced Exception Center (auto-categorization, bulk resolve)
- Dashboard Personalization (custom widget layouts beyond pin/favorite)

## Phase 3

- Power BI Embed (only if in-app Reports can't keep pace with data volume/complexity — see PRD §4 Non-Goals)
- Advanced Scenario Planning (multi-scenario what-if comparison, not just single simulation)
- Feature Flags (formalized progressive rollout tooling, beyond the Phase 1 environment banner)
- Advanced Analytics (predictive churn/attrition risk per outlet, salesman performance forecasting)

## Future Enhancement Backlog (unscheduled — candidates for Phase 3+)

- Native push notifications via a dedicated mobile shell (if responsive web push proves insufficient on iOS Safari)
- Voice-to-note capture for SPV field comments (Outlet 360 Notes)
- AI-assisted route note summarization (pattern reuse from the SFA-Handheldv2 Gemini API integration already in production)
- Distributor-facing read-only view of their own targets/compliance (would require a new role + careful scoping beyond the 5 defined roles)
- Multi-scenario target simulation shared/commented collaboratively (Notion-style comments-on-cells)
- Automated SLA escalation routing to a configurable secondary approver, not just notification-only escalation
- Read replica / caching layer for Reports if BigQuery query latency becomes a bottleneck under concurrent load

## Sequencing Notes

- Recommendation Engine and Approval Workflow are both Phase 1 because Target Management and Route Planner are unusable at enterprise scale without them (no governance, no triage) — they were not deferred to Phase 2 despite being the most complex pieces.
- Phase 2/3 items are explicitly the ones the PRD's Non-Goals section (§4) called out as deferred — nothing here contradicts that section.

## Related Documents

[01-PRD.md](01-PRD.md) · [07-screen-specifications.md](07-screen-specifications.md)
