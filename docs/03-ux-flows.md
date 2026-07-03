# UX Flow Diagrams
## Skintific Territory & Execution Platform (STEP)

All diagrams are [Mermaid](https://mermaid.js.org/) — render directly in GitHub/most markdown viewers, or paste into the Mermaid Live Editor.

## 1. Route Planning Flow (SPV)

```mermaid
flowchart TD
    A[Open Route Planner] --> B[Select Salesman]
    B --> C[Select Week]
    C --> D{Start from?}
    D -->|Blank| E[Add Outlets manually or via Smart Search]
    D -->|Template| F[Apply Route Template]
    D -->|Copy Previous Week| G[Clone last week's route]
    E --> H[Review panel: density, capacity, coverage-gap indicators]
    F --> H
    G --> H
    H --> I{Check Recommendation Panel}
    I -->|Accept suggestion| J[Drag outlet into day slot]
    I -->|Ignore suggestion| K[Continue manual edit]
    J --> H
    K --> H
    H --> L[Save Draft]
    L --> M{Autosave + unsaved-change protection active}
    M --> N[Click Save / Submit]
    N --> O[Route persisted, compliance tracking begins]
```

**Key UX rules:**
- Every step before "Save" is recoverable — autosave to local draft persistence (survives tab close / offline) per PRD §7.2.
- The Recommendation Panel is always visible alongside the planner, never a blocking modal — assignment stays manual (PRD §9 FR-6).
- Leaving the planner with unsaved changes triggers a confirmation dialog (Unsaved Change Protection).

## 2. Target Allocation + Approval Flow (SPV → AM → DM)

```mermaid
flowchart TD
    A[Head Office sets Area Target] --> B[SPV opens Target Management]
    B --> C[Distribute target across outlets]
    C --> D{Sum of outlet allocations = Area Target?}
    D -->|No| E[Inline warning + remaining-allocation indicator]
    E --> C
    D -->|Yes| F[Run What-if Simulator optional]
    F --> G[Save Draft]
    G --> H[Submit for Approval]
    H --> I[Area Manager reviews — Approval Inbox]
    I -->|Reject| J[Returns to SPV with comment]
    J --> C
    I -->|Approve| K[Distributor Manager reviews]
    K -->|Reject| J
    K -->|Approve| L[New target VERSION created, effective-dated]
    L --> M[Historical achievement frozen at version boundary]
    M --> N[Remaining target recalculated forward]
    N --> O[Audit log entry written]
```

**Key UX rules:**
- The submit button is disabled (not hidden) while D is unsatisfied — show the exact remaining/over amount, never just "invalid."
- Rejection always bounces to the original submitter (SPV) with the rejecting role's comment attached — never a dead end. This mirrors the validated PO Portal bounce-back pattern (Finance/Logistics rejection → returns to SA, not lost).
- Step L never overwrites; see [04-database-erd.md](04-database-erd.md#target-versioning) for the versioning model.

## 3. Tier Override Flow (SPV → AM → DM)

```mermaid
flowchart TD
    A[Outlet 360: current tier shown, e.g. Tier B] --> B[SPV clicks Override Tier]
    B --> C[Select new tier + reason]
    C --> D[Submit for Approval]
    D --> E[Area Manager approves/rejects]
    E -->|Reject| F[Back to SPV with comment]
    E -->|Approve| G[Distributor Manager approves/rejects]
    G -->|Reject| F
    G -->|Approve| H[Tier changed, audit log entry + tier history record written]
```

Same 3-step chain and bounce-back rule as Target Adjustment — see [08-approval-workflow.md](08-approval-workflow.md) for the formal state machine shared by all three approval types (Target Adjustment, Tier Override, Request Reopen).

## 4. Import / Export (Bulk Upload) Flow

```mermaid
flowchart TD
    A[Open Import & Export Center] --> B[Download Template for entity type]
    B --> C[Fill template offline]
    C --> D[Upload file]
    D --> E[Background validation job starts — progress indicator shown]
    E --> F{Validation result}
    F -->|Errors found| G[Preview grid with inline error highlighting]
    G --> H[Download Error File]
    H --> C
    F -->|Clean| I[Preview: affected outlets / salesmen / targets / routes]
    I --> J[Confirm]
    J --> K[Commit — background job + progress indicator]
    K --> L[Completion summary + link to affected records]
```

**Key UX rule:** nothing commits without the explicit Preview → Confirm step (FR-4). Large files never block the UI — upload and validation both run as background jobs with a visible progress indicator, consistent with the platform's 2s/3s interactive budgets (heavy work is explicitly exempted into the background, not into a frozen spinner).

## 5. Approval Inbox — Reviewer Flow (AM / DM)

```mermaid
flowchart TD
    A[Notification: new item pending your approval] --> B[Open Approvals Inbox]
    B --> C[List grouped by type: Target / Tier / Reopen]
    C --> D[Open item — detail panel slides in]
    D --> E[Review: requester, reason, before/after, SLA countdown, timeline]
    E --> F{Decision}
    F -->|Approve| G[Optional comment] --> H[Submit — under 1s]
    F -->|Reject| I[Required comment] --> H
    H --> J[Item leaves inbox, moves to History tab]
    J --> K[Requester notified, deep-linked back to the record]
```

**Key UX rule:** rejection requires a comment (so the bounce-back in Flow 2/3 always carries actionable feedback); approval comment is optional. SLA indicator changes color (on-track → at-risk → breached) rather than just showing a number, so non-tech-savvy reviewers triage at a glance.

## 6. Onboarding Flow (first login, any role)

```mermaid
flowchart TD
    A[First login via SSO] --> B[Role + territory auto-detected from SSO claims]
    B --> C[Interactive Tour offer: Take the tour / Skip]
    C -->|Take tour| D[3-5 step spotlight tour of role-specific Dashboard + primary module]
    C -->|Skip| E[Land on Dashboard]
    D --> E
    E --> F[Empty-state cards guide first action: e.g. Create Route, Apply Template]
    F --> G[Help Center + Release Notes always reachable via ? icon]
```

## 7. Related Documents

[01-PRD.md](01-PRD.md) · [08-approval-workflow.md](08-approval-workflow.md) · [07-screen-specifications.md](07-screen-specifications.md)
