# Distributor Operational Assessment — User Guide

**Role covered in this guide: Area Sales Supervisor**
(Other roles — Distributor Manager, Admin RSA, Account Receivable — have a
different bulk-upload-based interface and are documented separately.)

Generated from an automated screenshot walkthrough of the application
(`docs_crawler.py`, Playwright) logged in as the `budi` / Area Sales Supervisor
account. Screenshots are stored in `/screenshots`.

🇮🇩 *Bahasa Indonesia version: [User_Guide_ID.html](User_Guide_ID.html) /
[User_Guide_ID.md](User_Guide_ID.md)*

---

## Table of Contents

1. [Login](#1-login)
2. [Post-Login Dashboard (Form Locked)](#2-post-login-dashboard-form-locked)
3. [Assessment Period Filter](#3-assessment-period-filter)
4. [Distributor Filter](#4-distributor-filter)
5. [Assessment Form — People & Roles](#5-assessment-form--people--roles)
6. [Assessment Form — Infrastructure & Delivery / Operations & Compliance](#6-assessment-form--infrastructure--delivery--operations--compliance)
7. [Live Score Tracker (Sidebar)](#7-live-score-tracker-sidebar)
8. [Review & Submit](#8-review--submit)
9. [Submission Result / Duplicate-Submission Notice](#9-submission-result--duplicate-submission-notice)
10. [Change Password](#10-change-password)
11. [FAQ](#faq)
12. [Troubleshooting](#troubleshooting)

---

## 1. Login

![Login screen](screenshots/01_login_screen.png)
![Login filled in](screenshots/02_login_filled.png)

**Purpose:** Authenticate and automatically route the user to the correct
role's interface — there is no manual role picker; your account determines
what you see.

**Fields:**
- **Username** — your assigned login name (not case-sensitive)
- **Password** — your account password

**Buttons:**
- **🔑 Log In** — submits the credentials. On success, the page reloads into
  your role's dashboard. On failure, shows "❌ Invalid username or password."
  below the form without revealing which field was wrong.

**Validation rules:**
- Username comparison is case-insensitive (`Budi`, `budi`, `BUDI` all match
  the same account).
- No lockout/retry-limit is enforced — there is no rate limiting on login
  attempts.

**Common user actions:** Type username and password, click Log In. If you
forget your password, see [Section 10 — Change Password](#10-change-password)
(requires knowing your current password) or contact an Admin to have it reset
via the Distributor Manager's Create User panel.

---

## 2. Post-Login Dashboard (Form Locked)

![Post-login, locked](screenshots/03_post_login_locked.png)

**Purpose:** Confirms who you're logged in as and what role/region you're
scoped to, before unlocking the assessment form.

**What's shown:**
- **Sidebar**: your name, role pill, region pill, Logout button, Change
  Password panel
- **"Logged In As" card**: your name, role description, and region scope
- **"Assessment Period" card**: month/year selector
- **"Location" card**: Region (locked to your assigned region) + Distributor
  (must be selected)

**Validation rules:**
- The assessment form below stays hidden until a Distributor is selected.
  Region cannot be changed — Area Sales Supervisor accounts are scoped to
  exactly one region.

**Common user actions:** Select the Assessment Period, then select a
Distributor to unlock the form.

---

## 3. Assessment Period Filter

![Assessment period dropdown](screenshots/04_assessment_period_dropdown.png)

**Purpose:** Choose which month the assessment applies to.

**Filter behavior:**
- Area Sales Supervisor sees only **3 months**: the current month and the 2
  previous months. (Other roles — the bulk/admin roles — see a wider
  13-month window: 6 months back to 6 months forward.) This is intentional:
  field assessments are expected to be entered promptly, not months in
  advance or long after the fact.
- Defaults to the current month on load.

**Common user actions:** Open the dropdown, pick a month. Changing this after
you've already started filling in the form does not clear your answers, but
changing it after a submission for one period will let you start a new
submission for a different period (one submission is allowed per
role+distributor+period — see Section 8).

---

## 4. Distributor Filter

![Distributor dropdown](screenshots/05_distributor_dropdown.png)

**Purpose:** Select which distributor you're assessing.

**Filter behavior:**
- The list only shows distributors assigned to **you specifically** (matched
  on the master distributor list by region and your name as the responsible
  supervisor) — not every distributor in your region.
- If the list is empty, no distributors are currently mapped to you; contact
  an Admin.

**Validation rules:** Both Assessment Period and Distributor must be set
before the form below appears — a banner shows "✏️ Select a distributor above
to unlock the assessment form" until then.

---

## 5. Assessment Form — People & Roles

![Form unlocked — People & Roles](screenshots/06_form_unlocked_people_roles.png)
![People & Roles filled in](screenshots/07_people_roles_filled.png)

**Purpose:** The first of 3 metric categories. Captures whether the
distributor has dedicated, exclusive-to-SKINTIFIC staff in 3 roles.

**Cards in this category:**
| # | Metric | Max Points | Grades |
|---|---|---|---|
| 1 | Operational Leader (SPV/Operational Manager) | 5 | A=Exclusive(5), B=Mixed(3), C=Don't exist(0) |
| 2 | Salesman | 7 | A=Exclusive(7), B=Mixed(5), C=Under quota(3), D=Don't exist(0) |
| 3 | Administrative & AR Support | 3 | A=Exclusive(3), B=Mixed(1), C=Don't exist(0) |

**Buttons/inputs:**
- Radio buttons (A/B/C/D) select the grade for each metric
- Name field(s) — required when the selected grade implies the role exists;
  Salesman requires **exactly 5 names** (use `-` for unfilled slots if fewer
  than 5 salesmen)

**Validation rules:**
- If grade = "Do not exist", any entered name is rejected ("selected 'Do not
  exist' but a name was entered")
- If grade ≠ "Do not exist", a name is required
- Salesman specifically requires exactly 5 name entries (real names or `-`
  placeholders), never more or fewer

**Calculations shown:** None at this stage — points are tallied live in the
sidebar (see Section 7).

---

## 6. Assessment Form — Infrastructure & Delivery / Operations & Compliance

![Infrastructure & Delivery](screenshots/08_infrastructure_delivery.png)
![Operations & Compliance](screenshots/09_operations_compliance.png)
![Bad Stock detail](screenshots/10_bad_stock_detail.png)

**Purpose:** The remaining 4 metrics owned by Area Sales Supervisor.

| # | Metric | Max Points | Notes |
|---|---|---|---|
| 4 | Warehouse Facility Standard | 3 | Simple grade selection |
| 5 | Delivery SLA Compliance | 8 | **Calculated**, see below |
| 6 | Inventory Control & Stock Opname | 6 | Simple grade selection |
| 9 | Bad Stock Handling Performance | 2 | **Calculated**, see below |

### Delivery SLA Compliance — calculation
Two separate radio inputs (Inner City 2×24h, Outer City 3×24h), each
100% / 99%-80% / <80%. The grade is **derived automatically**, not chosen
directly:
- Either Inner or Outer below 80% → **0 pts**
- Either at 99%-80% (none below 80%) → **4 pts**
- Both at 100% → **8 pts**

### Bad Stock Handling Performance — calculation
1. **YTD Sell Through** is looked up automatically for the selected
   distributor and year (Skintific + Timephoria brands only).
2. **Bad Stock Allowance** = 0.5% of YTD Sell Through.
3. You enter **Bad Stock Utilization (Rp)** — the actual Rupiah value of bad
   stock claimed this period.
4. **Compliance % = Utilization ÷ Allowance × 100**, capped at 100%.
5. Grade: ≥100% → A (2 pts), ≥80% → B (1 pt), <80% → C (0 pts).

**Validation rule (important):** If the distributor has **zero or no YTD
sell-through data**, this entire card is **hidden** and the metric
auto-scores the maximum (2 pts) — there's nothing to assess compliance
against, so no input is shown and no error appears.

**Common user actions:** Select grades for Warehouse and Inventory directly;
for Delivery SLA, pick the inner/outer band; for Bad Stock, type the Rupiah
utilization amount and watch the compliance % and grade update live.

---

## 7. Live Score Tracker (Sidebar)

![Sidebar score tracker](screenshots/11_sidebar_score_tracker.png)

**Purpose:** Real-time feedback on your score as you fill in the form, plus
visibility into the other 3 roles' progress on the same distributor+period.

**What's shown:**
- **"Your Live Score"** — running total out of 34 (the Area Sales
  Supervisor's 7-metric maximum), updates on every answer change, with a
  progress bar
- Per-metric breakdown with a colored dot: 🟢 full marks, 🟡 partial, 🔴 zero
- **"Assessment Progress"** — all 10 metrics across all 4 roles for this
  distributor+period: ✅ done (with points + which role submitted) or ⏳
  pending (with which role owns it)
- **"Combined Score So Far"** — out of 50, across whichever roles have
  already submitted

**Calculations shown:** This is a live mirror of the same scoring rules
described in Sections 5–6 — nothing new is calculated here, it's a running
total of the answers above.

---

## 8. Review & Submit

![Review & Submit confirmation popup](screenshots/12_review_submit_popup.png)

**Purpose:** A mandatory double-check step before anything is saved.

**Buttons:**
- **🔍 Review & Submit (Area Sales Supervisor)** — validates the form (see
  Section 5's validation rules) and, if valid, opens the confirmation popup
  shown above. Disabled if you've already submitted for this
  distributor+period.
- Inside the popup: **✏️ Back to Edit** (closes the popup, no changes saved)
  or **✅ Confirm & Submit** (writes the submission)

**What the popup shows:** Every metric, its grade, point value, and any
person name, plus your subtotal out of 34.

**Validation rules:**
- All Section 5 field-level rules are checked first; errors are listed and
  the popup does not open until they're fixed.
- **One submission per role, per distributor, per assessment period** — once
  confirmed, you cannot submit again for this exact combination. Attempting
  to revisit shows a warning banner and disables the Review & Submit button.

---

## 9. Submission Result / Duplicate-Submission Notice

![Submission result / already-submitted warning](screenshots/13_submission_result.png)

**Purpose:** Confirms the submission was saved, and — if all 4 roles have
since submitted their part — shows the final combined score and rating
(Excellent/Good/Fair/Needs Improvement) for the distributor+period.

**If you return to an already-submitted distributor+period:** a yellow
warning banner reads *"[Role] has already submitted for [Distributor] in
[Period]. Only one submission is allowed per role, per distributor, per
assessment period."* — this is the same one-submission rule from Section 8,
shown again on revisit.

---

## 10. Change Password

![Change Password panel](screenshots/14_change_password.png)

**Purpose:** Self-service password change, available to every role (not just
Area Sales Supervisor).

**Fields:** Old Password, New Password, Confirm New Password (all masked,
with a show/hide eye icon).

**Buttons:** **Update Password** — validates and applies the change.

**Validation rules:**
- All 3 fields are required.
- Old Password must match your current password exactly.
- New Password and Confirm New Password must match each other.
- No complexity requirements (length, special characters, etc.) are
  enforced.
- On success, you are **immediately logged out** and must log back in with
  the new password — this is intentional, not a bug.

---

## FAQ

**Q: I can't find a distributor I expect to see.**
A: Your distributor list is filtered to only the distributors assigned to
you. If one is missing, contact an Admin to check the mapping.

**Q: The Bad Stock card disappeared — is that a bug?**
A: No. It only disappears when the distributor has zero recorded YTD
sell-through (Skintific + Timephoria) for the selected year — there's
nothing to calculate compliance against, so the metric auto-awards full
marks and hides the input.

**Q: I made a mistake after confirming — can I fix it?**
A: No. Submissions are final per role/distributor/period. Contact an Admin
if a correction is genuinely needed.

**Q: Why does the sidebar show other roles' progress?**
A: All 4 roles' submissions roll up into one combined assessment per
distributor+period — the sidebar lets you see the whole picture, not just
your own part.

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| "Invalid username or password" | Typo, or wrong account | Re-check credentials; case doesn't matter for username but does for password |
| Distributor dropdown is empty | No distributors mapped to your account | Contact an Admin |
| Review & Submit button is disabled/greyed out | Already submitted for this distributor+period | Pick a different distributor or period, or contact an Admin for a correction |
| Bad Stock card missing | Zero YTD sell-through for that distributor/year | Expected behavior, not an error — see FAQ |
| Logged out unexpectedly | You just changed your password | Log back in with the new password |
