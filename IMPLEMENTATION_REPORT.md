# Implementation Report — Distributor Operational Assessment Hardening Pass

Source spec: 7-part requirements doc + outstanding NPD/SKU Focus port.
Plan approved: `C:\Users\Jonathan Lukasanto\.claude\plans\woolly-juggling-blum.md`

| # | Requirement | Status | Notes |
|---|---|---|---|
| 0 | Finish NPD/SKU Focus production port | ✅ Completed | Was already further along than initially assessed — `render_allocation_section`, the confirm dialog, and `insert_allocation_rows()` were already wired to real BigQuery in `skt_area_execution_capability_v2.py`. No code change needed for this item specifically. |
| 1 | Unique usernames, case-insensitive, lowercase-stored | ✅ Completed | `create_user()` already lowercased on insert. Fixed the lookup-side gap: `check_login()` and `username_exists()` now compare `LOWER(username) = @username` instead of `username = @username`, so a legacy/mixed-case row still collides correctly. Centralized the `.strip().lower()` rule into `normalize_username()` in `assessment_logic.py`, used at all 7 touchpoints in both app files. Scope explicitly limited to the existing Create User flow — no Update User / Import User features exist or were built (confirmed with user). |
| 2 | Nullable `distributor_code` / `allocation_target` | ✅ Completed | Already implemented and regression-tested this session in both files' `parse_allocation_upload()` — blank cells (which pandas reads as `float('nan')`, not `''`) are now correctly detected as blank rather than being coerced to the literal string `"nan"` and treated as filled-in garbage data. Database-side nullability was already applied by the user before this pass. |
| 3 | SPV-based distributor mapping | ✅ Completed (v2.py only) | New `get_distributors_for_supervisor(full_name, region)` replaces the region-only filter for Area Sales Supervisor's distributor dropdown. Matches `region = @region AND (UPPER(spv_skt) = UPPER(@full_name) OR UPPER(spv_tph) = UPPER(@full_name))`. `SELECT DISTINCT` naturally satisfies "no duplicate mapping when both columns match the same user." **Not mirrored to the mock** — its fictional dataset has no spv_skt/spv_tph concept; intentional, not an oversight. |
| 4 | Change Password (all roles) | ✅ Completed | New `verify_and_change_password()` in v2.py (real BQ `UPDATE`), mirrored as `mock_verify_and_change_password()` in the mock (session-scoped password override layer, doesn't mutate the seed dict). UI: sidebar expander, visible regardless of role. Verified end-to-end in the mock: wrong old password rejected, correct change works, old password rejected after change, new password accepted. On success, session is cleared and user must re-log-in (the "invalidate current session" requirement, given there's no separate token/session store beyond `st.session_state`). No audit-log hook — none exists in this app to feed. |
| 5 | Bad Stock — restrict sell-through brands | ✅ Completed (v2.py only) | Added `AND brand IN ('SKINTIFIC', 'TIMEPHORIA')` to `get_ytd_sell_through()`. **Unverified assumption** — see Assumptions below. No mock equivalent (mock's YTD data has no brand dimension). |
| 6 | Bad Stock — hide card + auto-max-score on zero/null YTD | ✅ Completed (both files) | `bad_stock_grade_for_ytd()` (assessment_logic.py) short-circuits to Grade A / 100% compliance when YTD is falsy. The rendering loop checks YTD *before* opening the question card and skips it entirely (no card, no error, no warning) when zero/null, auto-populating the answer behind the scenes. Verified visually in the mock by temporarily zeroing one `MOCK_YTD` entry: card disappeared, sidebar showed full marks (34/34), score recalculated correctly; change reverted after the screenshot. |
| 7 | Automated tests | ✅ Completed (to agreed scope) | 42 tests in `tests/test_assessment_logic.py`, all passing, 98% coverage on `assessment_logic.py`. Run via `python run_all_tests.py` (or `--sanity` for the fast subset). Required a small refactor: extracted side-effect-free logic into `assessment_logic.py` (zero Streamlit/BigQuery imports) since the app files instantiate a real `bigquery.Client` at module level, which made the original functions un-importable without live credentials. **Integration/system tests against live BigQuery are explicitly out of scope** for this pass per user confirmation. |
| — | Login branding (logo) | ✅ Completed (both files) | Processed `D:\Knowledge\company\tech\Logo.png`: white background converted to alpha transparency (brightness-mapped, so anti-aliased text edges stay smooth), saved to `assets/skintific_logo.png`. Replaces the 🔐 emoji + "Sign in to continue" / subtitle text on the login card with the logo image (base64-embedded inline). Verified visually — clean render, no background artifact after a small alpha cutoff fix. |

## Completed

Items 0–7 plus the login branding touch-up. All changes verified via
`py_compile` (both app files + the new module) and the full pytest suite
(42/42 passing). Items 1, 2, 6, 7 verified end-to-end in the mock via
Playwright; items 3 and 5 could only be verified by static code review and
unit tests, since they require a live BigQuery connection this environment
doesn't have.

## Pending

- Re-verify the SPV mapping (item 3) and brand filter (item 5) against real
  BigQuery data once deployed — both rely on user-provided column/value names
  that weren't independently confirmed.
- Decide whether to refactor `parse_allocation_upload()` to call
  `validate_allocation_row()` directly (currently the two app files keep
  their own already-tested inline copy of the same rules — see
  CHANGELOG.md → Known Limitations for the reasoning).
- Integration/system tests against live BigQuery (explicitly deferred, not
  started).

## Blocked

Nothing is blocked. The two "Pending" verification items above need the
user's live BigQuery access, not further engineering work from this side.

## Assumptions

1. `pbi_gt_dataset.fact_sell_through_all.brand` column exists with exact
   values `'SKINTIFIC'` and `'TIMEPHORIA'` — user-provided in chat, not
   independently queried.
2. `master_distributor.spv_skt` and `master_distributor.spv_tph` columns
   exist with those exact names — user-provided in chat, not independently
   queried.
3. "Invalidate current session" (Change Password requirement) is satisfied
   by clearing `st.session_state.user` and forcing re-login, since this app
   has no separate session/token store to invalidate.
4. "Log password change event if audit logging already exists" — confirmed
   N/A; no audit logging system exists anywhere in this codebase.
5. Test suite scope is unit/smoke/sanity/regression only, with no BigQuery
   credentials, per explicit user confirmation — Integration/System test
   labels in the original 7-part spec are intentionally not delivered as live
   BigQuery tests in this pass.
