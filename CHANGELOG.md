# Changelog — Distributor Operational Assessment (Multi-Role)

All changes in this entry apply to `skt_area_execution_capability_v2.py`
(production) and `skt_area_execution_capability_mock.py` (local/no-BQ demo),
unless noted otherwise. The original single-role `skt_area_execution_capability.py`
is untouched throughout.

## Feature

Hardening pass covering 7 requirements raised from real production usage,
plus finishing one previously-incomplete feature port:

1. NPD & SKU Focus allocation bulk-upload — completed the production port
   (was previously only mock-tested; now writes to BigQuery via
   `insert_allocation_rows()`).
2. Unique usernames — case-insensitive comparison (`LOWER(username)`) on every
   lookup, not just on insert.
3. Nullable `distributor_code` / `allocation_target` on NPD/SKU Focus uploads
   — both optional, blank values stored as `NULL` instead of rejecting the row.
4. SPV-based distributor mapping — Area Sales Supervisor's distributor list
   is now filtered by `master_distributor.spv_skt` / `spv_tph` matching the
   logged-in user's `full_name`, not just by region.
5. Change Password — available to every role, in the sidebar next to Logout.
6. Bad Stock allowance — sell-through sum restricted to Skintific + Timephoria
   brands only.
7. Bad Stock zero-YTD handling — when a distributor has no YTD sell-through,
   the Bad Stock card is hidden entirely and the metric auto-scores max (2pts)
   instead of erroring or scoring 0.
8. Automated test suite — `tests/test_assessment_logic.py` (42 tests, pytest),
   runnable via `python run_all_tests.py`, no BigQuery credentials required.
9. Branding — login screen now shows the SKINTIFIC wordmark (background made
   transparent from the supplied logo) instead of a lock emoji + title text.

## Files Changed

| File | Change |
|---|---|
| `skt_area_execution_capability_v2.py` | Items 1–7, 9 above; imports shared logic from `assessment_logic.py` |
| `skt_area_execution_capability_mock.py` | Same items mirrored where applicable (3, 5, 6, 7, 9; item 4 intentionally not mirrored — see Known Limitations) |
| `assessment_logic.py` | **New.** Pure, dependency-free logic extracted for testability: `normalize_username`, `value_to_grade`, `get_sla_grade`, `bad_stock_grade_for_ytd`, `validate_allocation_row`, `dedupe_metric_points` |
| `tests/test_assessment_logic.py` | **New.** 42 tests — unit, regression, smoke, sanity (see Test Results) |
| `run_all_tests.py` | **New.** Single entry point: `python run_all_tests.py` or `--sanity` for the fast subset |
| `pytest.ini` | **New.** Registers the `sanity` marker, sets `testpaths = tests` |
| `requirements-dev.txt` | **New.** `pytest`, `pytest-cov` — kept out of the production `requirements.txt` used by Streamlit Cloud |
| `assets/skintific_logo.png` | **New.** Transparent-background wordmark, processed from `D:\Knowledge\company\tech\Logo.png` |

## Database Changes

None in this pass — all required schema changes (`distributor_sku_allocation`
table, nullable `distributor_code`/`allocation_target`, `assessment_users`
table, `master_distributor.distributor_code`) were already applied in prior
sessions per `bq_assessment_schema.sql`. This pass only changes application
code/queries against the existing schema.

**Query changes** (not DDL, but worth flagging):
- `get_ytd_sell_through()`: added `AND brand IN ('SKINTIFIC', 'TIMEPHORIA')`
- `check_login()` / `username_exists()`: `WHERE username = @username` → `WHERE LOWER(username) = @username`
- New query: `get_distributors_for_supervisor()` — `SELECT DISTINCT ... WHERE region = @region AND (UPPER(spv_skt) = UPPER(@full_name) OR UPPER(spv_tph) = UPPER(@full_name))`
- New query: `verify_and_change_password()` — `UPDATE assessment_users SET password = @new_password WHERE LOWER(username) = @username`

## Power BI Changes

None. No Power BI reports, datasets, or `pbi_gt_dataset` schema were modified
— only a `WHERE` clause was added to an existing query reading from
`pbi_gt_dataset.fact_sell_through_all`.

## Test Results

```
42 passed in 0.34s
Coverage: assessment_logic.py — 98% (65 statements, 1 missed)
```

Run yourself with `python run_all_tests.py` from the `streamlit_app` directory
(after `pip install -r requirements-dev.txt`).

Coverage by category:
- **Unit**: `normalize_username`, `value_to_grade` (all banded boundaries),
  `get_sla_grade` (all 9 combinations), `bad_stock_grade_for_ytd`,
  `validate_allocation_row`
- **Regression**: NaN-string blank-cell detection, Salesman 5-row
  double-count (`dedupe_metric_points`), zero-YTD Bad Stock crash
- **Smoke**: `ast.parse()` on both app files + `assessment_logic.py`
- **Sanity**: `pytest -m sanity` — 14-test fast subset

## Known Limitations

- **Brand filter values unverified**: `brand IN ('SKINTIFIC', 'TIMEPHORIA')`
  in `get_ytd_sell_through()` uses column/value names provided by the user,
  not independently confirmed against live `fact_sell_through_all` rows.
  Sanity-check after deploy.
- **SPV column names unverified**: `master_distributor.spv_skt` / `spv_tph`
  in `get_distributors_for_supervisor()` are user-provided, not independently
  confirmed against the live schema. First login by a real Area Sales
  Supervisor account should be checked against their expected distributor list.
- **Integration/system tests against live BigQuery are out of scope** for
  this pass (explicitly confirmed with the user) — the test suite covers pure
  logic only. A follow-up pass would need real read-only credentials and a
  designated test distributor.
- **`parse_allocation_upload()` was not refactored to call
  `validate_allocation_row()`** — both app files keep their own inline,
  already-verified implementation of the same rules, to avoid risking
  regression on a feature with many edge cases tested earlier this session.
  `validate_allocation_row()` exists as the independently-tested reference
  implementation of the rules; it is not yet the literal code path the apps
  execute.
- **Mock intentionally diverges on item 4** (SPV-based mapping) — the mock's
  fictional dataset has no `spv_skt`/`spv_tph` concept, so it keeps the
  simpler region-only distributor filter.
- **No password complexity policy** was added for Change Password, per spec
  ("no new password policy required"). No audit-log hook exists for password
  changes — there is no audit logging system anywhere in this app to feed.
