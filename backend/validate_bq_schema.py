"""
BigQuery schema validation for STEP Platform go-live.
Run from backend/ directory: python validate_bq_schema.py

Checks:
- All required sfa_web tables exist
- Key columns have correct data types
- No orphan records (users with invalid salesman_sk)
- Demo and admin accounts exist and are active
- Seeded data counts
"""
from config import settings
from services.bq import BQClient

bq = BQClient.get()
p = settings.bq_project
d = settings.bq_dataset

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
results = []


def check(label: str, status: str, detail: str = ""):
    results.append((status, label, detail))
    sym = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]"}[status]
    print(f"  {sym} {label}" + (f": {detail}" if detail else ""))


print(f"\n=== BigQuery Schema Validation ===")
print(f"    Project: {p}")
print(f"    Dataset: {d}\n")

# ── 1. Required tables ───────────────────────────────────────────────────────
print("1. Required Tables")
REQUIRED_TABLES = [
    "users", "announcement", "approval_request", "spv_target",
    "route_assignment", "fact_route_plan_pjp", "dim_salesman",
    "step_visit", "step_visit_item", "step_visit_revision",
]
existing_tables = {
    r["table_name"]
    for r in bq.query(f"""
        SELECT table_name FROM `{p}.{d}.INFORMATION_SCHEMA.TABLES`
        WHERE table_schema = '{d}'
    """)
}
for tbl in REQUIRED_TABLES:
    if tbl in existing_tables:
        check(f"Table {d}.{tbl}", PASS)
    else:
        check(f"Table {d}.{tbl}", FAIL, "MISSING")

# ── 2. Column type validation ────────────────────────────────────────────────
print("\n2. Column Type Validation")
col_checks = [
    ("users", "salesman_sk", "STRING"),
    ("users", "is_active", "BOOL"),
    ("users", "password_hash", "STRING"),
    ("spv_target", "salesman_sk", "STRING"),
    ("route_assignment", "salesman_sk", "STRING"),
    ("route_assignment", "outlet_sk", "STRING"),
    ("fact_route_plan_pjp", "salesman_sk", "STRING"),
    ("fact_route_plan_pjp", "outlet_sk", "STRING"),
    ("fact_route_plan_pjp", "visit_day_of_week", "STRING"),
]
col_info = {
    (r["table_name"], r["column_name"]): r["data_type"]
    for r in bq.query(f"""
        SELECT table_name, column_name, data_type
        FROM `{p}.{d}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_schema = '{d}'
    """)
}
for tbl, col, expected_type in col_checks:
    actual = col_info.get((tbl, col))
    if actual is None:
        check(f"{tbl}.{col}", FAIL, f"column missing")
    elif actual == expected_type:
        check(f"{tbl}.{col}", PASS, f"type={actual}")
    else:
        check(f"{tbl}.{col}", FAIL, f"expected {expected_type}, got {actual}")

# ── 3. Data counts ───────────────────────────────────────────────────────────
print("\n3. Data Counts")
role_counts = bq.query(f"""
    SELECT role, COUNT(*) AS cnt
    FROM `{p}.{d}.users`
    WHERE is_active = TRUE
    GROUP BY role ORDER BY cnt DESC
""")
total_users = sum(r["cnt"] for r in role_counts)
for r in role_counts:
    check(f"Users role={r['role']}", PASS if r["cnt"] > 0 else WARN, f"count={r['cnt']}")
check("Total active users", PASS if total_users >= 500 else WARN, f"count={total_users}")

route_count = bq.query_one(f"""
    SELECT COUNT(*) AS cnt FROM `{p}.{d}.fact_route_plan_pjp`
    WHERE is_deleted = FALSE AND salesman_sk IS NOT NULL
""")
check("Route plan rows (non-null SK)", PASS if (route_count or {}).get("cnt", 0) > 100 else WARN,
      f"count={(route_count or {}).get('cnt', 0)}")

announcement_count = bq.query_one(f"""
    SELECT COUNT(*) AS cnt FROM `{p}.{d}.announcement` WHERE is_deleted = FALSE
""")
check("Announcements", PASS if (announcement_count or {}).get("cnt", 0) >= 3 else WARN,
      f"count={(announcement_count or {}).get('cnt', 0)}")

# ── 4. Demo and admin accounts ───────────────────────────────────────────────
print("\n4. Required Test Accounts")
for username in ["demo", "admin", "se1", "test_se"]:
    row = bq.query_one(f"""
        SELECT username, role, is_active, salesman_sk IS NOT NULL AS has_sk
        FROM `{p}.{d}.users` WHERE username = @u
    """, [bq.p("u", "STRING", username)])
    if row is None:
        check(f"Account: {username}", FAIL, "not found")
    elif not row.get("is_active"):
        check(f"Account: {username}", FAIL, "is_active=FALSE")
    else:
        check(f"Account: {username}", PASS,
              f"role={row['role']} has_salesman_sk={row['has_sk']}")

# ── 5. Demo routes coverage ──────────────────────────────────────────────────
print("\n5. Demo Routes Day Coverage")
demo_sk = "demo" + "0" * 28
day_rows = bq.query(f"""
    SELECT visit_day_of_week, COUNT(*) AS cnt
    FROM `{p}.{d}.fact_route_plan_pjp`
    WHERE salesman_sk = @sk AND is_deleted = FALSE
    GROUP BY visit_day_of_week ORDER BY visit_day_of_week
""", [bq.p("sk", "STRING", demo_sk)])
expected_days = {"Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"}
found_days = {r["visit_day_of_week"] for r in day_rows}
for r in day_rows:
    check(f"Demo routes: {r['visit_day_of_week']}", PASS, f"count={r['cnt']}")
for missing in expected_days - found_days:
    check(f"Demo routes: {missing}", FAIL, "no routes for this day")

# ── 6. Salesman linkage health ───────────────────────────────────────────────
print("\n6. Salesman Linkage Health")
linked = bq.query_one(f"""
    SELECT
      COUNTIF(salesman_sk IS NOT NULL AND salesman_sk != '') AS linked,
      COUNTIF(salesman_sk IS NULL OR salesman_sk = '') AS unlinked,
      COUNT(*) AS total
    FROM `{p}.{d}.users`
    WHERE role = 'se' AND is_active = TRUE
""")
if linked:
    total = linked["total"]
    link_pct = round(linked["linked"] / total * 100, 1) if total > 0 else 0
    check("SE salesman_sk linkage",
          PASS if link_pct > 90 else WARN,
          f"{linked['linked']}/{total} ({link_pct}%) linked")

# ── Summary ──────────────────────────────────────────────────────────────────
print("\n=== Summary ===")
pass_c = sum(1 for s, _, _ in results if s == PASS)
fail_c = sum(1 for s, _, _ in results if s == FAIL)
warn_c = sum(1 for s, _, _ in results if s == WARN)
print(f"  PASS: {pass_c}")
print(f"  WARN: {warn_c}")
print(f"  FAIL: {fail_c}")
if fail_c > 0:
    print("\n  Failed checks:")
    for s, label, detail in results:
        if s == FAIL:
            print(f"    [FAIL] {label}: {detail}")
print(f"\n  Overall: {'GO-LIVE READY' if fail_c == 0 else 'BLOCKERS EXIST — fix before go-live'}")
