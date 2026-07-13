"""
Create 3 fixed test accounts for E2E approval flow testing,
and clone real outlet routes into test_se's salesman_sk.

  test_se    / STEP@2026  — SE  (has 70 real stores across all days)
  test_spv   / STEP@2026  — SPV
  test_dist  / STEP@2026  — dm (final approver)

Run from D:\\GitHub\\skintific-step\\backend:
  python create_test_users.py
"""
import uuid
from datetime import datetime, timezone

from config import settings
from services.auth import hash_password
from services.bq import BQClient

bq  = BQClient.get()
p   = settings.bq_project
d   = settings.bq_dataset
now = datetime.now(timezone.utc).isoformat()
pw  = hash_password("STEP@2026")

TEST_SE_SK = "testse" + "0" * 26   # 32-char fake salesman_sk for test_se
DAYS = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

# ── Step 1: Clone routes for test_se ─────────────────────────────────────────
print("Cloning routes for test_se...")

donor = bq.query_one(f"""
    SELECT salesman_sk, COUNT(*) as cnt
    FROM `{p}.{d}.fact_route_plan_pjp`
    WHERE salesman_sk IS NOT NULL AND is_deleted = FALSE
    GROUP BY salesman_sk ORDER BY cnt DESC LIMIT 1
""")

if not donor:
    print("  WARNING: No route data found in fact_route_plan_pjp — skipping route clone.")
else:
    donor_sk = donor["salesman_sk"]
    print(f"  Donor salesman: {donor_sk} ({donor['cnt']} routes)")

    # Streaming buffer rows from prior runs ARE visible to SELECT but cannot be
    # touched by DELETE/UPDATE. Skip the INSERT if routes already exist under this SK.
    existing = bq.query_one(
        f"SELECT COUNT(*) as n FROM `{p}.{d}.fact_route_plan_pjp` WHERE salesman_sk = @sk AND is_deleted = FALSE",
        [bq.p("sk", "STRING", TEST_SE_SK)],
    )
    if existing and existing["n"] > 0:
        print(f"  test_se already has {existing['n']} routes — skipping clone")
    else:
        bq.execute(f"""
            INSERT INTO `{p}.{d}.fact_route_plan_pjp` (
                route_plan_sk, salesman_sk, outlet_sk,
                source_salesman_name, source_outlet_code,
                distributor_code, distributor_name, region, asm_name,
                visit_day_of_week, visit_week_pattern, visit_frequency_code,
                batch_uploaded_at, sfa_web_loaded_at, is_deleted, brand_group
            )
            SELECT
                GENERATE_UUID()                  AS route_plan_sk,
                @test_sk                         AS salesman_sk,
                outlet_sk,
                'TEST SE'                        AS source_salesman_name,
                COALESCE(source_outlet_code, '') AS source_outlet_code,
                COALESCE(distributor_code, '')   AS distributor_code,
                COALESCE(distributor_name, '')   AS distributor_name,
                COALESCE(region, '')             AS region,
                COALESCE(asm_name, '')           AS asm_name,
                CASE MOD(ROW_NUMBER() OVER (), 7)
                  WHEN 0 THEN 'Senin'
                  WHEN 1 THEN 'Selasa'
                  WHEN 2 THEN 'Rabu'
                  WHEN 3 THEN 'Kamis'
                  WHEN 4 THEN 'Jumat'
                  WHEN 5 THEN 'Sabtu'
                  ELSE        'Minggu'
                END                              AS visit_day_of_week,
                ''                               AS visit_week_pattern,
                COALESCE(NULLIF(visit_frequency_code,''), 'F4') AS visit_frequency_code,
                CURRENT_TIMESTAMP()              AS batch_uploaded_at,
                CURRENT_TIMESTAMP()              AS sfa_web_loaded_at,
                FALSE                            AS is_deleted,
                COALESCE(brand_group, '')        AS brand_group
            FROM (
                SELECT DISTINCT outlet_sk, source_outlet_code, distributor_code,
                       distributor_name, region, asm_name, visit_frequency_code, brand_group
                FROM `{p}.{d}.fact_route_plan_pjp`
                WHERE salesman_sk = @donor_sk AND is_deleted = FALSE AND outlet_sk IS NOT NULL
                LIMIT 70
            )
        """, [
            bq.p("test_sk",  "STRING", TEST_SE_SK),
            bq.p("donor_sk", "STRING", donor_sk),
        ])

        count = bq.query_one(
            f"SELECT COUNT(*) as n FROM `{p}.{d}.fact_route_plan_pjp` WHERE salesman_sk = @sk",
            [bq.p("sk", "STRING", TEST_SE_SK)],
        )
        print(f"  Inserted {count['n']} route rows (DML — immediately queryable)")

# ── Step 2: Create / reset user accounts ─────────────────────────────────────
print("\nCreating user accounts...")

TEST_USERS = [
    dict(username="test_se",   full_name="Test SE",                role="salesman", salesman_sk=TEST_SE_SK),
    dict(username="test_spv",  full_name="Test SPV",               role="spv",      salesman_sk=None),
    dict(username="test_dist", full_name="Test Distributor Manager",role="dm",       salesman_sk=None),
]

existing = {r["username"] for r in bq.query(f"SELECT username FROM `{p}.{d}.users`")}

for u in TEST_USERS:
    if u["username"] in existing:
        bq.execute(
            f"""UPDATE `{p}.{d}.users`
                SET password_hash = @pw, salesman_sk = @sk, is_active = TRUE, updated_at = @now
                WHERE username = @un""",
            [
                bq.p("pw",  "STRING",    pw),
                bq.p("sk",  "STRING",    u["salesman_sk"]),
                bq.p("now", "TIMESTAMP", now),
                bq.p("un",  "STRING",    u["username"]),
            ],
        )
        print(f"  reset: {u['username']}")
    else:
        bq.execute(
            f"""INSERT INTO `{p}.{d}.users`
                  (user_id, username, full_name, password_hash, role, salesman_sk, is_active, created_at, updated_at)
                VALUES (@uid, @un, @fn, @pw, @role, @sk, TRUE, @now, @now)""",
            [
                bq.p("uid",  "STRING",    str(uuid.uuid4())),
                bq.p("un",   "STRING",    u["username"]),
                bq.p("fn",   "STRING",    u["full_name"]),
                bq.p("pw",   "STRING",    pw),
                bq.p("role", "STRING",    u["role"]),
                bq.p("sk",   "STRING",    u["salesman_sk"]),
                bq.p("now",  "TIMESTAMP", now),
            ],
        )
        print(f"  created: {u['username']}")

print("""
Done. Test credentials:

  test_se    / STEP@2026   — SE (salesman)    — has real store routes
  test_spv   / STEP@2026   — SPV              — approves first
  test_dist  / STEP@2026   — DM (dm role)     — final approver

Approval flow to test:
  1. Login as test_se → open Route List → check in to a store → submit visit
  2. Login as test_spv → Approvals → approve the visit
  3. Login as test_dist → Visits → approve → COMPLETED
""")
