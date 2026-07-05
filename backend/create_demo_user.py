"""
Create a demo test account with routes for all 7 days (Minggu-Sabtu).
Clones real outlet data from the donor salesman with the most routes.
username: demo    password: STEP@2026
"""
import uuid
from datetime import datetime, timezone

from config import settings
from services.auth import hash_password
from services.bq import BQClient

bq = BQClient.get()
p = settings.bq_project
d = settings.bq_dataset
now = datetime.now(timezone.utc).isoformat()

DEMO_SK = "demo" + "0" * 28   # 32-char fake salesman_sk
DAYS    = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]

# ── Step 1: best donor (non-NULL salesman_sk, most routes) ───────────────────
donor = bq.query_one(f"""
    SELECT salesman_sk, COUNT(*) as cnt
    FROM `{p}.{d}.fact_route_plan_pjp`
    WHERE salesman_sk IS NOT NULL AND is_deleted = FALSE
    GROUP BY salesman_sk ORDER BY cnt DESC LIMIT 1
""")
donor_sk = donor["salesman_sk"]
print(f"Donor: {donor_sk} ({donor['cnt']} routes)")

# ── Step 2: 70 distinct outlets from donor ───────────────────────────────────
outlets = bq.query(f"""
    SELECT DISTINCT
        outlet_sk, source_outlet_code, distributor_code, distributor_name,
        region, asm_name, visit_frequency_code, brand_group
    FROM `{p}.{d}.fact_route_plan_pjp`
    WHERE salesman_sk = @sk AND is_deleted = FALSE
      AND outlet_sk IS NOT NULL
    LIMIT 70
""", [bq.p("sk", "STRING", donor_sk)])
print(f"Outlets to clone: {len(outlets)}")

# ── Step 3: clear old demo routes ────────────────────────────────────────────
bq.execute(f"""
    DELETE FROM `{p}.{d}.fact_route_plan_pjp` WHERE salesman_sk = @sk
""", [bq.p("sk", "STRING", DEMO_SK)])

# ── Step 4: build rows for all 7 days ────────────────────────────────────────
rows = []
for i, o in enumerate(outlets):
    rows.append({
        "route_plan_sk":        str(uuid.uuid4()),
        "salesman_sk":          DEMO_SK,
        "outlet_sk":            o["outlet_sk"],
        "source_salesman_name": "DEMO USER",
        "source_outlet_code":   o.get("source_outlet_code") or "",
        "distributor_code":     o.get("distributor_code") or "",
        "distributor_name":     o.get("distributor_name") or "",
        "region":               o.get("region") or "",
        "asm_name":             o.get("asm_name") or "",
        "visit_day_of_week":    DAYS[i % len(DAYS)],
        "visit_week_pattern":   "",
        "visit_frequency_code": o.get("visit_frequency_code") or "F4",
        "batch_uploaded_at":    now,
        "sfa_web_loaded_at":    now,
        "is_deleted":           False,
        "brand_group":          o.get("brand_group") or "",
    })

bq.insert_rows("fact_route_plan_pjp", rows)
print(f"Inserted {len(rows)} route rows (10 per day across all 7 days)")

# ── Step 5: create/reset demo user ───────────────────────────────────────────
pw = hash_password("STEP@2026")
existing = bq.query(f"SELECT user_id FROM `{p}.{d}.users` WHERE username = 'demo'")
if existing:
    bq.execute(f"""
        UPDATE `{p}.{d}.users`
        SET password_hash = @pw, salesman_sk = @sk, is_active = TRUE,
            updated_at = CURRENT_TIMESTAMP()
        WHERE username = 'demo'
    """, [bq.p("pw", "STRING", pw), bq.p("sk", "STRING", DEMO_SK)])
    print("demo user updated")
else:
    bq.execute(f"""
        INSERT INTO `{p}.{d}.users`
          (user_id, username, full_name, password_hash, role, salesman_sk, is_active, created_at, updated_at)
        VALUES (@uid, 'demo', 'Demo User', @pw, 'se', @sk, TRUE, @now, @now)
    """, [
        bq.p("uid", "STRING", str(uuid.uuid4())),
        bq.p("pw",  "STRING", pw),
        bq.p("sk",  "STRING", DEMO_SK),
        bq.p("now", "TIMESTAMP", now),
    ])
    print("demo user created")

print(f"\nReady:")
print(f"  username : demo")
print(f"  password : STEP@2026")
print(f"  routes   : {len(rows)} stores, 10 per day, every day including Sunday")
