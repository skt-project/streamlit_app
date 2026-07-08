"""
Seed realistic demo visit data into sfa_web for STEP Web population.

Run AFTER seed_demo_data.py and after migrate_visit_tables.py:

    $env:BQ_SA_KEY_PATH = "D:\\Claude\\bq-sfa-web-api.json"
    python seed_visit_data.py

Writes ONLY to sfa_web. All reads from dim_outlet/dim_sku are read-only.
"""
import uuid
import random
from datetime import datetime, timezone, date, timedelta
from config import settings
from services.bq import BQClient

bq = BQClient.get()
SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"
now = datetime.now(timezone.utc)
today = date.today()

# Known salesman SKs from sfa_web.dim_salesman (confirmed in seed_demo_data.py)
SK_ERNA    = "60a2ef484fdf77f0be82bc3a1effd132"  # ERNA — brand_group: SKT
SK_NURLELA = "59c13ab60bc899c3a690138cabe8b5e6"  # NURLELA — brand_group: G2G

SALESMEN = [
    {"sk": SK_ERNA,    "name": "ERNA",    "brand_group": "SKT"},
    {"sk": SK_NURLELA, "name": "NURLELA", "brand_group": "G2G"},
]


def ok(label: str):
    print(f"  ✓ {label}")


def skip(label: str, e: Exception):
    msg = str(e)
    if "duplicate" in msg.lower() or "already" in msg.lower():
        print(f"  ~ {label} (already exists)")
    else:
        print(f"  ! {label} — {msg}")


# ─── 1. Get real outlet SKs from dim_outlet ───────────────────────────────────
print("\n=== Fetching outlet SKs from dim_outlet ===")
outlets = []
try:
    rows = bq.query(
        f"""
        SELECT outlet_sk, source_outlet_code, store_name
        FROM {SFA_WEB}.dim_outlet
        WHERE is_deleted = FALSE AND outlet_sk IS NOT NULL
        ORDER BY store_name
        LIMIT 30
        """,
        [],
    )
    outlets = [{"sk": str(r["outlet_sk"]), "code": r.get("source_outlet_code", ""), "name": r.get("store_name", "Toko")} for r in rows]
    print(f"  Found {len(outlets)} outlets")
except Exception as e:
    print(f"  ! Could not query dim_outlet: {e}")

if not outlets:
    print("  No outlets found — creating fallback synthetic outlet SKs")
    # Fallback: use GT-style outlet codes from the surrogate key formula
    # fn_surrogate_key('GT', cust_id) = TO_HEX(SHA256('GT|cust_id'))
    # These are placeholder SKs; real ones come from GT sync
    outlets = [
        {"sk": f"outlet_demo_{i:03d}", "code": f"DEMO{i:03d}", "name": f"Toko Demo {i}"}
        for i in range(1, 21)
    ]


# ─── 2. Get real SKU data from dim_sku ────────────────────────────────────────
print("\n=== Fetching SKUs from dim_sku ===")
skus = []
try:
    rows = bq.query(
        f"""
        SELECT sku_id, sku_name, brand, brand_group, category, stp
        FROM {SFA_WEB}.dim_sku
        WHERE is_deleted = FALSE AND is_active = TRUE
        ORDER BY brand_group, sku_name
        LIMIT 20
        """,
        [],
    )
    skus = list(rows)
    print(f"  Found {len(skus)} active SKUs")
except Exception as e:
    print(f"  ! Could not query dim_sku: {e}")

if not skus:
    print("  No SKUs found — creating fallback demo SKUs")
    skus = [
        {"sku_id": "SKT-001", "sku_name": "Skintific 10% Niacinamide", "brand": "Skintific", "brand_group": "SKT", "category": "Serum", "stp": 85000.0},
        {"sku_id": "SKT-002", "sku_name": "Skintific Mugwort Clay Mask", "brand": "Skintific", "brand_group": "SKT", "category": "Mask", "stp": 65000.0},
        {"sku_id": "SKT-003", "sku_name": "Skintific Acne Spot Treatment", "brand": "Skintific", "brand_group": "SKT", "category": "Treatment", "stp": 45000.0},
        {"sku_id": "SKT-004", "sku_name": "Skintific Ceramide Moisturizer", "brand": "Skintific", "brand_group": "SKT", "category": "Moisturizer", "stp": 75000.0},
        {"sku_id": "G2G-001", "sku_name": "Glad2Glow Brightening Serum", "brand": "Glad2Glow", "brand_group": "G2G", "category": "Serum", "stp": 55000.0},
        {"sku_id": "G2G-002", "sku_name": "Glad2Glow UV Shield SPF 50", "brand": "Glad2Glow", "brand_group": "G2G", "category": "Sunscreen", "stp": 70000.0},
        {"sku_id": "G2G-003", "sku_name": "Glad2Glow Collagen Cream", "brand": "Glad2Glow", "brand_group": "G2G", "category": "Moisturizer", "stp": 60000.0},
    ]

skt_skus = [s for s in skus if s.get("brand_group") == "SKT"] or skus[:4]
g2g_skus = [s for s in skus if s.get("brand_group") == "G2G"] or skus[4:]


# ─── 3. Seed fact_route_plan_pjp ─────────────────────────────────────────────
print("\n=== Seeding fact_route_plan_pjp ===")
DAYS_ID = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat"]
PATTERNS = ["Setiap Minggu", "Minggu Ganjil", "Minggu Genap"]
FREQS = ["F1", "F2"]

# Assign first 20 outlets: 10 for ERNA, 10 for NURLELA
erna_outlets  = outlets[:10] if len(outlets) >= 10 else outlets
nurlela_outlets = outlets[10:20] if len(outlets) >= 20 else outlets[:min(10, len(outlets))]

pjp_entries = []
for i, outlet in enumerate(erna_outlets):
    pjp_entries.append({
        "salesman": SK_ERNA,
        "outlet": outlet,
        "day": DAYS_ID[i % 5],
        "pattern": PATTERNS[i % 3],
        "freq": FREQS[i % 2],
    })
for i, outlet in enumerate(nurlela_outlets):
    pjp_entries.append({
        "salesman": SK_NURLELA,
        "outlet": outlet,
        "day": DAYS_ID[i % 5],
        "pattern": PATTERNS[(i + 1) % 3],
        "freq": FREQS[(i + 1) % 2],
    })

for entry in pjp_entries:
    rsk = f"PJP-{uuid.uuid4().hex[:16].upper()}"
    try:
        bq.execute(
            f"""
            INSERT INTO {SFA_WEB}.fact_route_plan_pjp
              (route_plan_sk, salesman_sk, outlet_sk, source_salesman_name,
               source_outlet_code, visit_day_of_week, visit_week_pattern,
               visit_frequency_code, batch_uploaded_at, sfa_step_loaded_at, is_deleted)
            VALUES (@rsk, @ssk, @osk, @sname, @ocode, @day, @pat, @freq, @now, @now, FALSE)
            """,
            [
                bq.p("rsk",   "STRING",    rsk),
                bq.p("ssk",   "STRING",    entry["salesman"]),
                bq.p("osk",   "STRING",    entry["outlet"]["sk"]),
                bq.p("sname", "STRING",    "SEED"),
                bq.p("ocode", "STRING",    entry["outlet"]["code"]),
                bq.p("day",   "STRING",    entry["day"]),
                bq.p("pat",   "STRING",    entry["pattern"]),
                bq.p("freq",  "STRING",    entry["freq"]),
                bq.p("now",   "TIMESTAMP", now.isoformat()),
            ],
        )
        ok(f"PJP: {entry['salesman'][:8]}… → {entry['outlet']['name'][:30]} ({entry['day']})")
    except Exception as e:
        skip(f"PJP: {entry['outlet']['name'][:30]}", e)


# ─── 4. Seed fact_visit + fact_visit_item ─────────────────────────────────────
print("\n=== Seeding fact_visit rows (4 weeks) ===")

def make_visit(salesman: dict, outlet: dict, visit_date: date, is_effective: bool) -> str:
    """Insert one visit and its items. Returns visit_id."""
    vid = f"VST-{uuid.uuid4().hex[:16].upper()}"
    checkin_dt  = datetime(visit_date.year, visit_date.month, visit_date.day,
                           8 + random.randint(0, 3), random.randint(0, 59), tzinfo=timezone.utc)
    checkout_dt = checkin_dt + timedelta(minutes=random.randint(20, 90))
    duration    = int((checkout_dt - checkin_dt).total_seconds() // 60)

    skus_for_brand = skt_skus if salesman["brand_group"] == "SKT" else g2g_skus
    items_to_insert = random.sample(skus_for_brand, min(random.randint(2, 5), len(skus_for_brand)))

    total_demand = 0.0
    if is_effective:
        for sku in items_to_insert:
            qty = random.randint(1, 10)
            stp = float(sku.get("stp") or 0)
            total_demand += qty * stp

    bq.execute(
        f"""
        INSERT INTO {SFA_WEB}.fact_visit
          (visit_id, salesman_sk, outlet_sk, visit_date, visit_type, brand_group,
           checkin_time, checkout_time, total_demand, effective_call, duration_minutes,
           visit_status, approval_status, created_at, updated_at, is_deleted)
        VALUES
          (@vid, @ssk, @osk, @vdate, 'ROUTE', @bg,
           @cin, @cout, @demand, @ec, @dur,
           'SUBMITTED', 'PENDING_SPV', @now, @now, FALSE)
        """,
        [
            bq.p("vid",    "STRING",    vid),
            bq.p("ssk",    "STRING",    salesman["sk"]),
            bq.p("osk",    "STRING",    outlet["sk"]),
            bq.p("vdate",  "DATE",      visit_date.isoformat()),
            bq.p("bg",     "STRING",    salesman["brand_group"]),
            bq.p("cin",    "TIMESTAMP", checkin_dt.isoformat()),
            bq.p("cout",   "TIMESTAMP", checkout_dt.isoformat()),
            bq.p("demand", "FLOAT64",   total_demand),
            bq.p("ec",     "STRING",    "YES" if is_effective else "NO"),
            bq.p("dur",    "INT64",     duration),
            bq.p("now",    "TIMESTAMP", now.isoformat()),
        ],
    )

    if is_effective and items_to_insert:
        for sku in items_to_insert:
            qty = random.randint(1, 10)
            stp = float(sku.get("stp") or 0)
            demand = qty * stp
            viid = f"VTI-{uuid.uuid4().hex[:16].upper()}"
            try:
                bq.execute(
                    f"""
                    INSERT INTO {SFA_WEB}.fact_visit_item
                      (visit_item_id, visit_id, sku_id, sku_name, brand, brand_group,
                       category, stp, qty, demand, created_at)
                    VALUES (@viid, @vid, @sid, @sname, @brand, @bg, @cat, @stp, @qty, @dem, @now)
                    """,
                    [
                        bq.p("viid",  "STRING",    viid),
                        bq.p("vid",   "STRING",    vid),
                        bq.p("sid",   "STRING",    sku["sku_id"]),
                        bq.p("sname", "STRING",    sku.get("sku_name", "")),
                        bq.p("brand", "STRING",    sku.get("brand", "")),
                        bq.p("bg",    "STRING",    sku.get("brand_group", "")),
                        bq.p("cat",   "STRING",    sku.get("category", "")),
                        bq.p("stp",   "FLOAT64",   stp),
                        bq.p("qty",   "INT64",     qty),
                        bq.p("dem",   "FLOAT64",   demand),
                        bq.p("now",   "TIMESTAMP", now.isoformat()),
                    ],
                )
            except Exception as e:
                skip(f"visit_item {viid}", e)

    return vid


# Generate 4 weeks of visits: Mon–Fri, 3–5 visits per day per salesman
total_visits = 0
for week_offset in range(4):
    week_monday = today - timedelta(days=today.weekday()) - timedelta(weeks=week_offset)
    for day_offset in range(5):  # Mon=0 to Fri=4
        visit_date = week_monday + timedelta(days=day_offset)
        if visit_date > today:
            continue

        for salesman in SALESMEN:
            # Pick 3-5 random outlets for this salesman on this day
            salesman_outlets = erna_outlets if salesman["sk"] == SK_ERNA else nurlela_outlets
            n_visits = random.randint(3, min(5, len(salesman_outlets)))
            day_outlets = random.sample(salesman_outlets, n_visits)

            for outlet in day_outlets:
                is_effective = random.random() < 0.65  # 65% effective call rate
                try:
                    vid = make_visit(salesman, outlet, visit_date, is_effective)
                    ok(f"Visit {visit_date} {salesman['name']} → {outlet['name'][:25]} (EC={is_effective})")
                    total_visits += 1
                except Exception as e:
                    skip(f"Visit {visit_date} {salesman['name']} → {outlet['name'][:25]}", e)

print(f"\n=== Done! Seeded {total_visits} demo visits ===")
print("The STEP Web Dashboard, Route Evaluate, and Reports pages should now show live data.")
