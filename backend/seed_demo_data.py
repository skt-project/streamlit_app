"""
Seed demo data into sfa_web tables for presentation.
Run ONCE after running 001_sfa_web_new_tables.sql migration.

Usage:
  python seed_demo_data.py
"""
import json
import uuid
from datetime import datetime, timezone, date
from config import settings
from services.bq import BQClient

bq = BQClient.get()
SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"
now = datetime.now(timezone.utc).isoformat()
today = date.today().isoformat()
month_start = date.today().replace(day=1).isoformat()


def run(sql, params):
    try:
        bq.execute(sql, params)
        print("  OK")
    except Exception as e:
        print(f"  SKIP (already exists or error): {e}")


print("=== 1. Announcements ===")
for a in [
    ("Campaign", "Flash Sale 7.7 Skintific", "Semua outlet wajib display bundle SKT 7.7. Target EC rate min 60%.", "Semua SE", "ho_admin"),
    ("Policy",  "Update SOP Kunjungan",       "Mulai 1 Juli 2026, SPV wajib approve kunjungan SE di hari yang sama.", "SPV & ASM", "ho_admin"),
    ("Training","Training Penggunaan STEP",    "Demo STEP Platform untuk seluruh tim sales. Hadir wajib.", "Semua", "ho_admin"),
]:
    run(
        f"""
        INSERT INTO {SFA_WEB}.announcement
          (announcement_id, type, title, body, audience, created_by, created_at, is_deleted)
        VALUES (@id, @tp, @title, @body, @aud, @by, @now, FALSE)
        """,
        [
            bq.p("id",    "STRING",    str(uuid.uuid4())),
            bq.p("tp",    "STRING",    a[0]),
            bq.p("title", "STRING",    a[1]),
            bq.p("body",  "STRING",    a[2]),
            bq.p("aud",   "STRING",    a[3]),
            bq.p("by",    "STRING",    a[4]),
            bq.p("now",   "TIMESTAMP", now),
        ],
    )

print("=== 2. Approval Requests ===")
for ap in [
    ("target_adjust", "Penyesuaian Target Juli - SKT Jakarta Pusat", "spv_jktpusat", "500", "450", "Kompetitor promo besar. Target terlalu tinggi."),
    ("tier_override",  "Request Upgrade Tier Toko Maju Jaya",        "spv_bek01",    "B",   "A",   "Toko konsisten EC setiap kunjungan 3 bulan terakhir."),
]:
    run(
        f"""
        INSERT INTO {SFA_WEB}.approval_request
          (approval_id, type, title, submitted_by, submitted_at, current_value,
           proposed_value, reason, status, comments_json, is_deleted)
        VALUES (@id, @tp, @title, @by, @now, @cur, @prop, @reason, 'pending', @cj, FALSE)
        """,
        [
            bq.p("id",     "STRING",    str(uuid.uuid4())),
            bq.p("tp",     "STRING",    ap[0]),
            bq.p("title",  "STRING",    ap[1]),
            bq.p("by",     "STRING",    ap[2]),
            bq.p("now",    "TIMESTAMP", now),
            bq.p("cur",    "STRING",    ap[3]),
            bq.p("prop",   "STRING",    ap[4]),
            bq.p("reason", "STRING",    ap[5]),
            bq.p("cj",     "STRING",    json.dumps([])),
        ],
    )

print("=== 3. spv_target rows (sample brand targets) ===")
# salesman_sk values are STRING hashes from sfa_web.dim_salesman
SK_ERNA    = "60a2ef484fdf77f0be82bc3a1effd132"  # ERNA
SK_NURLELA = "59c13ab60bc899c3a690138cabe8b5e6"  # NURLELA
for row in [
    (SK_ERNA,    "Skintific", 1000, 950),
    (SK_ERNA,    "G2G",       500,  480),
    (SK_NURLELA, "Skintific", 800,  760),
    (SK_NURLELA, "G2G",       400,  400),
    (SK_ERNA,    "Skintific", 600,  540),
]:
    run(
        f"""
        INSERT INTO {SFA_WEB}.spv_target
          (spv_target_id, salesman_sk, brand, period_month, management_target, spv_target,
           approval_status, created_by, created_at, is_deleted)
        VALUES (@id, @sk, @brand, DATE(@pm), @mt, @st, 'submitted', 'seed', @now, FALSE)
        """,
        [
            bq.p("id",    "STRING",    str(uuid.uuid4())),
            bq.p("sk",    "STRING",    row[0]),
            bq.p("brand", "STRING",    row[1]),
            bq.p("pm",    "DATE",      month_start),
            bq.p("mt",    "FLOAT64",   float(row[2])),
            bq.p("st",    "FLOAT64",   float(row[3])),
            bq.p("now",   "TIMESTAMP", now),
        ],
    )

print("=== Done! ===")
print("Run 001_sfa_web_new_tables.sql in BigQuery console FIRST if you haven't already.")
