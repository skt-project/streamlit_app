"""
One-time + recurring sync: populate sfa_web from source tables.
Run from backend/ directory:

    python sync_sfa_web.py

Uses BQ_SA_KEY_PATH env var (or set BQ_SA_KEY_JSON for base64).
Writes ONLY to sfa_web — all source datasets are read-only.

Sync order (deps matter):
  1. dim_salesman   — from gt_schema.gt_salesman_mapping + gt_master_salesman
  2. dim_outlet     — from gt_schema.master_store_database
  3. fact_route_plan_pjp — from gt_schema.gt_master_salesman_pjp (joins dims)
  4. views          — create/replace vw_salesman_active and vw_outlet_active
"""
import base64
import json
import os
import sys

from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT = "skintific-data-warehouse"
DATASET = "sfa_web"

# ------------------------------------------------------------------
# Auth
# ------------------------------------------------------------------
SA_KEY_PATH = os.getenv("BQ_SA_KEY_PATH", "")
SA_KEY_JSON = os.getenv("BQ_SA_KEY_JSON", "")

if SA_KEY_JSON:
    info = json.loads(base64.b64decode(SA_KEY_JSON).decode())
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
elif SA_KEY_PATH:
    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
else:
    creds = None  # ADC

client = bigquery.Client(project=PROJECT, credentials=creds)


def run(sql: str, label: str) -> None:
    print(f"  → {label} ...", end=" ", flush=True)
    try:
        job = client.query(sql)
        job.result()
        print("done")
    except Exception as e:
        print(f"FAILED\n    {e}")
        sys.exit(1)


T = lambda name: f"`{PROJECT}.{DATASET}.{name}`"
SK = lambda system, code: f"TO_HEX(MD5(CONCAT('{system}', '|', CAST(({code}) AS STRING))))"

# ------------------------------------------------------------------
# 1. dim_salesman
# ------------------------------------------------------------------
print("\n[1] dim_salesman")
# Only refresh GT_MAPPING rows — other source_system rows (SKT_EXCEL, etc.) are preserved
run(f"DELETE FROM {T('dim_salesman')} WHERE source_system = 'GT_MAPPING'", "delete GT_MAPPING rows")
run(f"""
INSERT INTO {T('dim_salesman')}
  (salesman_sk, source_system, source_salesman_code, salesman_name, salesman_type,
   role_type, distributor_code, region, spv_name, asm_name, is_active,
   brand_group, source_updated_at, sfa_web_loaded_at, is_deleted)
SELECT
  TO_HEX(MD5(CONCAT('GT_MAPPING', '|', m.salesman_id))),
  'GT_MAPPING',
  m.salesman_id,
  m.salesman,
  m.salesman_type,
  'SALESMAN',
  m.distributor_code,
  gm.region,
  gm.nama_spv_internal,
  gm.asm,
  m.is_active,
  'G2G',
  SAFE_CAST(m.updated_at AS TIMESTAMP),
  CURRENT_TIMESTAMP(),
  FALSE
FROM `{PROJECT}.gt_schema.gt_salesman_mapping` m
LEFT JOIN `{PROJECT}.gt_schema.gt_master_salesman` gm
  ON gm.nama_salesman = m.salesman
WHERE m.salesman_id IS NOT NULL
""", "insert from gt_salesman_mapping (brand_group=G2G)")

# ------------------------------------------------------------------
# 2. dim_outlet
# ------------------------------------------------------------------
print("\n[2] dim_outlet")
run(f"TRUNCATE TABLE {T('dim_outlet')}", "truncate")
run(f"""
INSERT INTO {T('dim_outlet')}
  (outlet_sk, source_system, source_outlet_code, store_name, brand, channel,
   store_grade, customer_category, region, distributor_code, distributor_name,
   asm_name, spv_name, address, latitude, longitude, operational_status,
   brand_group, source_updated_at, sfa_web_loaded_at, is_deleted)
SELECT
  TO_HEX(MD5(CONCAT('GT', '|', cust_id))),
  'GT',
  cust_id,
  store_name,
  brand,
  CASE WHEN ba_non_ba = 'BA' THEN 'BA' ELSE 'GT' END,
  COALESCE(sktf_store_grade_q1_25, g2g_store_grade_q1_25),
  customer_category,
  region,
  distributor_code,
  distributor,
  asm,
  spv,
  address,
  SAFE_CAST(latitude AS FLOAT64),
  SAFE_CAST(longitude AS FLOAT64),
  customer_status,
  CASE
    WHEN LOWER(brand) IN ('glad2glow', 'bodibreeze', 'next prime', 'g2g') THEN 'G2G'
    WHEN LOWER(brand) IN ('skintific', 'timephoria', 'facerinna') THEN 'SKT'
    ELSE NULL
  END,
  CAST(input_date AS TIMESTAMP),
  CURRENT_TIMESTAMP(),
  FALSE
FROM `{PROJECT}.gt_schema.master_store_database`
WHERE cust_id IS NOT NULL
""", "insert from master_store_database (brand_group derived)")

# ------------------------------------------------------------------
# 3. fact_route_plan_pjp
# ------------------------------------------------------------------
print("\n[3] fact_route_plan_pjp")
run(f"TRUNCATE TABLE {T('fact_route_plan_pjp')}", "truncate")
run(f"""
INSERT INTO {T('fact_route_plan_pjp')}
  (route_plan_sk, salesman_sk, outlet_sk, source_salesman_name, source_outlet_code,
   distributor_code, distributor_name, region, asm_name,
   visit_day_of_week, visit_week_pattern, visit_frequency_code,
   brand_group, batch_uploaded_at, sfa_web_loaded_at, is_deleted)
SELECT
  TO_HEX(MD5(CONCAT('PJP', '|', p.kode_distributor, '|', p.nama_salesman, '|', p.kode_toko, '|', p.hari, '|', IFNULL(p.minggu,'')))),
  sm.salesman_sk,
  ot.outlet_sk,
  p.nama_salesman,
  p.kode_toko,
  p.kode_distributor,
  p.nama_distributor,
  p.region,
  p.asm,
  p.hari,
  p.minggu,
  p.frekuensi,
  sm.brand_group,
  SAFE_CAST(p.uploaded_at AS TIMESTAMP),
  CURRENT_TIMESTAMP(),
  FALSE
FROM `{PROJECT}.gt_schema.gt_master_salesman_pjp` p
LEFT JOIN {T('dim_salesman')} sm
  ON sm.source_system = 'GT_MAPPING' AND sm.salesman_name = p.nama_salesman
LEFT JOIN {T('dim_outlet')} ot
  ON ot.source_system = 'GT' AND ot.source_outlet_code = p.kode_toko
""", "insert from gt_master_salesman_pjp (brand_group from salesman)")

# ------------------------------------------------------------------
# 4. Views
# ------------------------------------------------------------------
print("\n[4] views")
run(f"""
CREATE OR REPLACE VIEW {T('vw_salesman_active')} AS
SELECT
  salesman_sk, source_system, source_salesman_code, salesman_name,
  salesman_type, role_type, distributor_code, region, spv_name, asm_name,
  is_active, brand_group, source_updated_at
FROM {T('dim_salesman')}
WHERE is_deleted = FALSE
""", "vw_salesman_active")

run(f"""
CREATE OR REPLACE VIEW {T('vw_outlet_active')} AS
SELECT
  outlet_sk, source_system, source_outlet_code, store_name, brand, channel,
  store_grade, customer_category, region, distributor_code, distributor_name,
  asm_name, spv_name, address, latitude, longitude, operational_status, brand_group
FROM {T('dim_outlet')}
WHERE is_deleted = FALSE
""", "vw_outlet_active")

# ------------------------------------------------------------------
# ------------------------------------------------------------------
# 5. fact_management_target
# ------------------------------------------------------------------
print("\n[5] fact_management_target")
run(f"TRUNCATE TABLE {T('fact_management_target')}", "truncate")
run(f"""
INSERT INTO {T('fact_management_target')}
  (target_sk, outlet_sk, source_customer_id, calendar_date, brand, brand_group,
   management_target_amount, weekly_visit_target, region, distributor_name,
   spv_name, asm_name, source_loaded_at, sfa_web_loaded_at, is_deleted)
SELECT
  TO_HEX(MD5(CONCAT('TARGET', '|', customer_id, '|', CAST(calendar_date AS STRING), '|', brand_name))),
  TO_HEX(MD5(CONCAT('GT', '|', customer_id))),
  customer_id,
  calendar_date,
  brand_name,
  CASE
    WHEN brand_name IN ('Skintific', 'Timephoria', 'Facerinna') THEN 'SKT'
    WHEN brand_name IN ('Glad2Glow', 'Bodibreeze', 'Next Prime') THEN 'G2G'
    ELSE NULL
  END,
  target_amount,
  weekly_visit_target,
  region,
  distributor,
  spv_name,
  asm_name,
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  FALSE
FROM `{PROJECT}.gt_schema.fact_gt_target_v2_t`,
UNNEST([
  STRUCT('Skintific'   AS brand_name, skintific_target AS target_amount),
  STRUCT('Glad2Glow',  g2g_target),
  STRUCT('Timephoria', timephoria_target)
])
WHERE customer_id IS NOT NULL AND target_amount IS NOT NULL
""", "insert from fact_gt_target_v2_t (unpivoted 3 brands)")

# ------------------------------------------------------------------
# Row counts
# ------------------------------------------------------------------
print("\n[✓] Row counts:")
for tbl in ("dim_salesman", "dim_outlet", "fact_route_plan_pjp", "fact_management_target"):
    row = list(client.query(f"SELECT COUNT(*) AS n FROM {T(tbl)}").result())[0]
    print(f"    {tbl}: {row['n']:,}")

print("\nSync complete.")
