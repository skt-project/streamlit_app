"""
Ensure fact_route_plan_pjp exists in sfa_web, and add kecamatan/city/is_active
to dim_outlet if missing (used by outlet_web.py).

Safe to re-run: uses CREATE TABLE IF NOT EXISTS and ADD COLUMN IF NOT EXISTS.

    $env:BQ_SA_KEY_PATH = "D:\\Claude\\bq-sfa-web-api.json"
    python migrate_route_plan.py
"""
import os, sys
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT = "skintific-data-warehouse"
DATASET = "sfa_web"

SA_KEY_PATH = os.getenv("BQ_SA_KEY_PATH", "")
SA_KEY_JSON = os.getenv("BQ_SA_KEY_JSON", "")

if SA_KEY_JSON:
    import base64, json
    info = json.loads(base64.b64decode(SA_KEY_JSON).decode())
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery"])
elif SA_KEY_PATH:
    creds = service_account.Credentials.from_service_account_file(
        SA_KEY_PATH, scopes=["https://www.googleapis.com/auth/bigquery"])
else:
    creds = None

client = bigquery.Client(project=PROJECT, credentials=creds)
T = lambda n: f"`{PROJECT}.{DATASET}.{n}`"


def run(sql: str, label: str) -> None:
    print(f"  -> {label} ...", end=" ", flush=True)
    try:
        client.query(sql).result()
        print("done")
    except Exception as e:
        msg = str(e)
        if "already exists" in msg.lower():
            print("already exists, skipping")
        else:
            print(f"FAILED\n    {msg}")


print("\n[1] fact_route_plan_pjp")
run(f"""
CREATE TABLE IF NOT EXISTS {T('fact_route_plan_pjp')} (
  route_plan_sk         STRING    NOT NULL OPTIONS(description="Surrogate key: fn_surrogate_key('PJP', concat fields)"),
  salesman_sk           STRING             OPTIONS(description="FK dim_salesman"),
  outlet_sk             STRING             OPTIONS(description="FK dim_outlet"),
  source_salesman_name  STRING    NOT NULL OPTIONS(description="Raw salesman name from source"),
  source_outlet_code    STRING    NOT NULL OPTIONS(description="Raw outlet/store code from source"),
  distributor_code      STRING,
  distributor_name      STRING,
  region                STRING,
  asm_name              STRING,
  visit_day_of_week     STRING             OPTIONS(description="Indonesian day name: Senin/Selasa/Rabu/Kamis/Jumat"),
  visit_week_pattern    STRING             OPTIONS(description="Setiap Minggu | Minggu Ganjil | Minggu Genap"),
  visit_frequency_code  STRING             OPTIONS(description="F1=weekly | F2=biweekly"),
  sequence_order        INT64              OPTIONS(description="Visit sequence order within the day for this salesman"),
  batch_uploaded_at     TIMESTAMP NOT NULL,
  sfa_step_loaded_at    TIMESTAMP NOT NULL,
  is_deleted            BOOL      NOT NULL,
  PRIMARY KEY (route_plan_sk) NOT ENFORCED
)
PARTITION BY DATE(batch_uploaded_at)
CLUSTER BY salesman_sk, outlet_sk
OPTIONS (description = "GT recurring route plan (PJP): day-of-week pattern, not date-exploded.")
""", "CREATE fact_route_plan_pjp")


print("\n[2] dim_outlet — add kecamatan, city, is_active, default_salesman_sk if missing")
for col, dtype in [
    ("kecamatan",          "STRING"),
    ("city",               "STRING"),
    ("is_active",          "BOOL"),
    ("default_salesman_sk","STRING"),
    ("spv_salesman_sk",    "STRING"),
]:
    run(f"""
    ALTER TABLE {T('dim_outlet')}
    ADD COLUMN IF NOT EXISTS {col} {dtype}
    OPTIONS(description='Added by migrate_route_plan.py')
    """, f"ADD dim_outlet.{col}")


print("\n[3] dim_salesman — add brand_group if missing (needed by outlet joins)")
run(f"""
ALTER TABLE {T('dim_salesman')}
ADD COLUMN IF NOT EXISTS brand_group STRING
OPTIONS(description='SKT | G2G — added by migrate_route_plan.py')
""", "ADD dim_salesman.brand_group")


print("\n[OK] Done.")
for tbl in ["fact_route_plan_pjp"]:
    try:
        row = list(client.query(f"SELECT COUNT(*) AS n FROM {T(tbl)}").result())[0]
        print(f"    {tbl:<35} {row['n']:,} rows")
    except Exception as e:
        print(f"    {tbl:<35} ERROR: {e}")
