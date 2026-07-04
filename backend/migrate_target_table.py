"""
Migration: create fact_management_target in sfa_web.
Run once from backend/ directory:

    python migrate_target_table.py
"""
import base64, json, os, sys
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT = "skintific-data-warehouse"
DATASET = "sfa_web"

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
    creds = None

client = bigquery.Client(project=PROJECT, credentials=creds)
T = lambda name: f"`{PROJECT}.{DATASET}.{name}`"


def run(sql: str, label: str) -> None:
    print(f"  → {label} ...", end=" ", flush=True)
    try:
        client.query(sql).result()
        print("done")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("already exists, skipping")
        else:
            print(f"FAILED\n    {e}")
            sys.exit(1)


print("\nCreating fact_management_target in sfa_web...")

run(f"""
CREATE TABLE IF NOT EXISTS {T('fact_management_target')} (
  target_sk                STRING    NOT NULL OPTIONS(description="MD5 surrogate key: brand_group+customer_id+calendar_date+brand"),
  outlet_sk                STRING             OPTIONS(description="FK to dim_outlet — NULL if customer_id not in dim_outlet"),
  source_customer_id       STRING    NOT NULL,
  calendar_date            DATE      NOT NULL,
  brand                    STRING    NOT NULL OPTIONS(description="Skintific | Timephoria | Glad2Glow"),
  brand_group              STRING             OPTIONS(description="SKT | G2G — derived from brand"),
  management_target_amount FLOAT64            OPTIONS(description="Monthly sell-in target in IDR"),
  weekly_visit_target      INT64              OPTIONS(description="Target visit count per week"),
  region                   STRING,
  distributor_name         STRING,
  spv_name                 STRING,
  asm_name                 STRING,
  source_loaded_at         TIMESTAMP,
  sfa_web_loaded_at        TIMESTAMP NOT NULL,
  is_deleted               BOOL      NOT NULL,
  PRIMARY KEY (target_sk) NOT ENFORCED
)
PARTITION BY calendar_date
CLUSTER BY brand_group, region, brand
OPTIONS (description = "Monthly management targets per outlet per brand. Source: gt_schema.fact_gt_target_v2_t (unpivoted).")
""", "CREATE fact_management_target")

print("\nMigration complete.")
