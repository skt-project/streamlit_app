"""
Migration: add brand_group column to sfa_web tables.
Run once from backend/ directory:

    python migrate_brand_group.py
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


def run(sql: str, label: str) -> None:
    print(f"  → {label} ...", end=" ", flush=True)
    try:
        client.query(sql).result()
        print("done")
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            print("already exists, skipping")
        else:
            print(f"FAILED\n    {e}")
            sys.exit(1)


T = lambda name: f"`{PROJECT}.{DATASET}.{name}`"

print("\nAdding brand_group column to sfa_web tables...")

run(f"ALTER TABLE {T('dim_salesman')} ADD COLUMN IF NOT EXISTS brand_group STRING", "dim_salesman.brand_group")
run(f"ALTER TABLE {T('dim_outlet')} ADD COLUMN IF NOT EXISTS brand_group STRING", "dim_outlet.brand_group")
run(f"ALTER TABLE {T('fact_route_plan_pjp')} ADD COLUMN IF NOT EXISTS brand_group STRING", "fact_route_plan_pjp.brand_group")
run(f"ALTER TABLE {T('users')} ADD COLUMN IF NOT EXISTS brand_group STRING", "users.brand_group")

print("\nMigration complete.")
