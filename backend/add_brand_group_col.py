"""Add brand_group column to fact_management_target if missing."""
import os
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT = "skintific-data-warehouse"
DATASET = "sfa_web"

SA_KEY_PATH = os.getenv("BQ_SA_KEY_PATH", "")
creds = service_account.Credentials.from_service_account_file(
    SA_KEY_PATH, scopes=["https://www.googleapis.com/auth/bigquery"]
)
client = bigquery.Client(project=PROJECT, credentials=creds)
T = lambda n: f"`{PROJECT}.{DATASET}.{n}`"

sql = f"""ALTER TABLE {T('fact_management_target')}
ADD COLUMN IF NOT EXISTS brand_group STRING
OPTIONS(description='SKT | G2G -- derived from brand')"""

print("Adding brand_group column...", end=" ", flush=True)
client.query(sql).result()
print("done")
