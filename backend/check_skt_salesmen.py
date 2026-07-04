"""Quick check: how many SKT_EXCEL rows are already in dim_salesman."""
import base64, json, os
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT = "skintific-data-warehouse"
DATASET = "sfa_web"

SA_KEY_PATH = os.getenv("BQ_SA_KEY_PATH", "")
SA_KEY_JSON = os.getenv("BQ_SA_KEY_JSON", "")

if SA_KEY_JSON:
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

rows = list(client.query(f"""
    SELECT source_system, brand_group, COUNT(*) AS n
    FROM {T('dim_salesman')}
    GROUP BY 1, 2
    ORDER BY 1
""").result())

print("\ndim_salesman breakdown:")
total = 0
for r in rows:
    print(f"  {r['source_system']:<20} brand_group={r['brand_group']}  rows={r['n']}")
    total += r['n']
print(f"  {'TOTAL':<20}                    rows={total}")

sample = list(client.query(f"""
    SELECT salesman_name, region, spv_name, brand_group
    FROM {T('dim_salesman')}
    WHERE source_system = 'SKT_EXCEL'
    ORDER BY region, salesman_name
    LIMIT 8
""").result())

if sample:
    print("\nSample SKT salesmen:")
    for s in sample:
        print(f"  {s['salesman_name']:<35} {s['region']:<25} spv={s['spv_name']}")
else:
    print("\nNo SKT_EXCEL rows visible yet (still in streaming buffer).")
