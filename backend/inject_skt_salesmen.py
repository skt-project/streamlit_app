"""
Inject Skintific Group salesman data from Excel into sfa_web.dim_salesman.
Source: D:\Claude\# 2026 - Building Block (Sales Monitoring - National)(5).xlsx
Sheet: Rawdata (2)

Run from backend/ directory:

    python inject_skt_salesmen.py

Uses BQ_SA_KEY_PATH env var.
All injected rows get brand_group='SKT', source_system='SKT_EXCEL'.
Existing SKT_EXCEL rows are removed and re-inserted (idempotent).
"""
import base64, hashlib, json, os, sys
from collections import Counter, defaultdict

# ------------------------------------------------------------------
# Read Excel
# ------------------------------------------------------------------
try:
    import openpyxl
except ImportError:
    print("Installing openpyxl...")
    os.system("pip install openpyxl -q")
    import openpyxl

EXCEL_PATH = r"D:\Claude\# 2026 - Building Block (Sales Monitoring - National)(5).xlsx"
print(f"Reading {EXCEL_PATH}...")

wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True, data_only=True)
ws = wb["Rawdata (2)"]

# Columns: region, distributor, po_number, brand, order_date, store_code, store_name, reference_id, ASS, SALESMAN, Value, Mark
salesman_data: dict[str, dict] = defaultdict(lambda: {
    "regions": Counter(),
    "distributors": Counter(),
    "spvs": Counter(),
})

for row in ws.iter_rows(min_row=2, values_only=True):
    if not row or not row[9]:
        continue
    salesman = str(row[9]).strip()
    if not salesman or salesman in ("0", "NON PJP", "(blank)"):
        continue

    region     = str(row[0]).strip().upper() if row[0] else ""
    distributor = str(row[1]).strip() if row[1] else ""
    spv        = str(row[8]).strip().upper() if row[8] else ""

    salesman_data[salesman]["regions"][region] += 1
    salesman_data[salesman]["distributors"][distributor] += 1
    salesman_data[salesman]["spvs"][spv] += 1

print(f"Found {len(salesman_data)} unique SKT salesmen.\n")

# Build clean rows — pick most frequent region / distributor / spv per salesman
rows_to_insert = []
for name, info in salesman_data.items():
    region = info["regions"].most_common(1)[0][0] if info["regions"] else ""
    distributor = info["distributors"].most_common(1)[0][0] if info["distributors"] else ""
    spv = info["spvs"].most_common(1)[0][0] if info["spvs"] else ""

    salesman_sk = hashlib.md5(f"SKT_EXCEL|{name}".encode()).hexdigest()

    rows_to_insert.append({
        "salesman_sk":          salesman_sk,
        "source_system":        "SKT_EXCEL",
        "source_salesman_code": name,
        "salesman_name":        name,
        "salesman_type":        "GT",
        "role_type":            "SALESMAN",
        "distributor_code":     None,
        "distributor_name":     distributor,
        "region":               region,
        "spv_name":             spv,
        "asm_name":             None,
        "is_active":            True,
        "brand_group":          "SKT",
    })

# ------------------------------------------------------------------
# BigQuery
# ------------------------------------------------------------------
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

import datetime, io, tempfile

now = datetime.datetime.now(datetime.timezone.utc).isoformat()

# Build JSON lines for load job
import json as _json
lines = []
for r in rows_to_insert:
    lines.append(_json.dumps({
        "salesman_sk":          r["salesman_sk"],
        "source_system":        r["source_system"],
        "source_salesman_code": r["source_salesman_code"],
        "salesman_name":        r["salesman_name"],
        "salesman_type":        r["salesman_type"],
        "role_type":            r["role_type"],
        "distributor_code":     r["distributor_code"],
        "region":               r["region"],
        "spv_name":             r["spv_name"],
        "asm_name":             r["asm_name"],
        "is_active":            r["is_active"],
        "brand_group":          r["brand_group"],
        "sfa_web_loaded_at":    now,
        "is_deleted":           False,
    }))

# Load into a staging table first (load job = no streaming buffer)
STAGING = f"{PROJECT}.{DATASET}.skt_salesman_staging"
print("Loading into staging table...")
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    autodetect=False,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("salesman_sk", "STRING"),
        bigquery.SchemaField("source_system", "STRING"),
        bigquery.SchemaField("source_salesman_code", "STRING"),
        bigquery.SchemaField("salesman_name", "STRING"),
        bigquery.SchemaField("salesman_type", "STRING"),
        bigquery.SchemaField("role_type", "STRING"),
        bigquery.SchemaField("distributor_code", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("spv_name", "STRING"),
        bigquery.SchemaField("asm_name", "STRING"),
        bigquery.SchemaField("is_active", "BOOL"),
        bigquery.SchemaField("brand_group", "STRING"),
        bigquery.SchemaField("sfa_web_loaded_at", "TIMESTAMP"),
        bigquery.SchemaField("is_deleted", "BOOL"),
    ],
)
buf = io.BytesIO("\n".join(lines).encode())
job = client.load_table_from_file(buf, STAGING, job_config=job_config)
job.result()
print(f"  Staging loaded: {job.output_rows} rows")

# Now DELETE existing + INSERT from staging (load job rows have no streaming buffer)
print("Removing existing SKT_EXCEL rows from dim_salesman...")
client.query(
    f"DELETE FROM {T('dim_salesman')} WHERE source_system = 'SKT_EXCEL'"
).result()

print(f"Inserting {len(rows_to_insert)} SKT salesmen from staging...")
client.query(f"""
    INSERT INTO {T('dim_salesman')}
      (salesman_sk, source_system, source_salesman_code, salesman_name, salesman_type,
       role_type, distributor_code, region, spv_name, asm_name, is_active,
       brand_group, sfa_web_loaded_at, is_deleted)
    SELECT
      salesman_sk, source_system, source_salesman_code, salesman_name, salesman_type,
      role_type, distributor_code, region, spv_name, asm_name, is_active,
      brand_group, sfa_web_loaded_at, is_deleted
    FROM `{STAGING}`
""").result()

# Drop staging
client.query(f"DROP TABLE IF EXISTS `{STAGING}`").result()

# Verify
row = list(client.query(f"""
    SELECT COUNT(*) AS n FROM {T('dim_salesman')}
    WHERE source_system = 'SKT_EXCEL'
""").result())[0]
print(f"\n✓ {row['n']} SKT salesmen in dim_salesman.")

total = list(client.query(f"SELECT COUNT(*) AS n FROM {T('dim_salesman')}").result())[0]
print(f"  Total dim_salesman: {total['n']:,} rows (G2G + SKT)")

# Print sample
print("\nSample SKT salesmen:")
sample = list(client.query(f"""
    SELECT salesman_name, region, spv_name
    FROM {T('dim_salesman')}
    WHERE source_system = 'SKT_EXCEL'
    ORDER BY region, salesman_name
    LIMIT 10
""").result())
for s in sample:
    print(f"  {s['salesman_name']:<35} {s['region']:<25} spv={s['spv_name']}")
