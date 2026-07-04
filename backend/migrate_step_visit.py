"""
Create step_visit, step_visit_item, step_visit_revision tables.
These replace the logical 'fact_visit*' names that already exist
with a different source-sync schema.

    $env:BQ_SA_KEY_PATH = "D:\Claude\bq-sfa-web-api.json"
    python migrate_step_visit.py
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
            sys.exit(1)


print("\n[1] step_visit")
run(f"""
CREATE TABLE IF NOT EXISTS {T('step_visit')} (
  visit_id               STRING    NOT NULL OPTIONS(description="VST_ prefixed UUID"),
  salesman_sk            STRING    NOT NULL OPTIONS(description="FK dim_salesman"),
  outlet_sk              STRING             OPTIONS(description="FK dim_outlet"),
  schedule_id            STRING             OPTIONS(description="FK fact_route_plan_pjp.route_plan_sk"),
  visit_date             DATE      NOT NULL,
  visit_type             STRING    NOT NULL OPTIONS(description="ROUTE | NON_ROUTE"),
  brand_group            STRING             OPTIONS(description="SKT | G2G"),
  checkin_time           TIMESTAMP,
  checkin_latitude       FLOAT64,
  checkin_longitude      FLOAT64,
  checkin_photo_url      STRING,
  checkin_distance_m     FLOAT64            OPTIONS(description="Meters from outlet coords at check-in. Informational only."),
  checkout_time          TIMESTAMP,
  checkout_latitude      FLOAT64,
  checkout_longitude     FLOAT64,
  checkout_photo_url     STRING,
  total_demand           FLOAT64,
  effective_call         STRING             OPTIONS(description="YES | NO"),
  notes                  STRING,
  duration_minutes       INT64,
  visit_status           STRING             OPTIONS(description="CHECKED_IN | CHECKED_OUT | SUBMITTED"),
  approval_status        STRING             OPTIONS(description="DRAFT | SUBMITTED | PENDING_SPV | SPV_APPROVED | ASM_APPROVED | DDM_APPROVED | REVISION_REQUIRED | COMPLETED | REJECTED"),
  spv_username           STRING,
  spv_approved_at        TIMESTAMP,
  asm_username           STRING,
  asm_approved_at        TIMESTAMP,
  ddm_username           STRING,
  ddm_approved_at        TIMESTAMP,
  rejection_notes        STRING,
  revision_count         INT64,
  created_at             TIMESTAMP NOT NULL,
  updated_at             TIMESTAMP NOT NULL,
  is_deleted             BOOL      NOT NULL,
  PRIMARY KEY (visit_id) NOT ENFORCED
)
PARTITION BY visit_date
CLUSTER BY brand_group, salesman_sk
OPTIONS (description = "SFA field visit records. One row per store visit by a salesman.")
""", "CREATE step_visit")

print("\n[2] step_visit_item")
run(f"""
CREATE TABLE IF NOT EXISTS {T('step_visit_item')} (
  visit_item_id   STRING    NOT NULL,
  visit_id        STRING    NOT NULL OPTIONS(description="FK step_visit"),
  sku_id          STRING    NOT NULL OPTIONS(description="FK dim_sku"),
  sku_name        STRING,
  brand           STRING,
  brand_group     STRING,
  category        STRING,
  stp             FLOAT64,
  qty             INT64,
  demand          FLOAT64            OPTIONS(description="qty * stp"),
  created_at      TIMESTAMP NOT NULL,
  PRIMARY KEY (visit_item_id) NOT ENFORCED
)
PARTITION BY DATE(created_at)
CLUSTER BY visit_id
OPTIONS (description = "SKU line items for each SFA visit.")
""", "CREATE step_visit_item")

print("\n[3] step_visit_revision")
run(f"""
CREATE TABLE IF NOT EXISTS {T('step_visit_revision')} (
  revision_id     STRING    NOT NULL,
  visit_id        STRING    NOT NULL OPTIONS(description="FK step_visit"),
  revised_by      STRING    NOT NULL,
  revised_at      TIMESTAMP NOT NULL,
  field_name      STRING,
  old_value       STRING,
  new_value       STRING,
  revision_reason STRING,
  PRIMARY KEY (revision_id) NOT ENFORCED
)
PARTITION BY DATE(revised_at)
CLUSTER BY visit_id
OPTIONS (description = "Audit trail for SFA visit revisions.")
""", "CREATE step_visit_revision")

print("\n[OK] Done. Table row counts:")
for tbl in ["step_visit", "step_visit_item", "step_visit_revision"]:
    try:
        row = list(client.query(f"SELECT COUNT(*) AS n FROM {T(tbl)}").result())[0]
        print(f"    {tbl:<30} {row['n']:,} rows")
    except Exception as e:
        print(f"    {tbl:<30} ERROR: {e}")
