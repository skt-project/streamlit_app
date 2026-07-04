"""
Migration: create SFA visit/stock/SKU tables in sfa_web.
Run from backend/ directory:

    $env:BQ_SA_KEY_PATH = "D:\Claude\bq-sfa-web-api.json"
    python migrate_visit_tables.py

Safe to re-run — uses CREATE TABLE IF NOT EXISTS and ADD COLUMN IF NOT EXISTS.
Writes ONLY to sfa_web. Does NOT touch any source dataset.
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


# ------------------------------------------------------------------
# fact_visit
# ------------------------------------------------------------------
print("\n[1] fact_visit")
run(f"""
CREATE TABLE IF NOT EXISTS {T('fact_visit')} (
  visit_id               STRING    NOT NULL OPTIONS(description="VST_ prefixed UUID"),
  salesman_sk            STRING    NOT NULL OPTIONS(description="FK dim_salesman"),
  outlet_sk              STRING             OPTIONS(description="FK dim_outlet"),
  schedule_id            STRING             OPTIONS(description="FK fact_route_plan_pjp.route_plan_sk"),

  visit_date             DATE      NOT NULL,
  visit_type             STRING    NOT NULL OPTIONS(description="ROUTE | NON_ROUTE"),
  brand_group            STRING             OPTIONS(description="SKT | G2G — inherited from salesman"),

  checkin_time           TIMESTAMP,
  checkin_latitude       FLOAT64,
  checkin_longitude      FLOAT64,
  checkin_photo_url      STRING,
  checkin_distance_m     FLOAT64            OPTIONS(description="Meters from outlet coords at check-in"),

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
  revision_count         INT64              OPTIONS(description="Increments on each REVISION_REQUIRED cycle"),

  created_at             TIMESTAMP NOT NULL,
  updated_at             TIMESTAMP NOT NULL,
  is_deleted             BOOL      NOT NULL,
  PRIMARY KEY (visit_id) NOT ENFORCED
)
PARTITION BY visit_date
CLUSTER BY brand_group, salesman_sk
OPTIONS (description = "Field visit records. One row per store visit by a salesman.")
""", "CREATE fact_visit")

# ------------------------------------------------------------------
# fact_visit_item
# ------------------------------------------------------------------
print("\n[2] fact_visit_item")
run(f"""
CREATE TABLE IF NOT EXISTS {T('fact_visit_item')} (
  visit_item_id   STRING    NOT NULL OPTIONS(description="VTI_ prefixed UUID"),
  visit_id        STRING    NOT NULL OPTIONS(description="FK fact_visit"),
  sku_id          STRING    NOT NULL OPTIONS(description="FK dim_sku"),
  sku_name        STRING,
  brand           STRING,
  brand_group     STRING,
  category        STRING,
  stp             FLOAT64            OPTIONS(description="Selling price at time of visit"),
  qty             INT64,
  demand          FLOAT64            OPTIONS(description="qty * stp"),
  created_at      TIMESTAMP NOT NULL,
  PRIMARY KEY (visit_item_id) NOT ENFORCED
)
PARTITION BY DATE(created_at)
CLUSTER BY visit_id
OPTIONS (description = "SKU line items for each visit. One row per product entered during survey.")
""", "CREATE fact_visit_item")

# ------------------------------------------------------------------
# fact_visit_revision
# ------------------------------------------------------------------
print("\n[3] fact_visit_revision")
run(f"""
CREATE TABLE IF NOT EXISTS {T('fact_visit_revision')} (
  revision_id     STRING    NOT NULL,
  visit_id        STRING    NOT NULL OPTIONS(description="FK fact_visit"),
  revised_by      STRING    NOT NULL OPTIONS(description="username of who made the change"),
  revised_at      TIMESTAMP NOT NULL,
  field_name      STRING,
  old_value       STRING,
  new_value       STRING,
  revision_reason STRING,
  PRIMARY KEY (revision_id) NOT ENFORCED
)
PARTITION BY DATE(revised_at)
CLUSTER BY visit_id
OPTIONS (description = "Audit trail for visit revisions and supervisor modifications.")
""", "CREATE fact_visit_revision")

# ------------------------------------------------------------------
# dim_sku
# ------------------------------------------------------------------
print("\n[4] dim_sku")
run(f"""
CREATE TABLE IF NOT EXISTS {T('dim_sku')} (
  sku_id            STRING    NOT NULL,
  sku_name          STRING    NOT NULL,
  brand             STRING,
  brand_group       STRING             OPTIONS(description="SKT | G2G"),
  category          STRING,
  stp               FLOAT64            OPTIONS(description="Standard selling price (IDR)"),
  is_active         BOOL,
  source_updated_at TIMESTAMP,
  sfa_web_loaded_at TIMESTAMP NOT NULL,
  is_deleted        BOOL      NOT NULL,
  PRIMARY KEY (sku_id) NOT ENFORCED
)
CLUSTER BY brand_group, brand
OPTIONS (description = "Product master. Source: manually managed or migrated from sfa.master_sku.")
""", "CREATE dim_sku")

# ------------------------------------------------------------------
# fact_salesman_stock
# ------------------------------------------------------------------
print("\n[5] fact_salesman_stock")
run(f"""
CREATE TABLE IF NOT EXISTS {T('fact_salesman_stock')} (
  stock_id         STRING    NOT NULL,
  salesman_sk      STRING    NOT NULL OPTIONS(description="FK dim_salesman"),
  sku_id           STRING    NOT NULL OPTIONS(description="FK dim_sku"),
  sku_name         STRING,
  brand            STRING,
  brand_group      STRING,
  stp              FLOAT64,
  qty_current      INT64     NOT NULL,
  assigned_by_sk   STRING             OPTIONS(description="SPV salesman_sk who assigned this stock"),
  updated_at       TIMESTAMP NOT NULL,
  PRIMARY KEY (stock_id) NOT ENFORCED
)
CLUSTER BY salesman_sk
OPTIONS (description = "Current stock held by each SE. Updated on assignment and stock requests.")
""", "CREATE fact_salesman_stock")

# ------------------------------------------------------------------
# fact_stock_request
# ------------------------------------------------------------------
print("\n[6] fact_stock_request")
run(f"""
CREATE TABLE IF NOT EXISTS {T('fact_stock_request')} (
  request_id      STRING    NOT NULL,
  salesman_sk     STRING    NOT NULL OPTIONS(description="SE requesting stock — FK dim_salesman"),
  spv_sk          STRING    NOT NULL OPTIONS(description="SPV to approve — FK dim_salesman"),
  sku_id          STRING    NOT NULL OPTIONS(description="FK dim_sku"),
  sku_name        STRING,
  qty_requested   INT64     NOT NULL,
  qty_approved    INT64,
  status          STRING    NOT NULL OPTIONS(description="PENDING | APPROVED | REVISED | REJECTED"),
  notes_se        STRING,
  notes_spv       STRING,
  created_at      TIMESTAMP NOT NULL,
  updated_at      TIMESTAMP NOT NULL,
  PRIMARY KEY (request_id) NOT ENFORCED
)
PARTITION BY DATE(created_at)
CLUSTER BY spv_sk, status
OPTIONS (description = "SE stock requests pending SPV approval.")
""", "CREATE fact_stock_request")

# ------------------------------------------------------------------
# ALTER users table — add salesman_sk + supervisor_username
# ------------------------------------------------------------------
print("\n[7] users — add columns")
run(f"""
ALTER TABLE {T('users')}
ADD COLUMN IF NOT EXISTS salesman_sk STRING
OPTIONS(description='Links login user to dim_salesman row')
""", "ADD users.salesman_sk")

run(f"""
ALTER TABLE {T('users')}
ADD COLUMN IF NOT EXISTS supervisor_username STRING
OPTIONS(description='Username of direct supervisor (for SE -> SPV chain)')
""", "ADD users.supervisor_username")

run(f"""
ALTER TABLE {T('users')}
ADD COLUMN IF NOT EXISTS sfa_role STRING
OPTIONS(description='SE | SPV | ASM | DDM — field operations role (separate from STEP role)')
""", "ADD users.sfa_role")

# ------------------------------------------------------------------
# Row counts
# ------------------------------------------------------------------
print("\n[OK] Migration complete. Current table row counts:")
tables = [
    "fact_visit", "fact_visit_item", "fact_visit_revision",
    "dim_sku", "fact_salesman_stock", "fact_stock_request",
]
for tbl in tables:
    try:
        row = list(client.query(f"SELECT COUNT(*) AS n FROM {T(tbl)}").result())[0]
        print(f"    {tbl:<30} {row['n']:,} rows")
    except Exception as e:
        print(f"    {tbl:<30} ERROR: {e}")
