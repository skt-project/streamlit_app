"""
Bulk import / export / template download.

GET  /template/{entity}       — CSV column-header template (pjp|salesman|outlet|target)
POST /import/salesman         — MERGE dim_salesman from CSV
POST /import/outlet           — MERGE dim_outlet from CSV
POST /import/target           — upsert spv_target from CSV
GET  /export/pjp              — PJP list as CSV
GET  /export/salesman         — dim_salesman as CSV
GET  /export/outlet           — dim_outlet as CSV
GET  /export/route-compliance — route compliance MTD as CSV
GET  /export/achievement      — achievement vs target (current month) as CSV
GET  /export/visits           — visit log MTD as CSV
"""
import csv
import io
from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from config import settings
from dependencies import require_auth, require_role
from models.auth import UserContext
from services.audit import log_event
from services.bq import BQClient

router = APIRouter(tags=["import_export"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"
CHUNK = 500  # max rows per BigQuery MERGE


# ── Helpers ──────────────────────────────────────────────────────────────────

def _parse_csv(content: bytes) -> list[dict]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise HTTPException(status_code=422, detail="Empty or invalid CSV.")
    reader.fieldnames = [h.strip().lower().replace(" ", "_") for h in reader.fieldnames]
    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=422, detail="CSV has no data rows.")
    return rows


def _check_required(row: dict, required: set[str]) -> None:
    missing = required - set(row.keys())
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing required columns: {sorted(missing)}")


def _str_lit(v: Any) -> str:
    """Escape a value as a BigQuery double-quoted string literal."""
    s = str(v or "").replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").replace("\r", "")
    return f'"{s}"'


def _float_val(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v or default).strip())
    except (ValueError, TypeError):
        return default


def _csv_response(rows: list[dict], filename: str) -> StreamingResponse:
    if not rows:
        raise HTTPException(status_code=404, detail="No data found for export.")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow({k: ("" if v is None else str(v)) for k, v in row.items()})
    output.seek(0)
    encoded = output.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        iter([encoded]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Templates ────────────────────────────────────────────────────────────────

_TEMPLATES: dict[str, list[str]] = {
    "pjp":      ["salesman_sk", "outlet_sk", "visit_day_of_week", "week_number", "visit_frequency", "brand_group"],
    "salesman": ["source_salesman_code", "salesman_name", "salesman_type", "distributor_code", "region", "brand_group", "spv_name", "asm_name"],
    "outlet":   ["source_outlet_code", "store_name", "store_grade", "channel", "kecamatan", "city", "latitude", "longitude"],
    "target":   ["salesman_code", "brand", "period_month", "management_target", "spv_target"],
}


@router.get("/template/{entity}")
def download_template(entity: str, current_user: UserContext = Depends(require_auth)):
    cols = _TEMPLATES.get(entity)
    if not cols:
        raise HTTPException(status_code=404, detail=f"No template available for '{entity}'.")
    output = io.StringIO()
    csv.writer(output).writerow(cols)
    output.seek(0)
    encoded = output.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        iter([encoded]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="template-{entity}.csv"'},
    )


# ── Import: salesman ─────────────────────────────────────────────────────────

@router.post("/import/salesman")
def import_salesman(
    file: UploadFile = File(...),
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Only CSV files are accepted. Convert from Excel first.")

    rows = _parse_csv(file.file.read())
    if len(rows) > 10_000:
        raise HTTPException(status_code=422, detail="Maximum 10,000 rows per upload.")
    _check_required(rows[0], {"source_salesman_code", "salesman_name", "salesman_type"})

    bq = BQClient.get()
    max_sk_row = bq.query_one(f"SELECT COALESCE(MAX(salesman_sk), 0) AS m FROM {SFA_WEB}.dim_salesman") or {}
    max_sk = int(max_sk_row.get("m", 0) or 0)

    errors: list[str] = []
    processed = 0

    for chunk_start in range(0, len(rows), CHUNK):
        chunk = rows[chunk_start:chunk_start + CHUNK]
        struct_vals: list[str] = []

        for j, r in enumerate(chunk):
            code = r.get("source_salesman_code", "").strip()
            name = r.get("salesman_name", "").strip()
            if not code or not name:
                errors.append(f"Row {chunk_start + j + 2}: source_salesman_code and salesman_name are required")
                continue
            new_sk = max_sk + chunk_start + j + 1
            struct_vals.append(
                f"({_str_lit(code)}, {_str_lit(name)}, "
                f"{_str_lit(r.get('salesman_type', ''))}, "
                f"{_str_lit(r.get('distributor_code', ''))}, "
                f"{_str_lit(r.get('region', ''))}, "
                f"{_str_lit(r.get('brand_group', ''))}, "
                f"{_str_lit(r.get('spv_name', ''))}, "
                f"{_str_lit(r.get('asm_name', ''))}, "
                f"{new_sk})"
            )

        if not struct_vals:
            continue

        struct_type = (
            "STRUCT<source_salesman_code STRING, salesman_name STRING, salesman_type STRING, "
            "distributor_code STRING, region STRING, brand_group STRING, "
            "spv_name STRING, asm_name STRING, new_sk INT64>"
        )
        bq.execute(f"""
        MERGE {SFA_WEB}.dim_salesman t
        USING (SELECT * FROM UNNEST(ARRAY<{struct_type}>[{", ".join(struct_vals)}])) s
        ON t.source_salesman_code = s.source_salesman_code
        WHEN MATCHED THEN UPDATE SET
          salesman_name    = s.salesman_name,
          salesman_type    = s.salesman_type,
          distributor_code = NULLIF(s.distributor_code, ""),
          region           = NULLIF(s.region, ""),
          brand_group      = NULLIF(s.brand_group, ""),
          spv_name         = NULLIF(s.spv_name, ""),
          asm_name         = NULLIF(s.asm_name, ""),
          is_active        = TRUE
        WHEN NOT MATCHED THEN INSERT
          (salesman_sk, source_salesman_code, salesman_name, salesman_type,
           distributor_code, region, brand_group, spv_name, asm_name, is_active)
        VALUES
          (s.new_sk, s.source_salesman_code, s.salesman_name, NULLIF(s.salesman_type, ""),
           NULLIF(s.distributor_code, ""), NULLIF(s.region, ""), NULLIF(s.brand_group, ""),
           NULLIF(s.spv_name, ""), NULLIF(s.asm_name, ""), TRUE)
        """)
        processed += len(struct_vals)

    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors[:20]})

    bq.cache.invalidate("salesman:")
    bq.cache.invalidate("salesman-search:")
    log_event("import.salesman", "dim_salesman", "", current_user.username,
              payload={"rows": processed, "filename": file.filename})
    return {"processed": processed, "message": f"Imported {processed} salesman records."}


# ── Import: outlet ────────────────────────────────────────────────────────────

@router.post("/import/outlet")
def import_outlet(
    file: UploadFile = File(...),
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Only CSV files are accepted. Convert from Excel first.")

    rows = _parse_csv(file.file.read())
    if len(rows) > 20_000:
        raise HTTPException(status_code=422, detail="Maximum 20,000 rows per upload.")
    _check_required(rows[0], {"source_outlet_code", "store_name"})

    bq = BQClient.get()
    max_sk_row = bq.query_one(f"SELECT COALESCE(MAX(outlet_sk), 0) AS m FROM {SFA_WEB}.dim_outlet") or {}
    max_sk = int(max_sk_row.get("m", 0) or 0)

    errors: list[str] = []
    processed = 0

    for chunk_start in range(0, len(rows), CHUNK):
        chunk = rows[chunk_start:chunk_start + CHUNK]
        struct_vals: list[str] = []

        for j, r in enumerate(chunk):
            code = r.get("source_outlet_code", "").strip()
            name = r.get("store_name", "").strip()
            if not code or not name:
                errors.append(f"Row {chunk_start + j + 2}: source_outlet_code and store_name are required")
                continue
            lat = _float_val(r.get("latitude"), 0.0)
            lon = _float_val(r.get("longitude"), 0.0)
            new_sk = max_sk + chunk_start + j + 1
            struct_vals.append(
                f"({_str_lit(code)}, {_str_lit(name)}, "
                f"{_str_lit(r.get('store_grade', ''))}, "
                f"{_str_lit(r.get('channel', ''))}, "
                f"{_str_lit(r.get('kecamatan', ''))}, "
                f"{_str_lit(r.get('city', ''))}, "
                f"{lat}, {lon}, {new_sk})"
            )

        if not struct_vals:
            continue

        struct_type = (
            "STRUCT<source_outlet_code STRING, store_name STRING, store_grade STRING, "
            "channel STRING, kecamatan STRING, city STRING, "
            "latitude FLOAT64, longitude FLOAT64, new_sk INT64>"
        )
        bq.execute(f"""
        MERGE {SFA_WEB}.dim_outlet t
        USING (SELECT * FROM UNNEST(ARRAY<{struct_type}>[{", ".join(struct_vals)}])) s
        ON t.source_outlet_code = s.source_outlet_code
        WHEN MATCHED THEN UPDATE SET
          store_name  = s.store_name,
          store_grade = NULLIF(s.store_grade, ""),
          channel     = NULLIF(s.channel, ""),
          kecamatan   = NULLIF(s.kecamatan, ""),
          city        = NULLIF(s.city, ""),
          latitude    = IF(s.latitude = 0.0, latitude, s.latitude),
          longitude   = IF(s.longitude = 0.0, longitude, s.longitude),
          is_active   = TRUE
        WHEN NOT MATCHED THEN INSERT
          (outlet_sk, source_outlet_code, store_name, store_grade,
           channel, kecamatan, city, latitude, longitude, is_active)
        VALUES
          (s.new_sk, s.source_outlet_code, s.store_name, NULLIF(s.store_grade, ""),
           NULLIF(s.channel, ""), NULLIF(s.kecamatan, ""), NULLIF(s.city, ""),
           IF(s.latitude = 0.0, NULL, s.latitude), IF(s.longitude = 0.0, NULL, s.longitude), TRUE)
        """)
        processed += len(struct_vals)

    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors[:20]})

    bq.cache.invalidate("outlet-search:")
    bq.cache.invalidate("route:")
    bq.cache.invalidate("route-planner:")
    log_event("import.outlet", "dim_outlet", "", current_user.username,
              payload={"rows": processed, "filename": file.filename})
    return {"processed": processed, "message": f"Imported {processed} outlet records."}


# ── Import: target ────────────────────────────────────────────────────────────

@router.post("/import/target")
def import_target(
    file: UploadFile = File(...),
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Only CSV files are accepted. Convert from Excel first.")

    rows = _parse_csv(file.file.read())
    if len(rows) > 5_000:
        raise HTTPException(status_code=422, detail="Maximum 5,000 rows per upload.")
    _check_required(rows[0], {"salesman_code", "brand", "period_month", "management_target"})

    bq = BQClient.get()

    # Pre-fetch salesman code → SK mapping (single query, in-memory lookup)
    all_sm = bq.query(f"SELECT source_salesman_code, salesman_sk FROM {SFA_WEB}.dim_salesman WHERE is_active = TRUE")
    code_to_sk: dict[str, int] = {r["source_salesman_code"]: int(r["salesman_sk"]) for r in all_sm}

    errors: list[str] = []
    processed = 0
    username = current_user.username

    for chunk_start in range(0, len(rows), CHUNK):
        chunk = rows[chunk_start:chunk_start + CHUNK]
        struct_vals: list[str] = []

        for j, r in enumerate(chunk):
            sm_code = r.get("salesman_code", "").strip()
            brand   = r.get("brand", "").strip()
            pm_raw  = r.get("period_month", "").strip()
            mgmt    = _float_val(r.get("management_target"), 0.0)
            spv_t   = _float_val(r.get("spv_target"), 0.0)

            if not sm_code or not brand or not pm_raw:
                errors.append(f"Row {chunk_start + j + 2}: salesman_code, brand, period_month are required")
                continue

            pm_parts = pm_raw.replace("/", "-").split("-")
            if len(pm_parts) < 2:
                errors.append(f"Row {chunk_start + j + 2}: invalid period_month (expected YYYY-MM or YYYY-MM-01)")
                continue
            pm = f"{pm_parts[0]}-{pm_parts[1].zfill(2)}-01"

            sk = code_to_sk.get(sm_code)
            if sk is None:
                errors.append(f"Row {chunk_start + j + 2}: salesman_code '{sm_code}' not found")
                continue

            struct_vals.append(f"({sk}, {_str_lit(brand)}, {_str_lit(pm)}, {mgmt}, {spv_t})")

        if not struct_vals:
            continue

        struct_type = (
            "STRUCT<salesman_sk INT64, brand STRING, period_month STRING, "
            "management_target FLOAT64, spv_target FLOAT64>"
        )
        bq.execute(f"""
        MERGE {SFA_WEB}.spv_target t
        USING (SELECT * FROM UNNEST(ARRAY<{struct_type}>[{", ".join(struct_vals)}])) s
        ON t.salesman_sk = s.salesman_sk
          AND t.brand = s.brand
          AND DATE_TRUNC(t.period_month, MONTH) = DATE_TRUNC(DATE(s.period_month), MONTH)
          AND t.is_deleted = FALSE
        WHEN MATCHED THEN UPDATE SET
          management_target = s.management_target,
          spv_target        = s.spv_target,
          updated_at        = CURRENT_TIMESTAMP(),
          updated_by        = {_str_lit(username)}
        WHEN NOT MATCHED THEN INSERT
          (spv_target_id, salesman_sk, brand, period_month, management_target,
           spv_target, approval_status, created_by, created_at, updated_at, updated_by, is_deleted)
        VALUES
          (GENERATE_UUID(), s.salesman_sk, s.brand, DATE(s.period_month),
           s.management_target, s.spv_target, 'draft',
           {_str_lit(username)}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
           {_str_lit(username)}, FALSE)
        """)
        processed += len(struct_vals)

    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors[:20]})

    bq.cache.invalidate("target:")
    bq.cache.invalidate("dashboard:comply:")
    log_event("import.target", "spv_target", "", current_user.username,
              payload={"rows": processed, "filename": file.filename})
    return {"processed": processed, "message": f"Imported {processed} target records."}


# ── Exports ───────────────────────────────────────────────────────────────────

@router.get("/export/pjp")
def export_pjp(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    rows = bq.query(f"""
    SELECT
      o.source_outlet_code,
      o.store_name,
      sm.source_salesman_code AS salesman_code,
      sm.salesman_name,
      p.visit_day_of_week,
      p.visit_frequency_code,
      p.visit_week_pattern,
      p.brand_group
    FROM {SFA_WEB}.fact_route_plan_pjp p
    JOIN {SFA_WEB}.dim_outlet o USING (outlet_sk)
    JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
    WHERE p.is_deleted = FALSE
    ORDER BY sm.salesman_name, o.store_name
    LIMIT 200000
    """)
    return _csv_response(rows, "pjp-efektif.csv")


@router.get("/export/salesman")
def export_salesman(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    rows = bq.query(f"""
    SELECT
      source_salesman_code, salesman_name, salesman_type,
      distributor_code, region, brand_group,
      spv_name, asm_name,
      CAST(is_active AS STRING) AS is_active
    FROM {SFA_WEB}.dim_salesman
    ORDER BY salesman_name
    """)
    return _csv_response(rows, "master-salesman.csv")


@router.get("/export/outlet")
def export_outlet(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    rows = bq.query(f"""
    SELECT
      o.source_outlet_code,
      o.store_name,
      o.store_grade AS tier,
      o.channel,
      o.kecamatan,
      o.city,
      o.latitude,
      o.longitude,
      sm.source_salesman_code AS default_salesman_code,
      CAST(o.is_active AS STRING) AS is_active
    FROM {SFA_WEB}.dim_outlet o
    LEFT JOIN {SFA_WEB}.dim_salesman sm ON sm.salesman_sk = o.default_salesman_sk
    ORDER BY o.store_name
    """)
    return _csv_response(rows, "master-outlet.csv")


@router.get("/export/route-compliance")
def export_route_compliance(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    today       = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()
    rows = bq.query(
        f"""
        SELECT
          sm.source_salesman_code,
          sm.salesman_name,
          sm.region,
          sm.distributor_code,
          COUNT(DISTINCT p.outlet_sk)                                              AS planned_stores,
          COUNT(DISTINCT CASE WHEN v.visit_date IS NOT NULL THEN v.outlet_sk END)  AS visited_stores,
          ROUND(
            SAFE_DIVIDE(
              COUNT(DISTINCT CASE WHEN v.visit_date IS NOT NULL THEN v.outlet_sk END),
              NULLIF(COUNT(DISTINCT p.outlet_sk), 0)
            ) * 100, 1
          )                                                                         AS comply_pct,
          COUNT(v.visit_id)                                                         AS total_visits,
          COUNTIF(v.effective_call = 'YES')                                         AS effective_calls
        FROM {SFA_WEB}.dim_salesman sm
        LEFT JOIN {SFA_WEB}.fact_route_plan_pjp p
          ON p.salesman_sk = sm.salesman_sk AND p.is_deleted = FALSE
        LEFT JOIN {settings.table('fact_visit')} v
          ON v.salesman_sk = sm.salesman_sk AND v.is_deleted = FALSE
          AND v.visit_date BETWEEN @ms AND @today
        WHERE sm.is_active = TRUE
        GROUP BY sm.source_salesman_code, sm.salesman_name, sm.region, sm.distributor_code
        ORDER BY sm.salesman_name
        """,
        [bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
    )
    return _csv_response(rows, "route-compliance.csv")


@router.get("/export/achievement")
def export_achievement(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    today = date.today().isoformat()
    rows = bq.query(
        f"""
        SELECT
          sm.source_salesman_code,
          sm.salesman_name,
          sm.region,
          sm.brand_group,
          t.brand,
          CAST(DATE_TRUNC(t.period_month, MONTH) AS STRING) AS period_month,
          t.management_target,
          t.spv_target,
          ROUND(SAFE_DIVIDE(t.spv_target, NULLIF(t.management_target, 0)) * 100, 1) AS comply_pct,
          t.approval_status
        FROM {SFA_WEB}.spv_target t
        JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
        WHERE DATE_TRUNC(t.period_month, MONTH) = DATE_TRUNC(DATE(@today), MONTH)
          AND t.is_deleted = FALSE
        ORDER BY sm.salesman_name, t.brand
        """,
        [bq.p("today", "DATE", today)],
    )
    return _csv_response(rows, "achievement.csv")


@router.get("/export/visits")
def export_visits(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    today       = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()
    rows = bq.query(
        f"""
        SELECT
          CAST(v.visit_date AS STRING)    AS visit_date,
          sm.source_salesman_code,
          sm.salesman_name,
          o.source_outlet_code,
          o.store_name,
          o.store_grade                   AS tier,
          CAST(v.checkin_time AS STRING)  AS checkin_time,
          CAST(v.checkout_time AS STRING) AS checkout_time,
          v.effective_call,
          COALESCE(v.total_demand, 0)     AS total_demand,
          v.status
        FROM {settings.table('fact_visit')} v
        JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
        JOIN {SFA_WEB}.dim_outlet o ON o.outlet_sk = v.outlet_sk
        WHERE v.visit_date BETWEEN @ms AND @today AND v.is_deleted = FALSE
        ORDER BY v.visit_date DESC, sm.salesman_name
        LIMIT 100000
        """,
        [bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
    )
    return _csv_response(rows, "visit-log.csv")
