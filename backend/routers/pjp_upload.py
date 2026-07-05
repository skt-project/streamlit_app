"""
POST /pjp/upload   — Upload a CSV file to bulk-insert route plan rows into fact_route_plan_pjp.

Expected CSV columns (case-insensitive):
  salesman_sk, outlet_sk, visit_day_of_week, week_number
  Optional: visit_frequency, brand_group

visit_day_of_week: Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday
week_number: 1-52
"""
import csv
import io
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from config import settings
from dependencies import require_role
from models.auth import UserContext
from services.audit import log_event
from services.bq import BQClient

router = APIRouter(prefix="/pjp", tags=["pjp"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"

VALID_DAYS = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}


def _normalise_header(h: str) -> str:
    return h.strip().lower().replace(" ", "_")


@router.post("/upload")
def upload_pjp(
    file: UploadFile = File(...),
    current_user: UserContext = Depends(require_role("ho_admin")),
):
    """Upload a CSV file to bulk-replace route plan PJP rows."""
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Only CSV files are accepted.")

    content = file.file.read().decode("utf-8-sig")  # strip BOM if present
    reader = csv.DictReader(io.StringIO(content))

    # Normalise headers
    if reader.fieldnames is None:
        raise HTTPException(status_code=422, detail="Empty or invalid CSV.")
    reader.fieldnames = [_normalise_header(h) for h in reader.fieldnames]

    required = {"salesman_sk", "outlet_sk", "visit_day_of_week", "week_number"}
    missing = required - set(reader.fieldnames)
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing columns: {sorted(missing)}")

    rows = list(reader)
    if not rows:
        raise HTTPException(status_code=422, detail="CSV has no data rows.")
    if len(rows) > 50_000:
        raise HTTPException(status_code=422, detail="Maximum 50,000 rows per upload.")

    bq = BQClient.get()
    now = datetime.now(timezone.utc).isoformat()

    errors = []
    insert_rows = []
    for i, row in enumerate(rows, start=2):  # row 1 = header
        day = row.get("visit_day_of_week", "").strip().lower().capitalize()
        if day.lower() not in VALID_DAYS:
            errors.append(f"Row {i}: invalid visit_day_of_week '{day}'")
            continue
        try:
            week = int(row.get("week_number", 0))
            assert 1 <= week <= 53
        except (ValueError, AssertionError):
            errors.append(f"Row {i}: week_number must be 1-53")
            continue

        insert_rows.append({
            "id": str(uuid.uuid4()),
            "salesman_sk": row["salesman_sk"].strip(),
            "outlet_sk": row["outlet_sk"].strip(),
            "day": day,
            "week": week,
            "freq": int(row.get("visit_frequency", 1) or 1),
            "bg": row.get("brand_group", "").strip() or None,
        })

    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors[:20]})

    # Batch insert in chunks of 500
    CHUNK = 500
    inserted = 0
    for i in range(0, len(insert_rows), CHUNK):
        chunk = insert_rows[i:i + CHUNK]
        values = ", ".join(
            f"('{r['id']}', '{r['salesman_sk']}', '{r['outlet_sk']}', "
            f"'{r['day']}', {r['week']}, {r['freq']}, "
            f"{'NULL' if r['bg'] is None else repr(r['bg'])}, "
            f"FALSE, TIMESTAMP '{now}', TIMESTAMP '{now}')"
            for r in chunk
        )
        bq.execute(
            f"""
            INSERT INTO {SFA_WEB}.fact_route_plan_pjp
              (route_plan_id, salesman_sk, outlet_sk, visit_day_of_week,
               week_number, visit_frequency, brand_group,
               is_deleted, created_at, updated_at)
            VALUES {values}
            """,
            [],
        )
        inserted += len(chunk)

    log_event(
        "pjp.upload",
        "fact_route_plan_pjp",
        "",
        current_user.username,
        payload={"rows_inserted": inserted, "filename": file.filename},
    )
    return {
        "inserted": inserted,
        "filename": file.filename,
        "message": f"Successfully uploaded {inserted} route plan rows.",
    }
