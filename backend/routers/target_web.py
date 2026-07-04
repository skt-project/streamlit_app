"""
GET  /target/comply            — brand comply summary
GET  /target/spv               — SPV target rows for edit
POST /target/spv               — upsert single row
POST /target/spv/bulk          — upsert many rows
POST /target/spv/submit        — change draft → submitted
"""
import uuid
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/target", tags=["target"])

SFA_WEB  = f"`{settings.bq_project}.{settings.bq_dataset}`"
SFA_STEP = f"`{settings.bq_project}.sfa_step`"


class SpvTargetUpsert(BaseModel):
    salesman_sk: int
    brand: str
    period_month: str          # YYYY-MM-01
    spv_target_amount: float


class BulkUpsert(BaseModel):
    rows: list[SpvTargetUpsert]


class SubmitRequest(BaseModel):
    period_month: str
    brand: str | None = None


@router.get("/comply")
def get_comply(
    period_month: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    pm = period_month or date.today().replace(day=1).isoformat()

    rows = bq.query(
        f"""
        SELECT
          brand,
          SUM(management_target) AS management_target,
          SUM(spv_target)        AS spv_target,
          SAFE_DIVIDE(SUM(spv_target), NULLIF(SUM(management_target),0))*100 AS comply_pct
        FROM {SFA_WEB}.spv_target
        WHERE DATE_TRUNC(period_month, MONTH) = DATE(@pm)
          AND approval_status IN ('submitted','approved')
        GROUP BY brand
        ORDER BY brand
        """,
        [bq.p("pm", "DATE", pm)],
    )
    return rows


@router.get("/spv")
def get_spv_targets(
    period_month: str | None = Query(None),
    brand: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    pm = period_month or date.today().replace(day=1).isoformat()
    brand_clause = "AND brand = @brand" if brand else ""
    params = [bq.p("pm", "DATE", pm)]
    if brand:
        params.append(bq.p("brand", "STRING", brand))

    rows = bq.query(
        f"""
        SELECT
          t.spv_target_id,
          t.salesman_sk,
          sm.salesman_name,
          t.brand,
          CAST(t.period_month AS STRING) AS period_month,
          t.management_target,
          t.spv_target,
          t.approval_status
        FROM {SFA_WEB}.spv_target t
        JOIN {SFA_STEP}.dim_salesman sm USING (salesman_sk)
        WHERE DATE_TRUNC(t.period_month, MONTH) = DATE(@pm)
          {brand_clause}
        ORDER BY sm.salesman_name, t.brand
        """,
        params,
    )
    return {"rows": rows, "period_month": pm}


@router.post("/spv", status_code=201)
def upsert_spv_target(
    body: SpvTargetUpsert,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("spv", "asm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Not allowed")
    bq = BQClient.get()
    _upsert_one(bq, body, current_user.username)
    return {"message": "Target saved."}


@router.post("/spv/bulk", status_code=201)
def bulk_upsert(
    body: BulkUpsert,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("spv", "asm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Not allowed")
    bq = BQClient.get()
    for row in body.rows:
        _upsert_one(bq, row, current_user.username)
    return {"message": f"{len(body.rows)} rows saved."}


@router.post("/spv/submit")
def submit_targets(
    body: SubmitRequest,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    brand_clause = "AND brand = @brand" if body.brand else ""
    params = [
        bq.p("pm",  "DATE",      body.period_month),
        bq.p("now", "TIMESTAMP", datetime.now(timezone.utc).isoformat()),
    ]
    if body.brand:
        params.append(bq.p("brand", "STRING", body.brand))
    bq.execute(
        f"""
        UPDATE {SFA_WEB}.spv_target
        SET approval_status = 'submitted', updated_at = @now
        WHERE DATE_TRUNC(period_month, MONTH) = DATE(@pm)
          AND approval_status = 'draft'
          {brand_clause}
        """,
        params,
    )
    return {"message": "Targets submitted for approval."}


def _upsert_one(bq: BQClient, body: SpvTargetUpsert, username: str):
    existing = bq.query_one(
        f"""
        SELECT spv_target_id FROM {SFA_WEB}.spv_target
        WHERE salesman_sk = @sk AND brand = @brand
          AND DATE_TRUNC(period_month, MONTH) = DATE(@pm)
          AND is_deleted = FALSE
        LIMIT 1
        """,
        [
            bq.p("sk",    "INT64",  body.salesman_sk),
            bq.p("brand", "STRING", body.brand),
            bq.p("pm",    "DATE",   body.period_month),
        ],
    )
    now = datetime.now(timezone.utc).isoformat()
    if existing:
        bq.execute(
            f"""
            UPDATE {SFA_WEB}.spv_target
            SET spv_target = @val, updated_at = @now, updated_by = @by
            WHERE spv_target_id = @id
            """,
            [
                bq.p("val", "FLOAT64", body.spv_target_amount),
                bq.p("now", "TIMESTAMP", now),
                bq.p("by",  "STRING",    username),
                bq.p("id",  "STRING",    existing["spv_target_id"]),
            ],
        )
    else:
        bq.execute(
            f"""
            INSERT INTO {SFA_WEB}.spv_target
              (spv_target_id, salesman_sk, brand, period_month, management_target,
               spv_target, approval_status, created_by, created_at, updated_at, is_deleted)
            VALUES
              (@id, @sk, @brand, DATE(@pm), 0,
               @val, 'draft', @by, @now, @now, FALSE)
            """,
            [
                bq.p("id",    "STRING",    str(uuid.uuid4())),
                bq.p("sk",    "INT64",     body.salesman_sk),
                bq.p("brand", "STRING",    body.brand),
                bq.p("pm",    "DATE",      body.period_month),
                bq.p("val",   "FLOAT64",   body.spv_target_amount),
                bq.p("by",    "STRING",    username),
                bq.p("now",   "TIMESTAMP", now),
            ],
        )
