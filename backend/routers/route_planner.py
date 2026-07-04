"""
GET    /route-planner/salesmen        — salesmen with their weekly store lists
GET    /route-planner/stores          — outlet search for adding to route
POST   /route-planner/assignment      — assign store to salesman + day
DELETE /route-planner/assignment/{id} — remove assignment
"""
import uuid
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/route-planner", tags=["route-planner"])

SFA_STEP = f"`{settings.bq_project}.sfa_step`"
SFA_WEB  = f"`{settings.bq_project}.{settings.bq_dataset}`"

DAYS = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]


class AssignRequest(BaseModel):
    salesman_sk: int
    outlet_sk: int
    day_of_week: str
    sequence_order: int = 1
    week_pattern: str = "All"


@router.get("/salesmen")
def list_salesmen_routes(
    week: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user, "bg", "sm")

    salesmen = bq.query(
        f"""
        SELECT DISTINCT
          sm.salesman_sk,
          sm.salesman_name,
          sm.source_salesman_code,
          sm.region,
          sm.distributor_code
        FROM {SFA_STEP}.dim_salesman sm
        WHERE sm.is_active = TRUE {bg_clause}
        ORDER BY sm.salesman_name
        LIMIT 200
        """,
        bg_params,
    )

    if not salesmen:
        return []

    sk_list = ",".join(str(s["salesman_sk"]) for s in salesmen)

    pjp_rows = bq.query(
        f"""
        SELECT
          p.salesman_sk,
          p.outlet_sk,
          o.store_name,
          o.source_outlet_code,
          o.store_grade,
          p.visit_day_of_week,
          p.visit_frequency_code,
          p.visit_week_pattern,
          ROW_NUMBER() OVER (PARTITION BY p.salesman_sk, p.visit_day_of_week ORDER BY o.store_name) AS seq
        FROM {SFA_STEP}.fact_route_plan_pjp p
        JOIN {SFA_STEP}.dim_outlet o USING (outlet_sk)
        WHERE p.salesman_sk IN ({sk_list}) AND p.is_deleted = FALSE
        ORDER BY p.visit_day_of_week, seq
        """,
        [],
    )

    stores_by_sm: dict = {s["salesman_sk"]: {d: [] for d in DAYS} for s in salesmen}
    for r in pjp_rows:
        sk = r["salesman_sk"]
        day = r["visit_day_of_week"]
        if sk in stores_by_sm and day in stores_by_sm[sk]:
            stores_by_sm[sk][day].append({
                "route_plan_sk":    str(r["outlet_sk"]),
                "outlet_sk":        r["outlet_sk"],
                "store_name":       r["store_name"],
                "source_outlet_code": r["source_outlet_code"],
                "store_grade":      r.get("store_grade"),
                "visit_day_of_week": day,
                "visit_week_pattern": r.get("visit_week_pattern"),
                "sequence_no":      int(r["seq"]),
            })

    result = []
    for s in salesmen:
        sk = s["salesman_sk"]
        total = sum(len(v) for v in stores_by_sm[sk].values())
        result.append({
            **s,
            "stores_per_day":  stores_by_sm[sk],
            "total_stores":    total,
            "achievement_pct": None,
            "compliance_pct":  None,
        })
    return result


@router.get("/stores")
def search_stores(
    q: str = Query(..., min_length=2),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    rows = bq.query(
        f"""
        SELECT outlet_sk, source_outlet_code, store_name, store_grade, region
        FROM {SFA_STEP}.dim_outlet
        WHERE (LOWER(store_name) LIKE LOWER(CONCAT('%',@q,'%'))
           OR  LOWER(source_outlet_code) LIKE LOWER(CONCAT('%',@q,'%')))
          AND is_active = TRUE
        ORDER BY store_name
        LIMIT 30
        """,
        [bq.p("q", "STRING", q)],
    )
    return rows


@router.post("/assignment", status_code=201)
def assign_store(
    body: AssignRequest,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("spv", "asm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Not allowed")
    bq = BQClient.get()
    new_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    bq.execute(
        f"""
        INSERT INTO {SFA_WEB}.route_assignment
          (assignment_id, salesman_sk, outlet_sk, day_of_week, sequence_order,
           week_pattern, assigned_by, assigned_at, is_deleted)
        VALUES (@id, @sk, @osk, @day, @seq, @wp, @by, @now, FALSE)
        """,
        [
            bq.p("id",  "STRING",    new_id),
            bq.p("sk",  "INT64",     body.salesman_sk),
            bq.p("osk", "INT64",     body.outlet_sk),
            bq.p("day", "STRING",    body.day_of_week),
            bq.p("seq", "INT64",     body.sequence_order),
            bq.p("wp",  "STRING",    body.week_pattern),
            bq.p("by",  "STRING",    current_user.username),
            bq.p("now", "TIMESTAMP", now),
        ],
    )
    return {"assignment_id": new_id, "message": "Toko berhasil ditambahkan ke rute."}


@router.delete("/assignment/{assignment_id}")
def remove_assignment(
    assignment_id: str,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("spv", "asm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Not allowed")
    bq = BQClient.get()
    bq.execute(
        f"UPDATE {SFA_WEB}.route_assignment SET is_deleted = TRUE WHERE assignment_id = @id",
        [bq.p("id", "STRING", assignment_id)],
    )
    return {"message": "Assignment removed."}
