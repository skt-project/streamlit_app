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

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"

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

    cache_key = f"route-planner:salesmen:{current_user.brand_group or 'all'}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    # Single query: join salesman + PJP + outlet to avoid unquoted IN clause
    rows = bq.query(
        f"""
        SELECT
          sm.salesman_sk,
          sm.salesman_name,
          sm.source_salesman_code,
          sm.region,
          sm.distributor_code,
          p.outlet_sk,
          o.store_name,
          o.source_outlet_code AS outlet_code,
          o.store_grade,
          p.visit_day_of_week,
          p.visit_frequency_code,
          p.visit_week_pattern,
          ROW_NUMBER() OVER (
            PARTITION BY p.salesman_sk, p.visit_day_of_week ORDER BY o.store_name
          ) AS seq
        FROM {SFA_WEB}.dim_salesman sm
        LEFT JOIN {SFA_WEB}.fact_route_plan_pjp p ON p.salesman_sk = sm.salesman_sk AND p.is_deleted = FALSE
        LEFT JOIN {SFA_WEB}.dim_outlet o ON o.outlet_sk = p.outlet_sk
        WHERE sm.is_active = TRUE {bg_clause}
        ORDER BY sm.salesman_name, p.visit_day_of_week, o.store_name
        LIMIT 2000
        """,
        bg_params,
    )

    # Group rows by salesman
    salesmen_map: dict = {}
    for r in rows:
        sk = r["salesman_sk"]
        if sk not in salesmen_map:
            salesmen_map[sk] = {
                "salesman_sk": sk,
                "salesman_name": r["salesman_name"],
                "source_salesman_code": r["source_salesman_code"],
                "region": r["region"],
                "distributor_code": r["distributor_code"],
                "stores_per_day": {d: [] for d in DAYS},
            }
        day = r.get("visit_day_of_week")
        if day and day in salesmen_map[sk]["stores_per_day"] and r.get("outlet_sk"):
            salesmen_map[sk]["stores_per_day"][day].append({
                "route_plan_sk":      str(r["outlet_sk"]),
                "outlet_sk":          r["outlet_sk"],
                "store_name":         r.get("store_name"),
                "source_outlet_code": r.get("outlet_code"),
                "store_grade":        r.get("store_grade"),
                "visit_day_of_week":  day,
                "visit_week_pattern": r.get("visit_week_pattern"),
                "sequence_no":        int(r.get("seq") or 1),
            })

    result = []
    for sm in salesmen_map.values():
        total = sum(len(v) for v in sm["stores_per_day"].values())
        result.append({**sm, "total_stores": total, "achievement_pct": None, "compliance_pct": None})
    bq.cache.set(cache_key, result, ttl=600)  # 10 min — PJP changes only on import
    return result


@router.get("/stores")
def search_stores(
    q: str = Query(..., min_length=2),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    cache_key = f"route-planner:stores:{q.lower()}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached
    rows = bq.query(
        f"""
        SELECT outlet_sk, source_outlet_code, store_name, store_grade, region
        FROM {SFA_WEB}.dim_outlet
        WHERE (LOWER(store_name) LIKE LOWER(CONCAT('%',@q,'%'))
           OR  LOWER(source_outlet_code) LIKE LOWER(CONCAT('%',@q,'%')))
          AND is_active = TRUE
        ORDER BY store_name
        LIMIT 30
        """,
        [bq.p("q", "STRING", q)],
    )
    bq.cache.set(cache_key, rows, ttl=300)  # 5 min — dim_outlet stable between master imports
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
    bq.cache.invalidate("route-planner:salesmen:")
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
    bq.cache.invalidate("route-planner:salesmen:")
    return {"message": "Assignment removed."}
