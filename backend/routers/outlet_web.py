"""
GET  /outlet/list              — paginated outlet list
GET  /outlet/search            — typeahead
POST /outlet/assign            — assign salesman to outlet
GET  /store/360/{outlet_id}    — store 360° profile
GET  /pjp/summary              — PJP coverage stats
GET  /pjp/list                 — PJP list with filters
"""
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from config import settings
from dependencies import require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(tags=["outlet"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


class AssignOutletRequest(BaseModel):
    outlet_id: str        # outlet_sk as string
    salesman_sk: str


# ── Outlet list ─────────────────────────────────────────────────────────────────

@router.get("/outlet/list")
def list_outlets(
    search: str | None = Query(None),
    unassigned_only: bool | None = Query(None),
    limit: int = Query(100, le=500),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    clauses, params = [], []

    if search:
        clauses.append("(LOWER(o.store_name) LIKE LOWER(CONCAT('%',@q,'%')) OR LOWER(o.source_outlet_code) LIKE LOWER(CONCAT('%',@q,'%')))")
        params.append(bq.p("q", "STRING", search))
    if unassigned_only:
        clauses.append("sm.salesman_name IS NULL")

    where = ("WHERE o.is_active = TRUE AND " + " AND ".join(clauses)) if clauses else "WHERE o.is_active = TRUE"
    params.append(bq.p("lim", "INT64", limit))

    rows = bq.query(
        f"""
        SELECT
          CAST(o.outlet_sk AS STRING) AS outlet_id,
          o.outlet_sk,
          o.source_outlet_code,
          o.store_name,
          o.kecamatan,
          o.city,
          o.store_grade AS tier,
          o.channel,
          o.is_active,
          sm.salesman_name,
          sm.source_salesman_code AS salesman_code,
          sm.salesman_sk AS salesman_sk_linked
        FROM {SFA_WEB}.dim_outlet o
        LEFT JOIN {SFA_WEB}.dim_salesman sm
          ON sm.salesman_sk = o.default_salesman_sk AND sm.is_active = TRUE
        {where}
        ORDER BY o.store_name
        LIMIT @lim
        """,
        params,
    )
    for r in rows:
        r["outlet_id"] = str(r.get("outlet_sk", ""))
    return {"items": rows, "total": len(rows)}


@router.get("/outlet/search")
def search_outlets(
    q: str = Query(..., min_length=2),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    return bq.query_cached(
        f"outlet-search:{q.lower()}",
        f"""
        SELECT CAST(outlet_sk AS STRING) AS outlet_id, outlet_sk, source_outlet_code, store_name
        FROM {SFA_WEB}.dim_outlet
        WHERE (LOWER(store_name) LIKE LOWER(CONCAT('%',@q,'%'))
           OR  LOWER(source_outlet_code) LIKE LOWER(CONCAT('%',@q,'%')))
          AND is_active = TRUE
        ORDER BY store_name
        LIMIT 20
        """,
        [bq.p("q", "STRING", q)],
        ttl=300,  # 5 min — dim_outlet changes only on master import
    )


@router.post("/outlet/assign")
def assign_outlet(
    body: AssignOutletRequest,
    current_user: UserContext = Depends(require_auth),
):
    if current_user.role not in ("spv", "asm", "dm", "ho_admin"):
        raise HTTPException(status_code=403, detail="Not allowed")
    bq = BQClient.get()
    bq.execute(
        f"""
        UPDATE {SFA_WEB}.dim_outlet
        SET default_salesman_sk = @sk
        WHERE CAST(outlet_sk AS STRING) = @oid
        """,
        [
            bq.p("sk",  "INT64",  int(body.salesman_sk)),
            bq.p("oid", "STRING", body.outlet_id),
        ],
    )
    return {"message": "Outlet reassigned successfully."}


# ── Store 360° ──────────────────────────────────────────────────────────────────

@router.get("/store/360/{outlet_id}")
def store_360(outlet_id: str, current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    today = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()
    ytd_start = date.today().replace(month=1, day=1).isoformat()

    cache_key = f"store360:{outlet_id}:{today}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    profile = bq.query_one(
        f"""
        SELECT
          o.outlet_sk, o.source_outlet_code, o.store_name, o.kecamatan, o.city,
          o.store_grade AS tier, o.channel, o.is_active, o.latitude, o.longitude,
          sm.salesman_name, sm.source_salesman_code AS salesman_code,
          spv.salesman_name AS spv_name
        FROM {SFA_WEB}.dim_outlet o
        LEFT JOIN {SFA_WEB}.dim_salesman sm ON sm.salesman_sk = o.default_salesman_sk
        LEFT JOIN {SFA_WEB}.dim_salesman spv ON spv.salesman_sk = sm.spv_salesman_sk
        WHERE CAST(o.outlet_sk AS STRING) = @oid
        """,
        [bq.p("oid", "STRING", outlet_id)],
    )
    if not profile:
        raise HTTPException(status_code=404, detail="Outlet not found")

    kpi = bq.query_one(
        f"""
        SELECT
          COUNT(*) AS visit_mtd,
          COUNTIF(effective_call='YES') AS effective_call_mtd,
          COALESCE(SUM(total_demand),0) AS sellin_mtd
        FROM {settings.table('fact_visit')}
        WHERE CAST(outlet_sk AS STRING) = @oid
          AND visit_date BETWEEN @ms AND @today AND is_deleted = FALSE
        """,
        [bq.p("oid", "STRING", outlet_id), bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
    ) or {}

    ytd = bq.query_one(
        f"""
        SELECT COALESCE(SUM(total_demand),0) AS sellin_ytd
        FROM {settings.table('fact_visit')}
        WHERE CAST(outlet_sk AS STRING) = @oid AND visit_date >= @ytd AND is_deleted = FALSE
        """,
        [bq.p("oid", "STRING", outlet_id), bq.p("ytd", "DATE", ytd_start)],
    ) or {}

    visits = bq.query(
        f"""
        SELECT
          v.visit_date, sm.salesman_name, v.checkin_time, v.checkout_time,
          v.total_demand, v.effective_call
        FROM {settings.table('fact_visit')} v
        JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
        WHERE CAST(v.outlet_sk AS STRING) = @oid AND v.is_deleted = FALSE
        ORDER BY v.visit_date DESC, v.checkin_time DESC
        LIMIT 20
        """,
        [bq.p("oid", "STRING", outlet_id)],
    )
    for r in visits:
        if r.get("checkin_time"):  r["checkin_time"] = str(r["checkin_time"])
        if r.get("checkout_time"): r["checkout_time"] = str(r["checkout_time"])
        if r.get("visit_date"):    r["visit_date"] = str(r["visit_date"])

    pjp = bq.query_one(
        f"""
        SELECT visit_day_of_week, visit_frequency_code, visit_week_pattern
        FROM {SFA_WEB}.fact_route_plan_pjp
        WHERE CAST(outlet_sk AS STRING) = @oid AND is_deleted = FALSE
        LIMIT 1
        """,
        [bq.p("oid", "STRING", outlet_id)],
    )

    result = {
        **profile,
        "visit_mtd":          int(kpi.get("visit_mtd", 0) or 0),
        "effective_call_mtd": int(kpi.get("effective_call_mtd", 0) or 0),
        "sellin_mtd":         float(kpi.get("sellin_mtd", 0) or 0),
        "sellin_ytd":         float(ytd.get("sellin_ytd", 0) or 0),
        "visits":             visits,
        "pjp_schedule":       pjp,
    }
    bq.cache.set(cache_key, result, ttl=120)  # 2-min TTL — daily KPIs, acceptable lag
    return result


# ── PJP endpoints ───────────────────────────────────────────────────────────────

@router.get("/pjp/summary")
def pjp_summary(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()
    return bq.query_one_cached(
        "pjp:summary",
        f"""
        SELECT
          COUNT(DISTINCT o.outlet_sk) AS total_stores,
          COUNT(DISTINCT p.outlet_sk) AS stores_with_pjp,
          COUNT(DISTINCT o.outlet_sk) - COUNT(DISTINCT p.outlet_sk) AS stores_basis_only,
          SAFE_DIVIDE(COUNT(DISTINCT p.outlet_sk), NULLIF(COUNT(DISTINCT o.outlet_sk),0))*100 AS coverage_pct
        FROM {SFA_WEB}.dim_outlet o
        LEFT JOIN {SFA_WEB}.fact_route_plan_pjp p ON p.outlet_sk = o.outlet_sk AND p.is_deleted = FALSE
        WHERE o.is_active = TRUE
        """,
        [],
        ttl=600,  # 10 min — only changes on PJP upload
    ) or {}


@router.get("/pjp/list")
def pjp_list(
    search: str | None = Query(None),
    limit: int = Query(100, le=500),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    search_clause = "(LOWER(o.store_name) LIKE LOWER(CONCAT('%',@q,'%')) OR LOWER(sm.salesman_name) LIKE LOWER(CONCAT('%',@q,'%')))" if search else "TRUE"
    params = [bq.p("lim", "INT64", limit)]
    if search:
        params.append(bq.p("q", "STRING", search))

    rows = bq.query(
        f"""
        SELECT
          o.source_outlet_code,
          o.store_name,
          sm.salesman_name AS source_salesman_name,
          p.visit_day_of_week,
          p.visit_frequency_code,
          p.visit_week_pattern,
          'GT' AS source_system
        FROM {SFA_WEB}.fact_route_plan_pjp p
        JOIN {SFA_WEB}.dim_outlet o USING (outlet_sk)
        JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
        WHERE p.is_deleted = FALSE AND {search_clause}
        ORDER BY o.store_name
        LIMIT @lim
        """,
        params,
    )
    return rows
