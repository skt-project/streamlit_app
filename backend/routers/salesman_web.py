"""
GET /salesman/list          — paginated salesman list with filters
GET /salesman/search        — typeahead search
GET /salesman/360/{sk}      — full 360° profile for one salesman
"""
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/salesman", tags=["salesman"])

# sfa_web.dim_salesman is the correct table: it has the brand_group column
# (added by migrate_brand_group.py).  sfa_step.dim_salesman does NOT have
# brand_group, so brand_group_filter() would fail silently against that table.
SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


@router.get("/list")
def list_salesmen(
    search: str | None = Query(None),
    salesman_type: str | None = Query(None),
    is_active: bool | None = Query(None),
    limit: int = Query(100, le=500),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user, "bg")

    clauses: list[str] = []
    params: list = list(bg_params)

    if bg_clause:
        clauses.append(bg_clause.lstrip("AND ").strip())
    if search:
        clauses.append("(LOWER(salesman_name) LIKE LOWER(CONCAT('%',@q,'%')) OR LOWER(source_salesman_code) LIKE LOWER(CONCAT('%',@q,'%')))")
        params.append(bq.p("q", "STRING", search))
    if salesman_type:
        clauses.append("salesman_type = @stype")
        params.append(bq.p("stype", "STRING", salesman_type))
    if is_active is not None:
        clauses.append("is_active = @active")
        params.append(bq.p("active", "BOOL", is_active))

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(bq.p("lim", "INT64", limit))

    rows = bq.query(
        f"""
        SELECT
          salesman_sk, source_salesman_code, salesman_name, salesman_type,
          distributor_code, region, spv_name, asm_name, is_active, brand_group
        FROM {SFA_WEB}.dim_salesman
        {where}
        ORDER BY salesman_name
        LIMIT @lim
        """,
        params,
    )
    return {"items": rows, "total": len(rows)}


@router.get("/search")
def search_salesmen(
    q: str = Query(..., min_length=2),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user, "bg")
    bg_where = bg_clause if not bg_clause else f" {bg_clause}"
    cache_key = f"salesman-search:{q.lower()}:{current_user.brand_group or 'all'}"
    return bq.query_cached(
        cache_key,
        f"""
        SELECT salesman_sk, source_salesman_code, salesman_name, brand_group
        FROM {SFA_WEB}.dim_salesman
        WHERE (LOWER(salesman_name) LIKE LOWER(CONCAT('%',@q,'%'))
           OR  LOWER(source_salesman_code) LIKE LOWER(CONCAT('%',@q,'%')))
          AND is_active = TRUE
          {bg_where}
        ORDER BY salesman_name
        LIMIT 20
        """,
        [bq.p("q", "STRING", q)] + bg_params,
        ttl=300,  # 5 min — dim_salesman changes only on master import
    )


@router.get("/360/{salesman_sk}")
def salesman_360(
    salesman_sk: str,
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    today = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()

    cache_key = f"salesman360:{salesman_sk}:{today}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    profile = bq.query_one(
        f"""
        SELECT salesman_sk, source_salesman_code, salesman_name, salesman_type,
               distributor_code, region, spv_name, asm_name, is_active
        FROM {SFA_WEB}.dim_salesman
        WHERE salesman_sk = @sk
        """,
        [bq.p("sk", "STRING", salesman_sk)],
    )

    if not profile:
        raise HTTPException(status_code=404, detail="Salesman not found")

    mtd_row = bq.query_one(
        f"""
        SELECT
          COUNT(*) AS visit_mtd,
          COUNTIF(effective_call='YES') AS ec_mtd
        FROM {settings.table('fact_visit')}
        WHERE salesman_sk = @sk AND visit_date BETWEEN @ms AND @today AND is_deleted = FALSE
        """,
        [bq.p("sk", "STRING", salesman_sk), bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
    ) or {}

    today_row = bq.query_one(
        f"""
        SELECT
          COUNT(*) AS visit_today,
          COUNTIF(effective_call='YES') AS ec_today
        FROM {settings.table('fact_visit')}
        WHERE salesman_sk = @sk AND visit_date = @today AND is_deleted = FALSE
        """,
        [bq.p("sk", "STRING", salesman_sk), bq.p("today", "DATE", today)],
    ) or {}

    # Route compliance MTD: visited outlets vs planned (all PJP outlets)
    rc_row = bq.query_one(
        f"""
        SELECT
          (SELECT COUNT(DISTINCT outlet_sk)
           FROM {settings.table('fact_visit')}
           WHERE salesman_sk = @sk AND visit_date BETWEEN @ms AND @today AND is_deleted = FALSE
          ) AS visited,
          (SELECT COUNT(DISTINCT outlet_sk)
           FROM {SFA_WEB}.fact_route_plan_pjp
           WHERE salesman_sk = @sk AND is_deleted = FALSE AND outlet_sk IS NOT NULL
          ) AS planned
        """,
        [bq.p("sk", "STRING", salesman_sk), bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
    ) or {}

    visited = rc_row.get("visited", 0) or 0
    planned = rc_row.get("planned", 0) or 0
    rc_pct  = round((visited / planned * 100) if planned > 0 else 0.0, 1)

    # Today's schedule: all PJP outlets, mark which were visited today
    today_schedule = bq.query(
        f"""
        SELECT
          o.outlet_sk, o.store_name, o.source_outlet_code,
          ROW_NUMBER() OVER (ORDER BY o.store_name) AS sequence_order,
          v.checkin_time, v.total_demand,
          CASE WHEN v.visit_id IS NOT NULL THEN 'visited' ELSE 'pending' END AS status
        FROM {SFA_WEB}.fact_route_plan_pjp p
        JOIN {SFA_WEB}.dim_outlet o USING (outlet_sk)
        LEFT JOIN {settings.table('fact_visit')} v
          ON v.outlet_sk = o.outlet_sk AND v.salesman_sk = @sk AND v.visit_date = @today AND v.is_deleted = FALSE
        WHERE p.salesman_sk = @sk AND p.is_deleted = FALSE
        ORDER BY status DESC, o.store_name
        LIMIT 50
        """,
        [bq.p("sk", "STRING", salesman_sk), bq.p("today", "DATE", today)],
    )
    for r in today_schedule:
        if r.get("checkin_time"):
            r["checkin_time"] = str(r["checkin_time"])

    outlets = bq.query(
        f"""
        SELECT
          o.outlet_sk, o.source_outlet_code, o.store_name, o.kecamatan, o.store_grade AS tier,
          COUNT(DISTINCT v.visit_id) AS visit_mtd,
          COUNTIF(v.effective_call='YES') AS ec_mtd
        FROM {SFA_WEB}.fact_route_plan_pjp p
        JOIN {SFA_WEB}.dim_outlet o USING (outlet_sk)
        LEFT JOIN {settings.table('fact_visit')} v
          ON v.outlet_sk = o.outlet_sk AND v.salesman_sk = @sk
          AND v.visit_date BETWEEN @ms AND @today AND v.is_deleted = FALSE
        WHERE p.salesman_sk = @sk AND p.is_deleted = FALSE
        GROUP BY o.outlet_sk, o.source_outlet_code, o.store_name, o.kecamatan, tier
        ORDER BY o.store_name
        LIMIT 200
        """,
        [bq.p("sk", "STRING", salesman_sk), bq.p("ms", "DATE", month_start), bq.p("today", "DATE", today)],
    )

    result = {
        **profile,
        "visit_today":      int(today_row.get("visit_today", 0) or 0),
        "ec_today":         int(today_row.get("ec_today", 0) or 0),
        "visit_mtd":        int(mtd_row.get("visit_mtd", 0) or 0),
        "ec_mtd":           int(mtd_row.get("ec_mtd", 0) or 0),
        "route_comply_pct": rc_pct,
        "today_schedule":   today_schedule,
        "total_outlets":    len(outlets),
        "outlets":          outlets,
    }
    bq.cache.set(cache_key, result, ttl=120)  # 2-min TTL — daily KPIs, today's schedule
    return result
