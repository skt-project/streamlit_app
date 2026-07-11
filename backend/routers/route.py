"""
GET  /route/salesmen          — lightweight salesman list for the Route Planner rail
GET  /route/plan              — weekly PJP-derived route plan for one salesman
GET  /route/outlets           — searchable outlet list for a distributor
"""
from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from google.cloud import bigquery

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from models.route import RouteOutlet, SalesmanMiniOut, WeeklyRoutePlan
from services.bq import BQClient

router = APIRouter(prefix="/route", tags=["route"])

_DAYS_ID = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu"]


def _iso_week_monday(d: date) -> date:
    """Return the Monday of the ISO week containing d."""
    return d - timedelta(days=d.isoweekday() - 1)


def _week_label(monday: date) -> str:
    iso_year, iso_week, _ = monday.isocalendar()
    saturday = monday + timedelta(days=5)
    start = monday.strftime("%-d %b")
    end = saturday.strftime("%-d %b %Y")
    return f"Week {iso_week} · {start} – {end}"


@router.get("/salesmen", response_model=list[SalesmanMiniOut])
def list_salesmen_for_planner(
    distributor_code: str | None = Query(None),
    region: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    """Return the salesman rail data (no pagination — full list for the sidebar)."""
    bq = BQClient.get()

    cache_key = f"route:salesmen:{distributor_code}:{region}:{current_user.role}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    params: list = []
    bg_clause, bg_params = brand_group_filter(current_user)
    conditions = [f"TRUE {bg_clause}"]
    params.extend(bg_params)

    # Scope by role
    if current_user.role == "dm" and current_user.distributor_code:
        conditions.append("AND distributor_code = @scope_dist")
        params.append(bq.p("scope_dist", "STRING", current_user.distributor_code))
    elif current_user.role in ("spv", "asm") and current_user.territory:
        conditions.append("AND region = @scope_region")
        params.append(bq.p("scope_region", "STRING", current_user.territory))

    if distributor_code:
        conditions.append("AND distributor_code = @dist")
        params.append(bq.p("dist", "STRING", distributor_code))
    if region:
        conditions.append("AND region = @region")
        params.append(bq.p("region", "STRING", region))

    where = " ".join(conditions)
    rows = bq.query(
        f"""
        SELECT salesman_sk, source_salesman_code, salesman_name,
               distributor_code, region, spv_name, is_active, brand_group
        FROM {settings.table('vw_salesman_active')}
        WHERE {where}
        ORDER BY salesman_name
        LIMIT 500
        """,
        params,
    )

    result = [SalesmanMiniOut(**r) for r in rows]
    bq.cache.set(cache_key, result, ttl=120)
    return result


@router.get("/plan", response_model=WeeklyRoutePlan)
def get_weekly_plan(
    salesman_sk: str = Query(...),
    week: str | None = Query(None, description="ISO week string YYYY-Www, e.g. 2026-W27. Defaults to current week."),
    current_user: UserContext = Depends(require_auth),
):
    """
    Expand the PJP recurring pattern into a concrete weekly schedule.
    week_pattern 'Minggu Ganjil' = odd ISO week number, 'Minggu Genap' = even.
    F4/F4+ visits appear every week regardless of pattern.
    """
    bq = BQClient.get()

    # Parse week param
    if week:
        year, wnum = int(week[:4]), int(week[6:])
        monday = date.fromisocalendar(year, wnum, 1)
    else:
        monday = _iso_week_monday(date.today())

    iso_week_num = monday.isocalendar()[1]
    is_odd = (iso_week_num % 2) == 1

    cache_key = f"route:plan:{salesman_sk}:{monday.isoformat()}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    # Get salesman info
    sm_row = bq.query_one(
        f"""
        SELECT salesman_sk, salesman_name, distributor_code
        FROM {settings.table('vw_salesman_active')}
        WHERE salesman_sk = @sk
        """,
        [bq.p("sk", "STRING", salesman_sk)],
    )

    # Expand PJP pattern for this week
    rows = bq.query(
        f"""
        SELECT
          p.route_plan_sk,
          p.outlet_sk,
          p.source_outlet_code,
          o.store_name,
          o.address,
          o.brand,
          o.store_grade,
          p.visit_day_of_week,
          p.visit_frequency_code,
          p.visit_week_pattern
        FROM {settings.table('fact_route_plan_pjp')} p
        LEFT JOIN {settings.table('vw_outlet_active')} o USING (outlet_sk)
        WHERE p.salesman_sk = @sk
          AND p.is_deleted = FALSE
          AND (
            p.visit_week_pattern IS NULL
            OR p.visit_week_pattern = ''
            OR p.visit_frequency_code IN ('F4+', 'F4')
            OR (@is_odd = TRUE AND p.visit_week_pattern = 'Minggu Ganjil')
            OR (@is_odd = FALSE AND p.visit_week_pattern = 'Minggu Genap')
          )
        ORDER BY
          CASE p.visit_day_of_week
            WHEN 'Senin'   THEN 1 WHEN 'Selasa' THEN 2 WHEN 'Rabu'   THEN 3
            WHEN 'Kamis'   THEN 4 WHEN 'Jumat'  THEN 5 WHEN 'Sabtu'  THEN 6
            ELSE 7
          END,
          o.store_name
        """,
        [
            bq.p("sk", "STRING", salesman_sk),
            bq.p("is_odd", "BOOL", is_odd),
        ],
    )

    # Group by day
    days: dict[str, list[RouteOutlet]] = {d: [] for d in _DAYS_ID}
    for r in rows:
        day = r.get("visit_day_of_week") or "Senin"
        if day in days:
            days[day].append(RouteOutlet(**{k: r.get(k) for k in RouteOutlet.model_fields}))

    result = WeeklyRoutePlan(
        salesman_sk=salesman_sk,
        salesman_name=sm_row["salesman_name"] if sm_row else None,
        distributor_code=sm_row["distributor_code"] if sm_row else None,
        week_start=monday.isoformat(),
        week_label=_week_label(monday),
        is_odd_week=is_odd,
        days=days,
    )
    bq.cache.set(cache_key, result, ttl=60)
    return result


@router.get("/outlets")
def search_outlets(
    q: str | None = Query(None),
    distributor_code: str | None = Query(None),
    brand: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    conditions = ["TRUE"]
    params: list = []

    if distributor_code:
        conditions.append("AND distributor_code = @dist")
        params.append(bq.p("dist", "STRING", distributor_code))
    if brand:
        conditions.append("AND brand = @brand")
        params.append(bq.p("brand", "STRING", brand))
    if q:
        conditions.append("AND (LOWER(store_name) LIKE @q OR LOWER(source_outlet_code) LIKE @q)")
        params.append(bq.p("q", "STRING", f"%{q.lower()}%"))

    where = " ".join(conditions)
    offset = (page - 1) * page_size

    cache_key = f"route:outlets:{distributor_code or ''}:{brand or ''}:{q or ''}:{page}:{page_size}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    rows = bq.query(
        f"""
        SELECT outlet_sk, source_outlet_code, store_name, brand, store_grade,
               distributor_code, region, address
        FROM {settings.table('vw_outlet_active')}
        WHERE {where}
        ORDER BY store_name
        LIMIT @lim OFFSET @off
        """,
        params + [bq.p("lim", "INT64", page_size), bq.p("off", "INT64", offset)],
    )

    result = {"items": rows, "page": page, "page_size": page_size}
    bq.cache.set(cache_key, result, ttl=300)  # 5 min — outlet dim stable between imports
    return result
