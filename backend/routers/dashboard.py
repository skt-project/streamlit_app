"""
GET /dashboard/kpi    — single salesman KPIs for a date
GET /dashboard/team   — team KPIs for SPV/ASM/DDM
"""
from datetime import date

from fastapi import APIRouter, Depends, Query

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from models.dashboard import KpiOut, TeamKpiResponse, TeamMemberKpi
from services.bq import BQClient

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/kpi", response_model=KpiOut)
def get_kpi(
    salesman_sk: str | None = Query(None),
    visit_date: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    # Auto-resolve from JWT so mobile never needs to pass it explicitly
    sk = current_user.salesman_sk or salesman_sk or ""
    if not sk:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="salesman_sk required — link this user to a salesman in Administration")

    bq = BQClient.get()
    d = visit_date or date.today().isoformat()

    cache_key = f"kpi:{sk}:{d}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    row = bq.query_one(
        f"""
        SELECT
          COUNT(*) AS total_visits,
          COUNTIF(effective_call = 'YES') AS effective_calls,
          COALESCE(SUM(total_demand), 0) AS total_demand,
          COUNTIF(approval_status = 'PENDING_SPV') AS pending_approvals,
          COUNTIF(approval_status = 'REVISION_REQUIRED') AS revision_count
        FROM {settings.table('fact_visit')}
        WHERE salesman_sk = @sk AND visit_date = @vdate AND is_deleted = FALSE
        """,
        [bq.p("sk", "STRING", sk), bq.p("vdate", "DATE", d)],
    )

    # Route completion: scheduled stores vs visited
    scheduled = (bq.query_one(
        f"""
        SELECT COUNT(*) AS n FROM {settings.table('fact_route_plan_pjp')}
        WHERE salesman_sk = @sk AND is_deleted = FALSE
          AND visit_day_of_week = FORMAT_DATE('%A', DATE(@vdate))
        """,
        [bq.p("sk", "STRING", sk), bq.p("vdate", "DATE", d)],
    ) or {}).get("n", 0)

    total = row.get("total_visits", 0) if row else 0
    effective = row.get("effective_calls", 0) if row else 0
    strike_rate = round((effective / total * 100) if total > 0 else 0.0, 1)
    route_pct = round((total / scheduled * 100) if scheduled > 0 else 0.0, 1)

    result = KpiOut(
        total_visits=total,
        effective_calls=effective,
        strike_rate=strike_rate,
        total_demand=float(row.get("total_demand", 0) or 0) if row else 0.0,
        pending_approvals=int(row.get("pending_approvals", 0) or 0) if row else 0,
        revision_count=int(row.get("revision_count", 0) or 0) if row else 0,
        route_completion_pct=route_pct,
        date=d,
    )
    bq.cache.set(cache_key, result, ttl=120)  # 2 min — live day KPIs, acceptable lag
    return result


@router.get("/team", response_model=TeamKpiResponse)
def get_team_kpi(
    visit_date: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    d = visit_date or date.today().isoformat()
    bg_clause, bg_params = brand_group_filter(current_user, table_alias="v")

    cache_key = f"team-kpi:{current_user.salesman_sk or current_user.user_id}:{d}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    rows = bq.query(
        f"""
        SELECT
          v.salesman_sk,
          sm.salesman_name,
          COUNT(*) AS total_visits,
          COUNTIF(v.effective_call = 'YES') AS effective_calls,
          COALESCE(SUM(v.total_demand), 0) AS total_demand,
          COUNTIF(v.approval_status = 'PENDING_SPV') AS pending_approvals
        FROM {settings.table('fact_visit')} v
        JOIN {settings.table('dim_salesman')} sm USING (salesman_sk)
        WHERE v.visit_date = @vdate AND v.is_deleted = FALSE {bg_clause}
        GROUP BY v.salesman_sk, sm.salesman_name
        ORDER BY total_demand DESC
        LIMIT 100
        """,
        [bq.p("vdate", "DATE", d)] + bg_params,
    )

    members = []
    for r in rows:
        t = r.get("total_visits", 0) or 0
        e = r.get("effective_calls", 0) or 0
        members.append(TeamMemberKpi(
            salesman_sk=r["salesman_sk"],
            salesman_name=r.get("salesman_name"),
            total_visits=t,
            effective_calls=e,
            strike_rate=round((e / t * 100) if t > 0 else 0.0, 1),
            total_demand=float(r.get("total_demand", 0) or 0),
            pending_approvals=int(r.get("pending_approvals", 0) or 0),
        ))

    result = TeamKpiResponse(members=members, total_members=len(members))
    bq.cache.set(cache_key, result, ttl=120)  # 2 min — live team KPIs
    return result
