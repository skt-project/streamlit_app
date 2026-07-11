"""
GET /evaluate/team             — team-level call/EC rollup for a ISO week
GET /evaluate/salesman/{sk}    — individual salesman store-level detail for a ISO week
"""
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, Query

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/evaluate", tags=["evaluate"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


def _parse_week(week: str | None) -> tuple[str, str]:
    """Parse '2026-W27' → (Monday ISO date, Sunday ISO date). Defaults to current week."""
    if week:
        try:
            monday = datetime.strptime(week + "-1", "%G-W%V-%u").date()
            return monday.isoformat(), (monday + timedelta(days=6)).isoformat()
        except ValueError:
            pass
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat(), (monday + timedelta(days=6)).isoformat()


@router.get("/team")
def evaluate_team(
    week: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    date_from, date_to = _parse_week(week)
    bg_clause, bg_params = brand_group_filter(current_user, "bg", "v")

    cache_key = f"evaluate:team:{week or 'current'}:{current_user.brand_group or 'all'}"
    rows = bq.query_cached(
        cache_key,
        f"""
        SELECT
          v.salesman_sk,
          sm.salesman_name,
          COUNT(DISTINCT v.outlet_sk) AS call_count,
          COUNTIF(v.effective_call = 'YES') AS effective_call_count,
          SAFE_DIVIDE(COUNTIF(v.effective_call='YES'), NULLIF(COUNT(*),0))*100 AS ec_rate_pct
        FROM {settings.table('fact_visit')} v
        JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
        WHERE v.visit_date BETWEEN @dfrom AND @dto AND v.is_deleted = FALSE {bg_clause}
        GROUP BY v.salesman_sk, sm.salesman_name
        ORDER BY call_count DESC
        """,
        [bq.p("dfrom", "DATE", date_from), bq.p("dto", "DATE", date_to)] + bg_params,
        ttl=300,  # 5 min — team weekly aggregates, acceptable lag
    )
    return rows  # plain array — frontend expects EvaluateTeamRow[]


@router.get("/salesman/{salesman_sk}")
def evaluate_salesman(
    salesman_sk: str,
    week: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    date_from, date_to = _parse_week(week)

    # Show visited outlets + planned-but-not-visited outlets for the week.
    # Uses fact_route_plan_pjp directly (no vw_route_compliance needed).
    cache_key = f"evaluate:salesman:{salesman_sk}:{week or 'current'}"
    rows = bq.query_cached(
        cache_key,
        f"""
        -- Visited outlets (may or may not be in the PJP route plan)
        SELECT
          o.outlet_sk,
          o.store_name,
          o.store_grade,
          p.outlet_sk IS NOT NULL AS planned,
          TRUE AS is_call,
          CASE WHEN v.effective_call = 'YES' THEN TRUE ELSE FALSE END AS is_effective,
          CASE WHEN v.effective_call = 'YES' THEN 'OK' ELSE 'Low Conversion' END AS status
        FROM {settings.table('fact_visit')} v
        JOIN {SFA_WEB}.dim_outlet o USING (outlet_sk)
        LEFT JOIN {SFA_WEB}.fact_route_plan_pjp p
          ON p.outlet_sk = v.outlet_sk AND p.salesman_sk = @sk AND p.is_deleted = FALSE
        WHERE v.salesman_sk = @sk
          AND v.visit_date BETWEEN @dfrom AND @dto
          AND v.is_deleted = FALSE

        UNION ALL

        -- Planned outlets that were not visited this week
        SELECT
          o.outlet_sk,
          o.store_name,
          o.store_grade,
          TRUE AS planned,
          FALSE AS is_call,
          NULL AS is_effective,
          'Belum Terlaksana' AS status
        FROM {SFA_WEB}.fact_route_plan_pjp p
        JOIN {SFA_WEB}.dim_outlet o USING (outlet_sk)
        WHERE p.salesman_sk = @sk
          AND p.is_deleted = FALSE
          AND NOT EXISTS (
            SELECT 1 FROM {settings.table('fact_visit')} fv
            WHERE fv.outlet_sk = p.outlet_sk
              AND fv.salesman_sk = @sk
              AND fv.visit_date BETWEEN @dfrom AND @dto
              AND fv.is_deleted = FALSE
          )
        ORDER BY planned DESC, store_name
        LIMIT 200
        """,
        [bq.p("sk", "STRING", salesman_sk), bq.p("dfrom", "DATE", date_from), bq.p("dto", "DATE", date_to)],
        ttl=120,  # 2 min — more granular, bust sooner
    )
    return {"salesman_sk": salesman_sk, "date_from": date_from, "date_to": date_to, "stores": rows}
