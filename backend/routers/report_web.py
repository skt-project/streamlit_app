"""
GET /reports   — dynamic report data based on type + period + tier filters
"""
from datetime import date

from fastapi import APIRouter, Depends, Query

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/reports", tags=["reports"])

SFA_STEP = f"`{settings.bq_project}.sfa_step`"


def _period_clause(period: str) -> str:
    today = date.today().isoformat()
    month_start = date.today().replace(day=1).isoformat()
    if period == "Bulan Ini":
        return f"v.visit_date BETWEEN '{month_start}' AND '{today}'"
    if period == "Bulan Lalu":
        y, m = date.today().year, date.today().month
        if m == 1: y, m = y - 1, 12
        else: m -= 1
        ms = date(y, m, 1).isoformat()
        import calendar
        me = date(y, m, calendar.monthrange(y, m)[1]).isoformat()
        return f"v.visit_date BETWEEN '{ms}' AND '{me}'"
    if period == "Kuartal Ini":
        q = (date.today().month - 1) // 3
        qs = date(date.today().year, q * 3 + 1, 1).isoformat()
        return f"v.visit_date BETWEEN '{qs}' AND '{today}'"
    if period == "YTD":
        return f"v.visit_date BETWEEN '{date.today().replace(month=1, day=1).isoformat()}' AND '{today}'"
    return f"v.visit_date <= '{today}'"


@router.get("")
def get_report(
    type: str = Query("Achievement"),
    period: str = Query("Bulan Ini"),
    tier: str = Query("Semua Tier"),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user, "bg", "v")
    tier_clause = "AND o.store_grade = @tier" if tier != "Semua Tier" else ""
    params = list(bg_params)
    if tier != "Semua Tier":
        params.append(bq.p("tier", "STRING", tier))

    period_cond = _period_clause(period)

    if type == "Achievement":
        rows = bq.query(
            f"""
            SELECT
              sm.salesman_name,
              sm.source_salesman_code,
              COUNT(*) AS visit_count,
              COUNTIF(v.effective_call='YES') AS ec_count,
              COALESCE(SUM(v.total_demand),0) AS sell_in,
              SAFE_DIVIDE(COUNTIF(v.effective_call='YES'),NULLIF(COUNT(*),0))*100 AS ec_rate
            FROM {settings.table('fact_visit')} v
            JOIN {SFA_STEP}.dim_salesman sm USING (salesman_sk)
            JOIN {SFA_STEP}.dim_outlet o USING (outlet_sk)
            WHERE {period_cond} AND v.is_deleted = FALSE {tier_clause} {bg_clause}
            GROUP BY sm.salesman_name, sm.source_salesman_code
            ORDER BY sell_in DESC
            LIMIT 200
            """,
            params,
        )
        kpis = [
            {"label": "Total Visit", "value": str(sum(r.get("visit_count", 0) or 0 for r in rows))},
            {"label": "Total EC",    "value": str(sum(r.get("ec_count", 0) or 0 for r in rows))},
            {"label": "Sell-In (pcs)", "value": f"{sum(r.get('sell_in',0) or 0 for r in rows):,.0f}"},
        ]
    elif type == "Route Compliance":
        rows = bq.query(
            f"""
            SELECT
              sm.salesman_name,
              COUNT(*) AS planned,
              COUNTIF(rc.is_visited) AS visited,
              SAFE_DIVIDE(COUNTIF(rc.is_visited),NULLIF(COUNT(*),0))*100 AS comply_pct
            FROM {SFA_STEP}.vw_route_compliance rc
            JOIN {SFA_STEP}.dim_salesman sm USING (salesman_sk)
            WHERE {period_cond.replace('v.visit_date','rc.visit_date')}
            GROUP BY sm.salesman_name
            ORDER BY comply_pct DESC
            LIMIT 200
            """,
            params,
        )
        kpis = [
            {"label": "Planned Visits", "value": str(sum(r.get("planned", 0) or 0 for r in rows))},
            {"label": "Visited",        "value": str(sum(r.get("visited", 0) or 0 for r in rows))},
        ]
    elif type == "Effective Call Rate":
        rows = bq.query(
            f"""
            SELECT
              sm.salesman_name,
              COUNT(*) AS total_calls,
              COUNTIF(v.effective_call='YES') AS ec_count,
              SAFE_DIVIDE(COUNTIF(v.effective_call='YES'),NULLIF(COUNT(*),0))*100 AS ec_rate
            FROM {settings.table('fact_visit')} v
            JOIN {SFA_STEP}.dim_salesman sm USING (salesman_sk)
            WHERE {period_cond} AND v.is_deleted = FALSE {bg_clause}
            GROUP BY sm.salesman_name
            ORDER BY ec_rate DESC
            LIMIT 200
            """,
            params,
        )
        kpis = []
    else:  # Sell-In YTD
        rows = bq.query(
            f"""
            SELECT
              sm.salesman_name,
              COALESCE(SUM(v.total_demand),0) AS sell_in_ytd
            FROM {settings.table('fact_visit')} v
            JOIN {SFA_STEP}.dim_salesman sm USING (salesman_sk)
            WHERE {period_cond} AND v.is_deleted = FALSE {bg_clause}
            GROUP BY sm.salesman_name
            ORDER BY sell_in_ytd DESC
            LIMIT 200
            """,
            params,
        )
        kpis = []

    return {"rows": rows, "kpis": kpis}
