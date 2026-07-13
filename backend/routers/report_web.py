"""
GET  /reports              — dynamic report data (JSON)
GET  /reports/export.csv   — same data as CSV download
"""
import calendar
import csv
import io
from datetime import date

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/reports", tags=["reports"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"


def _period_dates(period: str) -> tuple[str, str]:
    """Return (start, end) ISO date strings for the given period label."""
    today = date.today()
    if period == "Bulan Ini":
        return today.replace(day=1).isoformat(), today.isoformat()
    if period == "Bulan Lalu":
        y, m = today.year, today.month
        if m == 1:
            y, m = y - 1, 12
        else:
            m -= 1
        return date(y, m, 1).isoformat(), date(y, m, calendar.monthrange(y, m)[1]).isoformat()
    if period == "Kuartal Ini":
        q = (today.month - 1) // 3
        return date(today.year, q * 3 + 1, 1).isoformat(), today.isoformat()
    if period == "YTD":
        return today.replace(month=1, day=1).isoformat(), today.isoformat()
    return date(today.year, 1, 1).isoformat(), today.isoformat()


def _build_rows(
    report_type: str,
    period: str,
    tier: str,
    current_user: UserContext,
    use_cache: bool = True,
) -> tuple[list[dict], list[dict]]:
    bq = BQClient.get()

    # Cache key includes all filter dimensions — different users with same brand_group share cache
    bg_key = current_user.brand_group or "all"
    cache_key = f"report:{report_type}:{period}:{tier}:{bg_key}"
    if use_cache:
        hit = bq.cache.get(cache_key)
        if hit is not None:
            return hit

    bg_clause, bg_params = brand_group_filter(current_user, "bg", "v")
    tier_clause = "AND o.store_grade = @tier" if tier != "Semua Tier" else ""
    params = list(bg_params)
    if tier != "Semua Tier":
        params.append(bq.p("tier", "STRING", tier))
    date_start, date_end = _period_dates(period)
    params += [bq.p("ds", "DATE", date_start), bq.p("de", "DATE", date_end)]
    period_cond = "v.visit_date BETWEEN @ds AND @de"

    if report_type == "Achievement":
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
            JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
            JOIN {SFA_WEB}.dim_outlet o USING (outlet_sk)
            WHERE {period_cond} AND v.is_deleted = FALSE {tier_clause} {bg_clause}
            GROUP BY sm.salesman_name, sm.source_salesman_code
            ORDER BY sell_in DESC
            LIMIT 500
            """,
            params,
        )
        kpis = [
            {"label": "Total Visit",    "value": str(sum(r.get("visit_count", 0) or 0 for r in rows))},
            {"label": "Total EC",       "value": str(sum(r.get("ec_count", 0) or 0 for r in rows))},
            {"label": "Sell-In (pcs)",  "value": f"{sum(r.get('sell_in', 0) or 0 for r in rows):,.0f}"},
        ]

    elif report_type == "Route Compliance":
        # Route compliance query joins on dim_salesman (alias "sm"), so we need
        # the brand_group filter scoped to that alias — not the "v" alias used
        # for visit-level queries elsewhere in this function.
        rc_bg_clause, rc_bg_params = brand_group_filter(current_user, "bg_rc", "sm")
        rows = bq.query(
            f"""
            SELECT
              sm.salesman_name,
              COUNT(DISTINCT p.outlet_sk) AS planned,
              COUNT(DISTINCT v.outlet_sk) AS visited,
              SAFE_DIVIDE(COUNT(DISTINCT v.outlet_sk), NULLIF(COUNT(DISTINCT p.outlet_sk), 0))*100 AS comply_pct
            FROM {SFA_WEB}.dim_salesman sm
            LEFT JOIN {SFA_WEB}.fact_route_plan_pjp p ON p.salesman_sk = sm.salesman_sk AND p.is_deleted = FALSE
            LEFT JOIN {settings.table('fact_visit')} v
              ON v.salesman_sk = sm.salesman_sk
              AND v.visit_date BETWEEN @ds AND @de
              AND v.is_deleted = FALSE
            WHERE sm.is_active = TRUE {rc_bg_clause}
            GROUP BY sm.salesman_name
            HAVING COUNT(DISTINCT p.outlet_sk) > 0
            ORDER BY comply_pct DESC
            LIMIT 500
            """,
            [bq.p("ds", "DATE", date_start), bq.p("de", "DATE", date_end)] + rc_bg_params,
        )
        kpis = [
            {"label": "Planned", "value": str(sum(r.get("planned", 0) or 0 for r in rows))},
            {"label": "Visited", "value": str(sum(r.get("visited", 0) or 0 for r in rows))},
        ]

    elif report_type == "Effective Call Rate":
        rows = bq.query(
            f"""
            SELECT
              sm.salesman_name,
              COUNT(*) AS total_calls,
              COUNTIF(v.effective_call='YES') AS ec_count,
              SAFE_DIVIDE(COUNTIF(v.effective_call='YES'),NULLIF(COUNT(*),0))*100 AS ec_rate
            FROM {settings.table('fact_visit')} v
            JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
            WHERE {period_cond} AND v.is_deleted = FALSE {bg_clause}
            GROUP BY sm.salesman_name
            ORDER BY ec_rate DESC
            LIMIT 500
            """,
            params,
        )
        kpis = []

    else:  # Sell-In YTD
        rows = bq.query(
            f"""
            SELECT
              sm.salesman_name,
              COALESCE(SUM(v.total_demand), 0) AS sell_in_ytd
            FROM {settings.table('fact_visit')} v
            JOIN {SFA_WEB}.dim_salesman sm USING (salesman_sk)
            WHERE {period_cond} AND v.is_deleted = FALSE {bg_clause}
            GROUP BY sm.salesman_name
            ORDER BY sell_in_ytd DESC
            LIMIT 500
            """,
            params,
        )
        kpis = []

    result = (rows, kpis)
    if use_cache:
        bq.cache.set(cache_key, result, ttl=120)  # 2-minute TTL — aggregate data, not real-time
    return result


@router.get("")
def get_report(
    type: str = Query("Achievement"),
    period: str = Query("Bulan Ini"),
    tier: str = Query("Semua Tier"),
    current_user: UserContext = Depends(require_auth),
):
    rows, kpis = _build_rows(type, period, tier, current_user)
    return {"rows": rows, "kpis": kpis}


@router.get("/export.csv")
def export_report_csv(
    type: str = Query("Achievement"),
    period: str = Query("Bulan Ini"),
    tier: str = Query("Semua Tier"),
    current_user: UserContext = Depends(require_auth),
):
    rows, _ = _build_rows(type, period, tier, current_user, use_cache=False)  # always fresh for downloads

    if not rows:
        return StreamingResponse(
            iter(["No data"]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=report_{type}_{period}.csv"},
        )

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow({k: (float(v) if hasattr(v, '__float__') and not isinstance(v, bool) else v)
                         for k, v in row.items()})

    output.seek(0)
    filename = f"report_{type.replace(' ', '_')}_{period.replace(' ', '_')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
