"""
GET /evaluate/team             — team-level call/EC rollup
GET /evaluate/salesman/{sk}    — individual salesman store-level detail
"""
from datetime import date

from fastapi import APIRouter, Depends, Query

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/evaluate", tags=["evaluate"])

SFA_STEP = f"`{settings.bq_project}.sfa_step`"


@router.get("/team")
def evaluate_team(
    visit_date: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    d = visit_date or date.today().isoformat()
    bg_clause, bg_params = brand_group_filter(current_user, "bg", "v")

    rows = bq.query(
        f"""
        SELECT
          v.salesman_sk,
          sm.salesman_name,
          COUNT(DISTINCT p.outlet_sk) AS planned,
          COUNT(DISTINCT v.outlet_sk) AS call_count,
          COUNTIF(v.effective_call = 'YES') AS effective_call_count,
          SAFE_DIVIDE(COUNTIF(v.effective_call='YES'), NULLIF(COUNT(*),0))*100 AS ec_rate_pct
        FROM {settings.table('fact_visit')} v
        JOIN {SFA_STEP}.dim_salesman sm USING (salesman_sk)
        LEFT JOIN {SFA_STEP}.vw_route_compliance p
          ON p.salesman_sk = v.salesman_sk AND p.visit_date = v.visit_date
        WHERE v.visit_date = @vdate AND v.is_deleted = FALSE {bg_clause}
        GROUP BY v.salesman_sk, sm.salesman_name
        ORDER BY call_count DESC
        """,
        [bq.p("vdate", "DATE", d)] + bg_params,
    )
    return {"date": d, "rows": rows}


@router.get("/salesman/{salesman_sk}")
def evaluate_salesman(
    salesman_sk: str,
    visit_date: str | None = Query(None),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    d = visit_date or date.today().isoformat()

    rows = bq.query(
        f"""
        SELECT
          o.outlet_sk,
          o.store_name,
          o.store_grade,
          p.outlet_sk IS NOT NULL AS planned,
          v.visit_id IS NOT NULL AS is_call,
          CASE WHEN v.visit_id IS NOT NULL AND v.effective_call = 'YES' THEN TRUE
               WHEN v.visit_id IS NOT NULL THEN FALSE ELSE NULL END AS is_effective,
          CASE
            WHEN v.visit_id IS NOT NULL AND v.effective_call = 'YES' THEN 'OK'
            WHEN v.visit_id IS NOT NULL THEN 'Low Conversion'
            ELSE 'Belum Terlaksana'
          END AS status
        FROM {SFA_STEP}.vw_route_compliance p
        JOIN {SFA_STEP}.dim_outlet o USING (outlet_sk)
        LEFT JOIN {settings.table('fact_visit')} v
          ON v.outlet_sk = o.outlet_sk
          AND v.salesman_sk = @sk
          AND v.visit_date = @vdate
          AND v.is_deleted = FALSE
        WHERE p.salesman_sk = @sk AND p.visit_date = @vdate
        ORDER BY p.sequence_order
        """,
        [bq.p("sk", "STRING", salesman_sk), bq.p("vdate", "DATE", d)],
    )
    return {"salesman_sk": salesman_sk, "date": d, "stores": rows}
