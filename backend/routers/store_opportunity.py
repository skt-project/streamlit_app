"""
GET /store-opportunity   — ranked stores by demand gap
Gap = estimated potential demand - actual MTD demand
"""
from datetime import date

from fastapi import APIRouter, Depends, Query

from config import settings
from dependencies import require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/store-opportunity", tags=["store-opportunity"])

SFA_WEB = f"`{settings.bq_project}.{settings.bq_dataset}`"

# Tier benchmarks: monthly potential demand (IDR) by store_grade
_TIER_BENCHMARK: dict[str, float] = {
    "A": 5_000_000,
    "B": 3_000_000,
    "C": 1_500_000,
    "D": 750_000,
}


@router.get("")
def get_store_opportunity(
    tier: str | None = Query(None, description="A/B/C/D or blank for all"),
    brand: str | None = Query(None, description="SKT/G2G or blank for all"),
    limit: int = Query(200, le=500),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    today = date.today()
    year_month = today.strftime("%Y-%m")

    cache_key = f"store-opp:{year_month}:{tier or 'all'}:{brand or 'all'}:{limit}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    tier_filter  = "AND o.store_grade = @tier" if tier else ""
    brand_filter = "AND o.brand = @brand"      if brand else ""
    params = [bq.p("year_month_prefix", "STRING", year_month)]
    if tier:
        params.append(bq.p("tier", "STRING", tier))
    if brand:
        params.append(bq.p("brand", "STRING", brand))

    rows = bq.query(
        f"""
        WITH mtd_demand AS (
          SELECT
            outlet_sk,
            SUM(total_demand) AS actual_demand_mtd,
            MAX(visit_date)   AS last_visit_date,
            COUNT(*)          AS visit_count_mtd
          FROM {settings.table('fact_visit')}
          WHERE FORMAT_DATE('%Y-%m', visit_date) = @year_month_prefix
            AND is_deleted = FALSE
          GROUP BY outlet_sk
        )
        SELECT
          o.outlet_sk,
          o.source_outlet_code,
          o.store_name,
          o.store_grade,
          o.brand,
          o.channel,
          o.city,
          o.region,
          sm.salesman_name,
          sm.salesman_sk,
          COALESCE(m.actual_demand_mtd, 0) AS actual_demand_mtd,
          m.last_visit_date,
          COALESCE(m.visit_count_mtd, 0)   AS visit_count_mtd
        FROM {SFA_WEB}.dim_outlet o
        LEFT JOIN mtd_demand m USING (outlet_sk)
        LEFT JOIN {SFA_WEB}.dim_salesman sm ON sm.salesman_sk = o.default_salesman_sk
        WHERE o.is_deleted = FALSE
          {tier_filter}
          {brand_filter}
        ORDER BY actual_demand_mtd ASC
        LIMIT {limit}
        """,
        params,
    )

    results = []
    for r in rows:
        grade = r.get("store_grade") or "D"
        benchmark = _TIER_BENCHMARK.get(grade, _TIER_BENCHMARK["D"])
        actual = float(r.get("actual_demand_mtd") or 0)
        gap = max(0.0, benchmark - actual)
        results.append({
            "outlet_sk":          r.get("outlet_sk"),
            "source_outlet_code": r.get("source_outlet_code"),
            "store_name":         r.get("store_name"),
            "store_grade":        grade,
            "brand":              r.get("brand"),
            "channel":            r.get("channel"),
            "city":               r.get("city"),
            "region":             r.get("region"),
            "salesman_name":      r.get("salesman_name"),
            "salesman_sk":        r.get("salesman_sk"),
            "actual_demand_mtd":  actual,
            "potential_demand":   benchmark,
            "gap":                gap,
            "last_visit_date":    str(r["last_visit_date"]) if r.get("last_visit_date") else None,
            "visit_count_mtd":    int(r.get("visit_count_mtd") or 0),
        })

    results.sort(key=lambda x: x["gap"], reverse=True)
    bq.cache.set(cache_key, results, ttl=300)  # 5-min TTL — MTD data, acceptable lag
    return results
