"""GET /sku — product master list for survey form."""
from fastapi import APIRouter, Depends, Query

from config import settings
from dependencies import brand_group_filter, require_auth
from models.auth import UserContext
from models.sku import SkuListResponse, SkuOut
from services.bq import BQClient

router = APIRouter(prefix="/sku", tags=["sku"])


@router.get("", response_model=SkuListResponse)
def list_sku(
    q: str | None = Query(None, description="Search by name"),
    brand: str | None = Query(None),
    category: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()
    bg_clause, bg_params = brand_group_filter(current_user)
    conditions = [f"is_deleted = FALSE AND (is_active IS NULL OR is_active = TRUE) {bg_clause}"]
    params = list(bg_params)

    if q:
        conditions.append("AND LOWER(sku_name) LIKE @q")
        params.append(bq.p("q", "STRING", f"%{q.lower()}%"))
    if brand:
        conditions.append("AND brand = @brand")
        params.append(bq.p("brand", "STRING", brand))
    if category:
        conditions.append("AND category = @cat")
        params.append(bq.p("cat", "STRING", category))

    where = " ".join(conditions)
    offset = (page - 1) * page_size

    cache_key = f"sku:{current_user.brand_group or 'all'}:{q or ''}:{brand or ''}:{category or ''}:{page}:{page_size}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    total = (bq.query_one(
        f"SELECT COUNT(*) AS n FROM {settings.table('dim_sku')} WHERE {where}",
        params,
    ) or {}).get("n", 0)

    rows = bq.query(
        f"""
        SELECT sku_id, sku_name, brand, brand_group, category, stp, is_active
        FROM {settings.table('dim_sku')}
        WHERE {where}
        ORDER BY brand, sku_name
        LIMIT @lim OFFSET @off
        """,
        params + [bq.p("lim", "INT64", page_size), bq.p("off", "INT64", offset)],
    )

    result = SkuListResponse(
        items=[SkuOut(**r) for r in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_next=(offset + page_size) < total,
    )
    bq.cache.set(cache_key, result, ttl=300)  # 5 min — dim_sku changes only on master import
    return result
