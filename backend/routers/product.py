"""GET /product — product master for mobile demand entry.

Reads directly from gt_schema.master_product (the canonical product catalog).
Returns only fields needed by the mobile app:
  sku_id, sku_name, brand, brand_group (derived), category, stp (price_for_store)

Access control (TASK 3):
  - Normal salesman  → only products whose brand belongs to their brand_group
  - ho_admin / no brand_group → all products (demo and admin accounts)
  - brand_group set but unknown → no products (AND 1=0 from brand_list_filter)
"""
from fastapi import APIRouter, Depends

from config import settings
from dependencies import BRAND_GROUPS, brand_list_filter, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/product", tags=["product"])

# Reverse-map brand → brand_group for response enrichment.
# brand_group is not stored in master_product — it is derived here.
_BRAND_TO_GROUP: dict[str, str] = {
    brand: grp
    for grp, brands in BRAND_GROUPS.items()
    for brand in brands
}

# gt_schema is the read-only canonical product catalog (separate from sfa_web).
_GT_TABLE = f"`{settings.bq_project}.gt_schema.master_product`"


@router.get("")
def list_products(current_user: UserContext = Depends(require_auth)):
    bq = BQClient.get()

    # brand_list_filter handles all cases:
    #   ho_admin / brand_group=None → ("", [])      → no WHERE restriction
    #   known brand_group           → ("AND brand IN (...)", [params])
    #   unknown brand_group         → ("AND 1=0", []) → empty result set
    bg_clause, bg_params = brand_list_filter(
        current_user, col="brand", param_prefix="pb"
    )

    cache_key = f"product:{current_user.brand_group or 'all'}"
    cached = bq.cache.get(cache_key)
    if cached is not None:
        return cached

    rows = bq.query(
        f"""
        SELECT
            sku                                 AS sku_id,
            product_name                        AS sku_name,
            brand,
            category,
            COALESCE(price_for_store, srp, 0)  AS stp
        FROM {_GT_TABLE}
        WHERE 1=1 {bg_clause}
        ORDER BY brand, product_name
        """,
        bg_params,
    )

    items = []
    for r in rows:
        d = dict(r)
        d["brand_group"] = _BRAND_TO_GROUP.get(d.get("brand") or "", None)
        d["is_active"] = True
        items.append(d)

    result = {"items": items, "total": len(items)}
    bq.cache.set(cache_key, result, ttl=300)  # 5 min — read-only GT table, stable between imports
    return result
