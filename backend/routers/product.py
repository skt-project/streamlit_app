"""GET /product — product master for mobile demand entry.

Reads directly from gt_schema.master_product (the canonical product catalog).
Returns only fields needed by the mobile app:
  sku_id, sku_name, brand, brand_group (derived), category, stp (price_for_store)

brand_group is not stored in master_product; it is derived from brand using
the BRAND_GROUPS mapping defined in dependencies.py.
"""
from fastapi import APIRouter, Depends

from dependencies import BRAND_GROUPS, require_auth
from models.auth import UserContext
from services.bq import BQClient

router = APIRouter(prefix="/product", tags=["product"])

# Reverse-map brand → brand_group for enrichment
_BRAND_TO_GROUP: dict[str, str] = {
    brand: grp
    for grp, brands in BRAND_GROUPS.items()
    for brand in brands
}

_GT_PROJECT = "skintific-data-warehouse"
_GT_TABLE = f"`{_GT_PROJECT}.gt_schema.master_product`"


@router.get("")
def list_products(
    current_user: UserContext = Depends(require_auth),
):
    bq = BQClient.get()

    # Build brand-level filter from user's business group
    conditions: list[str] = []
    params: list = []

    if current_user.brand_group and current_user.role != "ho_admin":
        brands = BRAND_GROUPS.get(current_user.brand_group, [])
        if brands:
            placeholders = ", ".join(f"@pb_{i}" for i in range(len(brands)))
            conditions.append(f"brand IN ({placeholders})")
            for i, brand in enumerate(brands):
                params.append(bq.p(f"pb_{i}", "STRING", brand))
        else:
            # brand_group set but not recognised → no products visible
            return {"items": [], "total": 0}

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    rows = bq.query(
        f"""
        SELECT
            sku                                         AS sku_id,
            product_name                                AS sku_name,
            brand,
            category,
            COALESCE(price_for_store, srp, 0)          AS stp
        FROM {_GT_TABLE}
        {where}
        ORDER BY brand, product_name
        """,
        params,
    )

    items = []
    for r in rows:
        d = dict(r)
        d["brand_group"] = _BRAND_TO_GROUP.get(d.get("brand") or "", None)
        d["is_active"] = True
        items.append(d)

    return {"items": items, "total": len(items)}
