from pydantic import BaseModel


class SkuOut(BaseModel):
    sku_id: str
    sku_name: str
    brand: str | None = None
    brand_group: str | None = None
    category: str | None = None
    stp: float | None = None
    is_active: bool | None = None


class SkuListResponse(BaseModel):
    items: list[SkuOut]
    total: int
    page: int
    page_size: int
    has_next: bool
