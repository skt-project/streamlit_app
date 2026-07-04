from datetime import date, datetime
from pydantic import BaseModel


class SalesmanOut(BaseModel):
    salesman_sk: str
    source_salesman_code: str
    salesman_name: str | None = None
    salesman_type: str | None = None
    role_type: str
    distributor_code: str | None = None
    region: str | None = None
    spv_name: str | None = None
    asm_name: str | None = None
    is_active: bool | None = None
    brand_group: str | None = None
    source_updated_at: datetime | None = None


class SalesmanListResponse(BaseModel):
    items: list[SalesmanOut]
    total: int
    page: int
    page_size: int
    has_next: bool


class SalesmanCreateRequest(BaseModel):
    source_salesman_code: str
    salesman_name: str
    salesman_type: str = "GTI"
    role_type: str = "SALESMAN"
    distributor_code: str
    region: str | None = None
    spv_name: str | None = None
    asm_name: str | None = None


class SalesmanUpdateRequest(BaseModel):
    salesman_name: str | None = None
    salesman_type: str | None = None
    distributor_code: str | None = None
    region: str | None = None
    spv_name: str | None = None
    asm_name: str | None = None
    is_active: bool | None = None
