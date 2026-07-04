from datetime import datetime
from typing import Literal
from pydantic import BaseModel


class StockOut(BaseModel):
    stock_id: str
    salesman_sk: str
    sku_id: str
    sku_name: str | None = None
    brand: str | None = None
    brand_group: str | None = None
    stp: float | None = None
    qty_current: int
    assigned_by_sk: str | None = None
    updated_at: datetime | None = None


class StockRequestIn(BaseModel):
    sku_id: str
    qty_requested: int
    notes_se: str | None = None


class StockRequestApproveIn(BaseModel):
    qty_approved: int
    notes_spv: str | None = None


class StockRequestRejectIn(BaseModel):
    notes_spv: str


class StockRequestOut(BaseModel):
    request_id: str
    salesman_sk: str
    spv_sk: str
    sku_id: str
    sku_name: str | None = None
    qty_requested: int
    qty_approved: int | None = None
    status: str
    notes_se: str | None = None
    notes_spv: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
