from datetime import date, datetime
from typing import Literal
from pydantic import BaseModel, Field


VisitType = Literal["ROUTE", "NON_ROUTE"]
VisitStatus = Literal["DRAFT", "CHECKED_IN", "CHECKED_OUT", "SUBMITTED"]
ApprovalStatus = Literal[
    "DRAFT", "SUBMITTED", "PENDING_SPV",
    "SPV_APPROVED", "ASM_APPROVED", "DDM_APPROVED",
    "REVISION_REQUIRED", "COMPLETED", "REJECTED",
]


class CheckinRequest(BaseModel):
    salesman_sk: str
    outlet_sk: str
    visit_date: date
    visit_type: VisitType = "ROUTE"
    checkin_latitude: float | None = None
    checkin_longitude: float | None = None
    checkin_photo_url: str | None = None
    schedule_id: str | None = None
    offline_mode: bool = False
    captured_at: datetime | None = None   # local device timestamp


class VisitItemIn(BaseModel):
    sku_id: str
    sku_name: str | None = None
    brand: str | None = None
    brand_group: str | None = None
    category: str | None = None
    stp: float = 0.0
    qty: int = 0


class CheckoutRequest(BaseModel):
    checkout_latitude: float | None = None
    checkout_longitude: float | None = None
    checkout_photo_url: str | None = None
    notes: str | None = None
    total_demand: float = 0.0
    effective_call: Literal["YES", "NO"] = "NO"
    items: list[VisitItemIn] = Field(default_factory=list)
    offline_mode: bool = False
    captured_at: datetime | None = None


class SubmitRequest(BaseModel):
    offline_mode: bool = False


class ApproveRequest(BaseModel):
    notes: str | None = None


class RejectRequest(BaseModel):
    rejection_notes: str


class ResubmitRequest(BaseModel):
    total_demand: float = 0.0
    notes: str | None = None
    checkout_photo_url: str | None = None
    items: list[VisitItemIn] = Field(default_factory=list)


class VisitItemOut(BaseModel):
    visit_item_id: str
    sku_id: str
    sku_name: str | None = None
    brand: str | None = None
    category: str | None = None
    stp: float | None = None
    qty: int | None = None
    demand: float | None = None


class VisitOut(BaseModel):
    visit_id: str
    salesman_sk: str
    outlet_sk: str | None = None
    schedule_id: str | None = None
    visit_date: date
    visit_type: str
    brand_group: str | None = None

    checkin_time: datetime | None = None
    checkin_latitude: float | None = None
    checkin_longitude: float | None = None
    checkin_photo_url: str | None = None
    checkin_distance_m: float | None = None
    gps_warning: bool = False

    checkout_time: datetime | None = None
    checkout_latitude: float | None = None
    checkout_longitude: float | None = None
    checkout_photo_url: str | None = None

    total_demand: float | None = None
    effective_call: str | None = None
    notes: str | None = None
    duration_minutes: int | None = None

    visit_status: str | None = None
    approval_status: str | None = None

    spv_username: str | None = None
    spv_approved_at: datetime | None = None
    asm_username: str | None = None
    asm_approved_at: datetime | None = None
    ddm_username: str | None = None
    ddm_approved_at: datetime | None = None

    rejection_notes: str | None = None
    revision_count: int | None = None

    created_at: datetime | None = None
    updated_at: datetime | None = None
    items: list[VisitItemOut] = Field(default_factory=list)


class VisitListResponse(BaseModel):
    items: list[VisitOut]
    total: int
    page: int
    page_size: int
    has_next: bool


class CheckinResponse(BaseModel):
    visit_id: str
    checkin_distance_m: float | None = None
    gps_warning: bool = False
    offline_mode: bool = False
