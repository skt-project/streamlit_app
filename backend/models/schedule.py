from pydantic import BaseModel


class ScheduleStoreOut(BaseModel):
    route_plan_sk: str
    outlet_sk: str | None = None
    source_outlet_code: str
    store_name: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    brand: str | None = None
    brand_group: str | None = None
    store_grade: str | None = None
    visit_day_of_week: str | None = None
    visit_week_pattern: str | None = None
    visit_frequency_code: str | None = None
    distributor_code: str | None = None


class ScheduleDownloadResponse(BaseModel):
    salesman_sk: str
    week: str
    stores: list[ScheduleStoreOut]
    total: int
