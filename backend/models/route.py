from pydantic import BaseModel


class RouteOutlet(BaseModel):
    route_plan_sk: str
    outlet_sk: str | None = None
    source_outlet_code: str
    store_name: str | None = None
    address: str | None = None
    brand: str | None = None
    store_grade: str | None = None
    visit_day_of_week: str | None = None
    visit_frequency_code: str | None = None
    visit_week_pattern: str | None = None


class WeeklyRoutePlan(BaseModel):
    salesman_sk: str
    salesman_name: str | None = None
    distributor_code: str | None = None
    week_start: str  # ISO date string for Monday
    week_label: str  # e.g. "Week 27 · 30 Jun – 5 Jul 2026"
    is_odd_week: bool
    days: dict[str, list[RouteOutlet]]  # key = Indonesian day name


class SalesmanMiniOut(BaseModel):
    salesman_sk: str
    source_salesman_code: str
    salesman_name: str | None = None
    distributor_code: str | None = None
    region: str | None = None
    spv_name: str | None = None
    is_active: bool | None = None
    brand_group: str | None = None
