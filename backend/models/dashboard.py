from pydantic import BaseModel


class KpiOut(BaseModel):
    total_visits: int = 0
    effective_calls: int = 0
    strike_rate: float = 0.0          # effective_calls / total_visits * 100
    total_demand: float = 0.0
    pending_approvals: int = 0
    revision_count: int = 0
    route_completion_pct: float = 0.0  # checked-in / scheduled * 100
    date: str | None = None


class TeamMemberKpi(BaseModel):
    salesman_sk: str
    salesman_name: str | None = None
    total_visits: int = 0
    effective_calls: int = 0
    strike_rate: float = 0.0
    total_demand: float = 0.0
    pending_approvals: int = 0


class TeamKpiResponse(BaseModel):
    members: list[TeamMemberKpi]
    total_members: int
