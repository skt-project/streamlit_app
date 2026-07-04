from pydantic import BaseModel


class LoginRequest(BaseModel):
    username: str
    password: str


class UserContext(BaseModel):
    user_id: str
    username: str
    role: str
    territory: str | None = None
    distributor_code: str | None = None
    brand_group: str | None = None  # 'SKT' | 'G2G' | None (ho_admin sees all)
    salesman_sk: str | None = None  # FK → sfa_web.dim_salesman (STRING hash); set for SE/SPV users


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserContext
