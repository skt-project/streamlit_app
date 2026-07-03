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


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserContext
