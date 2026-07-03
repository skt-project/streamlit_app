from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    bq_project: str = "skintific-data-warehouse"
    bq_dataset: str = "sfa_web"
    bq_sa_key_path: str = ""  # empty = use Application Default Credentials (Cloud Run)

    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = 24

    cors_origins: str = "http://localhost:8080"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split() if o.strip()]

    def table(self, name: str) -> str:
        return f"`{self.bq_project}.{self.bq_dataset}.{name}`"


settings = Settings()
