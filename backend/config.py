from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    bq_project: str = "skintific-data-warehouse"
    bq_dataset: str = "sfa_web"
    bq_sa_key_path: str = ""      # local dev: path to JSON key file
    bq_sa_key_json: str = ""      # cloud deploy: base64-encoded JSON key content

    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = 24

    cors_origins: str = "https://step.netlify.app http://localhost:8080 http://localhost:8081 http://localhost:19006 http://localhost:19000"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split() if o.strip()]

    # Logical → physical table name overrides.
    # fact_visit already exists with a different source-sync schema; use step_visit for SFA.
    _TABLE_ALIASES: dict = {
        "fact_visit":          "step_visit",
        "fact_visit_item":     "step_visit_item",
        "fact_visit_revision": "step_visit_revision",
    }

    def table(self, name: str) -> str:
        physical = self._TABLE_ALIASES.get(name, name)
        return f"`{self.bq_project}.{self.bq_dataset}.{physical}`"


settings = Settings()
