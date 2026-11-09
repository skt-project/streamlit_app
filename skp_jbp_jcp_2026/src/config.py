"""Settings from Streamlit Secrets (or environment variables) and run-mode resolution.

No credential or private key is stored in code. Error messages about the
service-account JSON never include its contents.
"""
from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field

KEYS = (
    "GCP_PROJECT_ID",
    "SOURCE_SPREADSHEET_ID",
    "SOURCE_SHEET_NAME",
    "LOG_SHEET_NAME",
    "GCS_BUCKET",
    "ADMIN_USERNAME",
    "ADMIN_PASSWORD",
    "APP_MODE",
    "LOCAL_DATA_DIR",
)

MODE_AUTO, MODE_LIVE, MODE_MOCK = "auto", "live", "mock"

SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/devstorage.read_write",
)


@dataclass(frozen=True)
class Settings:
    gcp_project_id: str = ""
    source_spreadsheet_id: str = ""
    source_sheet_name: str = "Template"
    log_sheet_name: str = "SKP_UPLOAD_LOG"
    gcs_bucket: str = ""
    admin_username: str = field(default="", repr=False)
    admin_password: str = field(default="", repr=False)
    app_mode: str = MODE_AUTO
    local_data_dir: str = ".local_data"
    service_account_info: dict | None = field(default=None, repr=False)
    service_account_error: str = ""

    @property
    def service_account_email(self) -> str:
        return (self.service_account_info or {}).get("client_email", "")

    @property
    def admin_configured(self) -> bool:
        return bool(self.admin_username and self.admin_password)


def parse_service_account(raw: object) -> tuple[dict | None, str]:
    """Accept a JSON string or a TOML table. Returns (info, error_message)."""
    if raw is None or raw == "":
        return None, ""
    if isinstance(raw, Mapping):
        info = dict(raw)
    else:
        try:
            info = json.loads(str(raw), strict=False)  # tolerate real newlines in private_key
        except json.JSONDecodeError as exc:
            return None, (
                f"GCP_SERVICE_ACCOUNT_JSON bukan JSON yang valid (baris {exc.lineno}, kolom {exc.colno}). "
                "Tempel ulang isi file JSON service account di antara tanda ''' ... '''."
            )
        if not isinstance(info, dict):
            return None, "GCP_SERVICE_ACCOUNT_JSON harus berupa objek JSON."
    if info.get("type") != "service_account":
        return None, "GCP_SERVICE_ACCOUNT_JSON harus bertipe \"service_account\"."
    missing = [k for k in ("client_email", "private_key", "token_uri") if not info.get(k)]
    if missing:
        return None, "GCP_SERVICE_ACCOUNT_JSON belum lengkap (tidak ada: " + ", ".join(missing) + ")."
    return info, ""


def settings_from_mapping(secrets: Mapping, env: Mapping | None = None) -> Settings:
    env = env if env is not None else os.environ
    values: dict[str, str] = {}
    for key in KEYS:
        value = secrets.get(key) if key in secrets else env.get(key)
        if value is not None and str(value).strip() != "":
            values[key] = str(value).strip()

    raw_sa = secrets.get("GCP_SERVICE_ACCOUNT_JSON") or secrets.get("gcp_service_account") or env.get("GCP_SERVICE_ACCOUNT_JSON")
    info, error = parse_service_account(raw_sa)

    mode = values.get("APP_MODE", MODE_AUTO).lower()
    if mode not in (MODE_AUTO, MODE_LIVE, MODE_MOCK):
        mode = MODE_AUTO
    return Settings(
        gcp_project_id=values.get("GCP_PROJECT_ID", info.get("project_id", "") if info else ""),
        source_spreadsheet_id=values.get("SOURCE_SPREADSHEET_ID", ""),
        source_sheet_name=values.get("SOURCE_SHEET_NAME", "Template"),
        log_sheet_name=values.get("LOG_SHEET_NAME", "SKP_UPLOAD_LOG"),
        gcs_bucket=values.get("GCS_BUCKET", ""),
        admin_username=values.get("ADMIN_USERNAME", ""),
        admin_password=values.get("ADMIN_PASSWORD", ""),
        app_mode=mode,
        local_data_dir=values.get("LOCAL_DATA_DIR", ".local_data"),
        service_account_info=info,
        service_account_error=error,
    )


def load_settings() -> Settings:
    """Read Streamlit Secrets; works without a secrets.toml (falls back to env vars)."""
    try:
        import streamlit as st

        secrets: Mapping = {k: st.secrets[k] for k in st.secrets.keys()}
    except Exception:  # no secrets.toml, or not running under Streamlit
        secrets = {}
    return settings_from_mapping(secrets)


@dataclass(frozen=True)
class RuntimeMode:
    """Which backends the app uses and whether uploading is allowed."""

    mock: bool
    source_live: bool
    can_upload: bool
    problems: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return "mock" if self.mock else "live"


def resolve_mode(settings: Settings) -> RuntimeMode:
    """auto: live when a service account is configured, otherwise mock.

    In live mode a missing bucket or spreadsheet blocks uploads with a clear
    message instead of silently falling back to local files (which would be
    lost on Streamlit Cloud)."""
    has_sa = settings.service_account_info is not None
    has_sheet = bool(settings.source_spreadsheet_id)
    mock = settings.app_mode == MODE_MOCK or (settings.app_mode == MODE_AUTO and not has_sa)

    problems: list[str] = []
    if settings.service_account_error:
        problems.append(settings.service_account_error)
    if mock:
        return RuntimeMode(mock=True, source_live=has_sa and has_sheet, can_upload=True, problems=tuple(problems))

    if not has_sa:
        problems.append("Service account belum dikonfigurasi (GCP_SERVICE_ACCOUNT_JSON).")
    if not has_sheet:
        problems.append("SOURCE_SPREADSHEET_ID belum diisi.")
    if not settings.gcs_bucket:
        problems.append("GCS_BUCKET belum diisi. Upload dinonaktifkan sampai bucket dikonfigurasi.")
    return RuntimeMode(
        mock=False,
        source_live=has_sa and has_sheet,
        can_upload=has_sa and has_sheet and bool(settings.gcs_bucket),
        problems=tuple(problems),
    )
