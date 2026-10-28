"""Konfigurasi aplikasi dari secrets (tidak ada nilai rahasia di kode)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

DEFAULT_OUTPUT_WORKSHEET = "Distributor Stock"
DEFAULT_TABLE = "timephoria_smart_coverage_tester_feedback"
DEFAULT_EVIDENCE_ROOT = "Timephoria Distributor Stock Evidence"
DEFAULT_HO_WORKSHEET = "Opening Stock"

_SHEET_URL_RE = re.compile(r"/spreadsheets/d/([A-Za-z0-9_-]+)")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_PROJECT_RE = re.compile(r"^[A-Za-z0-9_:.-]+$")
_SKU_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class ConfigError(Exception):
    """Secrets tidak lengkap / tidak valid. Pesan aman ditampilkan ke user."""


def extract_sheet_id(value: str) -> str:
    """Terima ID polos atau URL Google Sheets lengkap, kembalikan ID-nya."""
    value = (value or "").strip()
    match = _SHEET_URL_RE.search(value)
    return match.group(1) if match else value


@dataclass(frozen=True)
class AppConfig:
    output_sheet_id: str
    bigquery_project: str
    bigquery_dataset: str
    bigquery_table: str
    evidence_bucket: str
    evidence_root_folder: str
    output_worksheet: str
    sku_codes: tuple[str, ...]
    ho_stock_worksheet: str = DEFAULT_HO_WORKSHEET
    admin_username: str = ""
    admin_password: str = field(default="", repr=False)

    @property
    def table_fqn(self) -> str:
        return f"{self.bigquery_project}.{self.bigquery_dataset}.{self.bigquery_table}"


def _sku_codes(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",")]
    elif raw is None:
        parts = []
    else:
        parts = [str(p).strip() for p in raw]
    seen: set[str] = set()
    out: list[str] = []
    for code in parts:
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return tuple(out)


def load_config(secrets: Mapping[str, Any], fallback_project: str = "") -> AppConfig:
    """Bangun AppConfig dari bagian `[app]` pada secrets."""
    app = dict(secrets.get("app", {}) or {})

    def get(key: str, default: str = "") -> str:
        return str(app.get(key, default) or default).strip()

    admin = secrets.get("admin", {}) or {}
    cfg = AppConfig(
        output_sheet_id=extract_sheet_id(get("output_google_sheet_id")),
        bigquery_project=get("bigquery_project", fallback_project),
        bigquery_dataset=get("bigquery_dataset", "gt_schema"),
        bigquery_table=get("bigquery_table", DEFAULT_TABLE),
        evidence_bucket=get("evidence_bucket"),
        evidence_root_folder=get("evidence_root_folder", DEFAULT_EVIDENCE_ROOT),
        output_worksheet=get("output_worksheet", DEFAULT_OUTPUT_WORKSHEET),
        sku_codes=_sku_codes(app.get("sku_codes")),
        ho_stock_worksheet=get("ho_stock_worksheet", DEFAULT_HO_WORKSHEET),
        admin_username=str(admin.get("username", "") or "").strip(),
        admin_password=str(admin.get("password", "") or ""),
    )
    missing = [
        name for name, val in (
            ("output_google_sheet_id", cfg.output_sheet_id),
            ("bigquery_project", cfg.bigquery_project),
            ("evidence_bucket", cfg.evidence_bucket),
            ("sku_codes", cfg.sku_codes),
        ) if not val
    ]
    if missing:
        raise ConfigError(f"Secrets [app] belum lengkap: {', '.join(missing)}")
    if not _PROJECT_RE.match(cfg.bigquery_project):
        raise ConfigError("bigquery_project mengandung karakter tidak valid.")
    if not re.match(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$", cfg.evidence_bucket):
        raise ConfigError("evidence_bucket bukan nama bucket GCS yang valid.")
    for label, val in (("bigquery_dataset", cfg.bigquery_dataset), ("bigquery_table", cfg.bigquery_table)):
        if not _IDENT_RE.match(val):
            raise ConfigError(f"{label} hanya boleh huruf, angka, '_' dan '-'.")
    bad = [c for c in cfg.sku_codes if not _SKU_RE.match(c)]
    if bad:
        raise ConfigError("sku_codes berisi kode yang tidak valid.")
    return cfg
