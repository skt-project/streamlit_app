"""Konfigurasi aplikasi dari secrets (tidak ada nilai rahasia di kode)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

DEFAULT_OUTPUT_WORKSHEET = "Store Sales"

_SHEET_URL_RE = re.compile(r"/spreadsheets/d/([A-Za-z0-9_-]+)")
_IDENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")           # project id / dataset / table
_PROJECT_RE = re.compile(r"^[A-Za-z0-9_:.-]+$")


class ConfigError(Exception):
    """Secrets tidak lengkap / tidak valid. Pesan aman ditampilkan ke user."""


def extract_sheet_id(value: str) -> str:
    """Terima ID polos atau URL Google Sheets lengkap, kembalikan ID-nya."""
    value = (value or "").strip()
    m = _SHEET_URL_RE.search(value)
    return m.group(1) if m else value


@dataclass(frozen=True)
class AppConfig:
    master_sheet_id: str
    output_sheet_id: str
    bigquery_project: str
    bigquery_dataset: str
    bigquery_table: str
    evidence_bucket: str               # bucket GCS untuk foto evidence
    evidence_root_folder: str          # prefix/folder utama di dalam bucket
    store_master_worksheet: str
    sku_master_worksheet: str
    output_worksheet: str
    # Login tab Admin (dari secrets [admin]); kosong = tab admin nonaktif. repr=False agar tidak bocor ke log.
    admin_username: str = field(default="", repr=False)
    admin_password: str = field(default="", repr=False)

    @property
    def table_fqn(self) -> str:
        return f"{self.bigquery_project}.{self.bigquery_dataset}.{self.bigquery_table}"


def load_config(secrets: Mapping[str, Any], fallback_project: str = "") -> AppConfig:
    """Bangun AppConfig dari bagian `[app]` pada secrets."""
    app = dict(secrets.get("app", {}) or {})

    def get(key: str, default: str = "") -> str:
        return str(app.get(key, default) or default).strip()

    cfg = AppConfig(
        master_sheet_id=extract_sheet_id(get("master_google_sheet_id")),
        output_sheet_id=extract_sheet_id(get("output_google_sheet_id")),
        bigquery_project=get("bigquery_project", fallback_project),
        bigquery_dataset=get("bigquery_dataset", "gt_schema"),
        bigquery_table=get("bigquery_table", "store_sales_transaction"),
        evidence_bucket=get("evidence_bucket"),
        evidence_root_folder=get("evidence_root_folder", get("drive_root_folder_name", "Store Sales Evidence")),
        store_master_worksheet=get("store_master_worksheet", "Store Master"),
        sku_master_worksheet=get("sku_master_worksheet", "SKU Master"),
        output_worksheet=get("output_worksheet", DEFAULT_OUTPUT_WORKSHEET),
        admin_username=str((secrets.get("admin", {}) or {}).get("username", "") or "").strip(),
        admin_password=str((secrets.get("admin", {}) or {}).get("password", "") or ""),
    )
    missing = [
        name for name, val in (
            ("master_google_sheet_id", cfg.master_sheet_id),
            ("output_google_sheet_id", cfg.output_sheet_id),
            ("bigquery_project", cfg.bigquery_project),
            ("evidence_bucket", cfg.evidence_bucket),
        ) if not val
    ]
    if missing:
        raise ConfigError(f"Secrets [app] belum lengkap: {', '.join(missing)}")
    # Nama tabel tidak bisa di-parameterize di BigQuery, jadi dibatasi karakternya.
    if not _PROJECT_RE.match(cfg.bigquery_project):
        raise ConfigError("bigquery_project mengandung karakter tidak valid.")
    if not re.match(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$", cfg.evidence_bucket):
        raise ConfigError("evidence_bucket bukan nama bucket GCS yang valid.")
    for label, val in (("bigquery_dataset", cfg.bigquery_dataset), ("bigquery_table", cfg.bigquery_table)):
        if not _IDENT_RE.match(val):
            raise ConfigError(f"{label} hanya boleh huruf, angka, '_' dan '-'.")
    return cfg
