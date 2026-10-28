"""Pembuatan kredensial + client Google dari secrets / ADC. Tidak ada kredensial di kode."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import google.auth
import gspread
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from bigquery_store import BigQueryStore
from config import AppConfig, load_config
from gcs_evidence import GcsEvidence
from sheets_output import SheetsMirror

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/devstorage.read_write",
]


def _service_account_info(secrets: Mapping[str, Any]) -> dict | None:
    for section in ("gcp_service_account", "connections"):
        block = secrets.get(section)
        if section == "connections" and block:
            block = (block or {}).get("bigquery")
        if block and block.get("private_key"):
            info = dict(block)
            info["private_key"] = str(info["private_key"]).replace("\\n", "\n")
            return info
    return None


def _base_credentials(secrets: Mapping[str, Any]):
    info = _service_account_info(secrets)
    if info:
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES), info.get("project_id", "")
    creds, project = google.auth.default(scopes=SCOPES)
    return creds, project or ""


@dataclass
class Backends:
    config: AppConfig
    gc: gspread.Client
    bq_client: bigquery.Client
    bq_store: BigQueryStore
    mirror: SheetsMirror
    evidence: GcsEvidence


def build_backends(secrets: Mapping[str, Any]) -> Backends:
    base, project = _base_credentials(secrets)
    cfg = load_config(secrets, fallback_project=project)
    gc = gspread.authorize(base)
    bq_client = bigquery.Client(project=cfg.bigquery_project, credentials=base)
    storage_client = storage.Client(project=cfg.bigquery_project, credentials=base)
    return Backends(
        config=cfg,
        gc=gc,
        bq_client=bq_client,
        bq_store=BigQueryStore(bq_client, cfg.table_fqn),
        mirror=SheetsMirror(lambda: gc.open_by_key(cfg.output_sheet_id), cfg.output_worksheet),
        evidence=GcsEvidence(storage_client, cfg.evidence_bucket, cfg.evidence_root_folder),
    )
