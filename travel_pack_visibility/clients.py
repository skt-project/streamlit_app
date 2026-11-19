"""Kredensial + client Google dari secrets (atau Application Default Credentials). Tidak ada kredensial di kode."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

import google.auth
import gspread
from google.cloud import storage
from google.oauth2 import service_account

from config import AppConfig, load_config
from evidence import GcsEvidence
from responses import ResponseSheet
from service import Submitter

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/devstorage.read_write",
]


def _service_account_info(secrets: Mapping[str, Any]) -> dict | None:
    block = secrets.get("gcp_service_account")
    if block and block.get("private_key"):
        info = dict(block)
        info["private_key"] = str(info["private_key"]).replace("\\n", "\n")
        return info
    raw = secrets.get("GCP_SERVICE_ACCOUNT_JSON")
    if raw:
        return json.loads(raw)
    return None


def _credentials(secrets: Mapping[str, Any]):
    info = _service_account_info(secrets)
    if info:
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES), info.get("project_id")
    creds, project = google.auth.default(scopes=SCOPES)
    return creds, project


@dataclass
class Backends:
    config: AppConfig
    gc: Any
    responses: ResponseSheet
    submitter: Submitter


def build_backends(secrets: Mapping[str, Any]) -> Backends:
    cfg = load_config(secrets)
    creds, project = _credentials(secrets)
    gc = gspread.authorize(creds)
    storage_client = storage.Client(project=project or None, credentials=creds)
    responses = ResponseSheet(lambda: gc.open_by_key(cfg.output_sheet_id), cfg.output_worksheet)
    evidence = GcsEvidence(storage_client, cfg.evidence_bucket, cfg.evidence_prefix)
    return Backends(config=cfg, gc=gc, responses=responses, submitter=Submitter(evidence, responses))
