"""Use-case layer: plan documents, submit (GCS + log), admin aggregations.

Kept free of Streamlit so the whole submit flow is testable with fakes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from src.sheets import (
    COL_BRAND,
    COL_CODE,
    COL_NAME,
    COL_PROGRAM,
    COL_REGION,
    LOG_KEY,
    LogStore,
    Store,
    log_key,
    now_wib,
)
from src.storage import DocumentStorage, StorageError
from src.validation import (
    FileCheck,
    build_object_path,
    build_target_filename,
    clean_cell,
    filename_warning,
    validate_pdf,
    validate_pic,
)

log = logging.getLogger("skp.service")

STATUS_NOT_UPLOADED = "Belum diupload"
NOTE_NAME_CHECK = "cek nama file"


@dataclass
class PlannedDocument:
    brand: str
    program: str
    quarter: str
    skp_number: str
    original_filename: str
    data: bytes
    target_filename: str
    object_path: str
    check: FileCheck
    name_warning: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.brand, self.program, self.quarter)


def plan_document(store: Store, brand: str, program: str, quarter: str, skp_number: str,
                  original_filename: str, data: bytes) -> PlannedDocument:
    target = build_target_filename(store.name, skp_number, brand, program, quarter)
    path = build_object_path(quarter, program, store.region, target)
    check = validate_pdf(original_filename, data)
    warning = filename_warning(original_filename, skp_number) if check.ok else None
    if warning:
        check.warnings.append(warning)
    return PlannedDocument(brand, program, quarter, clean_cell(skp_number), original_filename, data,
                           target, path, check, warning)


def resolve_path_collisions(store: Store, docs: list[PlannedDocument], logs: pd.DataFrame) -> list[PlannedDocument]:
    """Two different combinations must never share one object (that would
    overwrite another document). On a clash the brand is added to the name."""
    owner: dict[str, tuple[str, ...]] = {}
    if not logs.empty:
        for rec in logs.to_dict("records"):
            if rec.get("GCS Path"):
                owner.setdefault(rec["GCS Path"], log_key(rec))
    for doc in docs:
        key = log_key({COL_CODE: store.code, COL_BRAND: doc.brand, COL_PROGRAM: doc.program, "Kuartal": doc.quarter})
        holder = owner.get(doc.object_path)
        if holder is not None and holder != key:
            doc.target_filename = doc.target_filename[:-4] + f" ({doc.brand}).pdf"
            doc.object_path = build_object_path(doc.quarter, doc.program, store.region, doc.target_filename)
        owner.setdefault(doc.object_path, key)
    return docs


@dataclass
class SubmitResult:
    saved: list[dict] = field(default_factory=list)
    failed: list[tuple[PlannedDocument, str]] = field(default_factory=list)
    log_error: str | None = None
    removed_old: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return bool(self.saved) and not self.failed and self.log_error is None


def submit_documents(store: Store, pic: str, docs: list[PlannedDocument], storage: DocumentStorage,
                     log_store: LogStore, uploader: str | None = None, timestamp: str | None = None) -> SubmitResult:
    """Upload every valid document, then upsert one log row per document.

    Upload and log are both idempotent for a given combination, so a retry after
    a partial failure is always safe."""
    pic_error = validate_pic(pic)
    if pic_error:
        raise ValueError(pic_error)
    if not docs:
        raise ValueError("Tidak ada dokumen untuk diupload.")
    invalid = [d for d in docs if not d.check.ok]
    if invalid:
        raise ValueError(f"{len(invalid)} dokumen belum valid.")

    pic = clean_cell(pic)
    stamp = timestamp or now_wib()
    result = SubmitResult()
    records: list[dict] = []
    for doc in docs:
        try:
            storage.upload_pdf(doc.object_path, doc.data)
        except StorageError as exc:
            result.failed.append((doc, str(exc)))
            continue
        except Exception as exc:  # noqa: BLE001
            log.exception("Unexpected upload error")
            result.failed.append((doc, f"Upload gagal ({type(exc).__name__})."))
            continue
        records.append({
            "Timestamp": stamp,
            "PIC Submit": pic,
            COL_CODE: store.code,
            COL_NAME: store.name,
            COL_REGION: store.region,
            COL_BRAND: doc.brand,
            COL_PROGRAM: doc.program,
            "Kuartal": doc.quarter,
            "Nomor SKP": doc.skp_number,
            "Nama File": doc.target_filename,
            "GCS Path": doc.object_path,
            "Uploader": uploader or pic,
            "_note": NOTE_NAME_CHECK if doc.name_warning and doc.skp_number else "",
        })
    if not records:
        return result

    try:
        plan = log_store.upsert(records)
    except Exception as exc:  # noqa: BLE001
        log.warning("Log write failed: %s", type(exc).__name__)
        result.log_error = (
            "File sudah tersimpan, tetapi metadata gagal ditulis ke spreadsheet "
            f"({type(exc).__name__}). Silakan submit ulang; file akan ditimpa, tidak dobel."
        )
        already_failed = {id(f) for f, _ in result.failed}
        result.failed.extend((d, result.log_error) for d in docs if id(d) not in already_failed)
        return result

    for rec in records:
        key = log_key(rec)
        rec["Status"] = ("Replaced" if key in plan.previous else "Uploaded") + (f" ({rec['_note']})" if rec["_note"] else "")
        result.saved.append({k: v for k, v in rec.items() if not k.startswith("_")})
        old_path = plan.previous.get(key, {}).get("GCS Path", "")
        # Same combination, new object name (e.g. SKP number corrected in the
        # source sheet): remove the superseded file so one combination = one file.
        if old_path and old_path != rec["GCS Path"] and old_path not in plan.paths_in_use:
            try:
                if storage.delete(old_path):
                    result.removed_old.append(old_path)
            except Exception as exc:  # noqa: BLE001 - cleanup must never fail a submit
                log.warning("Could not remove superseded object: %s", type(exc).__name__)
    return result


# --------------------------------------------------------------------------
# Admin
# --------------------------------------------------------------------------

def dashboard_stats(logs: pd.DataFrame) -> dict:
    """Headline numbers. 'Total upload' = submit sessions (rows written together
    share Timestamp + PIC + store); 'Total dokumen' = stored combinations."""
    if logs.empty:
        return {"uploads": 0, "stores": 0, "documents": 0, "regions": 0, "brands": 0}
    return {
        "uploads": int(logs[["Timestamp", "PIC Submit", COL_CODE]].drop_duplicates().shape[0]),
        "stores": int(logs[COL_CODE].str.upper().nunique()),
        "documents": int(logs["GCS Path"].replace("", pd.NA).dropna().nunique()),
        "regions": int(logs[COL_REGION].replace("", pd.NA).dropna().nunique()),
        "brands": int(logs[COL_BRAND].replace("", pd.NA).dropna().nunique()),
    }


def count_by(logs: pd.DataFrame, column: str) -> pd.DataFrame:
    if logs.empty:
        return pd.DataFrame(columns=[column, "Dokumen"])
    values = logs[column].replace("", "(kosong)")
    out = values.value_counts().rename_axis(column).reset_index(name="Dokumen")
    return out.sort_values(column).reset_index(drop=True)


def compliance_frame(logs: pd.DataFrame, expected: pd.DataFrame) -> pd.DataFrame:
    """Uploaded documents plus every SKP in the source sheet that has no upload yet."""
    columns = [COL_CODE, COL_NAME, COL_REGION, COL_BRAND, COL_PROGRAM, "Kuartal", "Nomor SKP",
               "Nama File", "PIC Submit", "Timestamp", "Status", "GCS Path"]
    uploaded = logs.copy() if not logs.empty else pd.DataFrame(columns=columns)
    for c in columns:
        if c not in uploaded.columns:
            uploaded[c] = ""
    uploaded = uploaded[columns]
    if expected is None or expected.empty:
        return uploaded.reset_index(drop=True)
    have = {log_key(r) for r in uploaded.to_dict("records")}
    missing = [r for r in expected.to_dict("records") if log_key(r) not in have]
    pending = pd.DataFrame(missing)
    if pending.empty:
        return uploaded.reset_index(drop=True)
    for c in columns:
        if c not in pending.columns:
            pending[c] = ""
    pending["Status"] = STATUS_NOT_UPLOADED
    return pd.concat([uploaded, pending[columns]], ignore_index=True)


def apply_filters(df: pd.DataFrame, regions=(), quarters=(), programs=(), brands=(), store_query: str = "",
                  status: str = "Semua") -> pd.DataFrame:
    out = df
    if regions:
        out = out[out[COL_REGION].isin(regions)]
    if quarters:
        out = out[out["Kuartal"].isin(quarters)]
    if programs:
        out = out[out[COL_PROGRAM].isin(programs)]
    if brands:
        out = out[out[COL_BRAND].isin(brands)]
    q = clean_cell(store_query).upper()
    if q:
        out = out[out[COL_NAME].str.upper().str.contains(q, regex=False) | out[COL_CODE].str.upper().str.contains(q, regex=False)]
    if status == "Sudah upload":
        out = out[out["Status"] != STATUS_NOT_UPLOADED]
    elif status == "Belum upload":
        out = out[out["Status"] == STATUS_NOT_UPLOADED]
    return out


def uploads_for_store(logs: pd.DataFrame, code: str) -> dict[tuple[str, str, str], dict]:
    """(brand, program, quarter) -> latest log record for one store."""
    if logs.empty:
        return {}
    rows = logs[logs[COL_CODE].str.upper() == clean_cell(code).upper()]
    return {(r[COL_BRAND], r[COL_PROGRAM], r["Kuartal"]): r for r in rows.to_dict("records")}


__all__ = [
    "LOG_KEY",
    "PlannedDocument",
    "SubmitResult",
    "apply_filters",
    "compliance_frame",
    "count_by",
    "dashboard_stats",
    "plan_document",
    "resolve_path_collisions",
    "submit_documents",
    "uploads_for_store",
]
