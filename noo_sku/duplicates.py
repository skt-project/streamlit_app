"""Row-level duplicate classification, computed on ENRICHED pool rows.

Ordering matters and is mandated: validation → enrichment → normalisation →
identity resolution → duplicate detection. Classifying raw input would miss the
case where two differently-typed uploads enrich to the same final record.

Two comparisons, both scoped to the authenticated distributor so one DB's rows
can never collide with another's:

    identity key   "is this the same store / the same mapping?"
    content hash   "is the business content identical to a previous submission?"

Excluded from the content hash:

* ``input_time`` — system-generated; a later timestamp must never make an
  otherwise identical row look new (brief §11, §13).
* volatile store enrichment (``se_kae``, ``spv``, ``aom``, ``area``,
  ``province``, ASM/region) — these are looked up at submission time and their
  availability drifts as master data catches up with new stores. Hashing them
  would turn an unchanged re-submission into a spurious CORRECTION the week
  after the store lands in the master. Identity and every user-entered business
  field remain hashed.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from . import config
from .normalize import norm_key, row_hash

NEW = "NEW"
CORRECTION = "CORRECTION"
EXACT_DUPLICATE = "EXACT_DUPLICATE"
DUPLICATE_IN_FILE = "DUPLICATE_IN_FILE"

INSERTABLE = (NEW, CORRECTION)

#: Pool columns that identify a record, in a fixed order.
NOO_IDENTITY_COLUMNS = ("customer_branch_code", "customer_store_code")
SKU_IDENTITY_COLUMNS = ("customer_branch_code", "product_code",
                        "customer_product_code")


def _content_columns(headers, not_owned):
    """Business columns of a pool layout, in header order.

    Drops the timestamp, the volatile enrichment set, and everything
    Streamlit does not own — BD Support's manual flags, live spreadsheet
    formulas, and columns deliberately never populated. None of these reflect
    what the admin submitted, so none may affect duplicate classification: a
    formula recalculating, or a permanently blank column being filled in
    later, must never change a stored hash.
    """
    skip = (set(config.TIMESTAMP_COLUMNS)
            | set(config.VOLATILE_ENRICHMENT_COLUMNS) | set(not_owned))
    return tuple(c for c in headers if c not in skip)


NOO_CONTENT_COLUMNS = _content_columns(config.POOL_NOO_HEADERS,
                                       config.POOL_NOO_NOT_OWNED)
SKU_CONTENT_COLUMNS = _content_columns(config.POOL_SKU_HEADERS,
                                       config.POOL_SKU_NOT_OWNED)


@dataclass(frozen=True)
class Classification:
    row_number: int
    bucket: str
    identity: str
    content: str
    note: str = ""

    @property
    def insertable(self) -> bool:
        return self.bucket in INSERTABLE


def _identity(row, columns) -> str:
    return "|".join(norm_key(row.get(c)) for c in columns)


def _content(row, columns) -> str:
    return row_hash([row.get(c) for c in columns])


def noo_identity(row) -> str:
    return _identity(row, NOO_IDENTITY_COLUMNS)


def noo_content(row) -> str:
    return _content(row, NOO_CONTENT_COLUMNS)


def sku_identity(row) -> str:
    return _identity(row, SKU_IDENTITY_COLUMNS)


def sku_content(row) -> str:
    return _content(row, SKU_CONTENT_COLUMNS)


def classify(pool_rows, row_numbers, *, existing_identities, existing_contents,
             identity_fn, content_fn):
    """Bucket every enriched row of an upload.

    ``pool_rows`` are enriched records keyed by pool column — not raw input.
    """
    results = []
    seen_in_file: set = set()

    for row, number in zip(pool_rows, row_numbers):
        identity = identity_fn(row)
        content = content_fn(row)

        if identity in seen_in_file:
            bucket, note = DUPLICATE_IN_FILE, (
                "Baris ini muncul lebih dari sekali di dalam file yang sama.")
        elif content in existing_contents:
            bucket, note = EXACT_DUPLICATE, (
                "Data yang persis sama sudah pernah diupload sebelumnya.")
        elif identity in existing_identities:
            bucket, note = CORRECTION, (
                "Data serupa sudah ada, tetapi isinya berbeda — "
                "diperlakukan sebagai koreksi dan tetap diinput sebagai baris baru.")
        else:
            bucket, note = NEW, ""

        seen_in_file.add(identity)
        results.append(Classification(number, bucket, identity, content, note))

    return results


def summarise(classifications, error_rows=0) -> dict:
    counts = Counter(c.bucket for c in classifications)
    return {
        "total": len(classifications) + error_rows,
        "new": counts.get(NEW, 0),
        "correction": counts.get(CORRECTION, 0),
        "exact_duplicate": counts.get(EXACT_DUPLICATE, 0),
        "duplicate_in_file": counts.get(DUPLICATE_IN_FILE, 0),
        "error": error_rows,
        "insertable": sum(counts.get(b, 0) for b in INSERTABLE),
    }


def verdict(summary) -> tuple[str, str]:
    """Decide what the app should offer for a whole file.

    Duplicates never block the eligible rows: they are skipped, reported, and the
    remaining NEW/CORRECTION rows proceed once the user confirms.
    """
    if summary["total"] == 0:
        return "reject", "File tidak berisi data yang bisa diproses."

    skipped = summary["exact_duplicate"] + summary["duplicate_in_file"]
    if summary["insertable"] == 0:
        if skipped:
            return "reject", (
                f"Semua {skipped} baris sudah pernah diupload sebelumnya. "
                "Tidak ada baris baru untuk diproses.")
        return "reject", "Tidak ada baris yang memenuhi syarat untuk diupload."

    if skipped:
        return "confirm", (
            f"{summary['insertable']} baris akan diupload. "
            f"{skipped} baris sudah pernah diupload sebelumnya dan tidak akan "
            "dimasukkan kembali.")
    return "confirm", f"{summary['insertable']} baris siap diupload."
