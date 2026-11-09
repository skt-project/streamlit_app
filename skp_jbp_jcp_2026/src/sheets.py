"""Google Sheets integration: source store list (sheet "Template") and the
upload log (sheet "SKP_UPLOAD_LOG").

The pure functions (parse / search / detect / plan_log_upsert) hold all the
business rules and are unit-tested. The two log backends (Google Sheets and a
local CSV for mock mode) share the same upsert plan so they behave the same.
"""
from __future__ import annotations

import csv
import logging
import os
import threading
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Protocol

import pandas as pd

from src.validation import BRANDS, PROGRAMS, QUARTER_SOURCE_COLUMNS, QUARTERS, clean_cell

log = logging.getLogger("skp.sheets")

COL_PROGRAM = "Jenis Program"
COL_BRAND = "Brand"
COL_CODE = "Kode Toko/Group"
COL_NAME = "Nama Toko/Group"
COL_REGION = "Region"

SOURCE_HEADERS: list[str] = [COL_PROGRAM, COL_BRAND, COL_CODE, COL_NAME, COL_REGION, *QUARTER_SOURCE_COLUMNS.values()]

LOG_HEADERS: list[str] = [
    "Timestamp",
    "PIC Submit",
    COL_CODE,
    COL_NAME,
    COL_REGION,
    COL_BRAND,
    COL_PROGRAM,
    "Kuartal",
    "Nomor SKP",
    "Nama File",
    "GCS Path",
    "Status",
    "Uploader",
]
LOG_KEY: tuple[str, ...] = (COL_CODE, COL_BRAND, COL_PROGRAM, "Kuartal")

STATUS_UPLOADED = "Uploaded"
STATUS_REPLACED = "Replaced"


class SourceFormatError(ValueError):
    """The source sheet does not have the expected header."""


class LogFormatError(ValueError):
    """The log sheet header was changed by hand and is missing columns."""


# --------------------------------------------------------------------------
# Source sheet: parsing, store index, search, auto-detection
# --------------------------------------------------------------------------

def _header_index(row: list[str], wanted: Iterable[str]) -> dict[str, int] | None:
    cleaned = {clean_cell(c).upper(): i for i, c in enumerate(row) if clean_cell(c)}
    if not all(w.upper() in cleaned for w in (COL_CODE, COL_NAME)):
        return None
    return {w: cleaned[w.upper()] for w in wanted if w.upper() in cleaned}


def parse_source_values(values: list[list[str]]) -> pd.DataFrame:
    """Turn raw sheet values into a clean DataFrame with SOURCE_HEADERS columns.

    - Header row is located by name (first row containing the code + name headers),
      so extra columns or re-ordered columns are tolerated.
    - Cells are cleaned (NBSP, whitespace, '#N/A' -> '').
    - Rows without a store code are dropped (e.g. stray formula rows).
    - Exact duplicate rows are dropped.
    - Region spelling is unified case-insensitively to its most common variant
      ('EAST JAVA' -> 'East Java') so one region = one GCS folder.
    """
    header_row = None
    index: dict[str, int] = {}
    for i, row in enumerate(values[:20]):
        found = _header_index(row, SOURCE_HEADERS)
        if found:
            header_row, index = i, found
            break
    if header_row is None:
        raise SourceFormatError(f"Header '{COL_CODE}' dan '{COL_NAME}' tidak ditemukan di sheet sumber.")
    missing = [h for h in SOURCE_HEADERS if h not in index]
    if missing:
        raise SourceFormatError("Kolom berikut tidak ditemukan di sheet sumber: " + ", ".join(missing))

    records = []
    for row in values[header_row + 1:]:
        rec = {h: clean_cell(row[i]) if i < len(row) else "" for h, i in index.items()}
        if not rec[COL_CODE]:
            continue
        rec[COL_BRAND] = rec[COL_BRAND].upper()
        rec[COL_PROGRAM] = rec[COL_PROGRAM].upper()
        records.append(rec)

    df = pd.DataFrame(records, columns=SOURCE_HEADERS).fillna("").astype(str)
    if df.empty:
        return df
    df = df.drop_duplicates().reset_index(drop=True)

    counts = Counter(r for r in df[COL_REGION] if r)
    canonical: dict[str, str] = {}
    for region, _ in counts.most_common():
        canonical.setdefault(region.upper(), region)
    df[COL_REGION] = df[COL_REGION].map(lambda r: canonical.get(r.upper(), r) if r else "")
    return df


@dataclass(frozen=True)
class Store:
    code: str
    name: str
    region: str
    aliases: tuple[str, ...] = ()

    @property
    def label(self) -> str:
        region = self.region or "Region belum diisi"
        return f"{self.code} — {self.name} · {region}"


def build_store_index(df: pd.DataFrame) -> pd.DataFrame:
    """One row per store code with a canonical name (most frequent, then first seen)."""
    columns = ["code", "name", "region", "aliases", "order"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    rows = []
    for order, (code, grp) in enumerate(df.groupby(COL_CODE, sort=False)):
        names = [n for n in grp[COL_NAME] if n]
        counts = Counter(names)
        canonical = max(counts, key=lambda n: (counts[n], -names.index(n))) if names else code
        aliases = tuple(sorted({n for n in names if n != canonical}))
        region = next((r for r in grp[COL_REGION] if r), "")
        rows.append({"code": code, "name": canonical, "region": region, "aliases": aliases, "order": order})
    return pd.DataFrame(rows, columns=columns)


def store_from_index(index: pd.DataFrame, code: str) -> Store | None:
    hit = index[index["code"].str.upper() == clean_cell(code).upper()]
    if hit.empty:
        return None
    row = hit.iloc[0]
    return Store(code=row["code"], name=row["name"], region=row["region"], aliases=tuple(row["aliases"]))


def _compact(text: str) -> str:
    return "".join(ch for ch in clean_cell(text).upper() if ch.isalnum())


def search_stores_by_code(index: pd.DataFrame, query: str, limit: int = 50) -> list[Store]:
    """Case/space/punctuation-insensitive match on code: exact, then prefix, then contains."""
    q = _compact(query)
    if not q or index.empty:
        return []
    ranked = []
    for row in index.itertuples(index=False):
        code = _compact(row.code)
        if q == code:
            rank = 0
        elif code.startswith(q):
            rank = 1
        elif q in code:
            rank = 2
        else:
            continue
        ranked.append((rank, row.order, Store(row.code, row.name, row.region, tuple(row.aliases))))
    ranked.sort(key=lambda t: (t[0], t[1]))
    return [s for _, _, s in ranked[:limit]]


def search_stores_by_name(index: pd.DataFrame, query: str, limit: int = 50) -> list[Store]:
    """Case-insensitive match on canonical name and name variants (aliases).

    Every code is returned once; a name that exists under two codes gives two
    results so the user can pick the right one.
    """
    q = clean_cell(query).upper()
    if not q or index.empty:
        return []
    ranked = []
    for row in index.itertuples(index=False):
        names = [row.name, *row.aliases]
        best = None
        for n in names:
            u = n.upper()
            if u == q:
                best = 0 if best is None else min(best, 0)
            elif u.startswith(q):
                best = 1 if best is None else min(best, 1)
            elif q in u:
                best = 2 if best is None else min(best, 2)
        if best is not None:
            ranked.append((best, row.order, Store(row.code, row.name, row.region, tuple(row.aliases))))
    ranked.sort(key=lambda t: (t[0], t[1]))
    return [s for _, _, s in ranked[:limit]]


@dataclass
class Detection:
    """What the source sheet says about one store."""

    brands: set[str] = field(default_factory=set)
    programs: set[tuple[str, str]] = field(default_factory=set)  # (brand, program)
    skp: dict[tuple[str, str, str], str] = field(default_factory=dict)  # (brand, program, quarter) -> nomor
    conflicts: list[str] = field(default_factory=list)

    def skp_number(self, brand: str, program: str, quarter: str) -> str:
        return self.skp.get((brand, program, quarter), "")

    def quarters_for(self, brand: str, program: str) -> list[str]:
        return [q for q in QUARTERS if (brand, program, q) in self.skp]


def detect_programs(df: pd.DataFrame, code: str) -> Detection:
    """Auto-detection: brand from 'Brand', program from 'Jenis Program', quarter
    when 'SKP Qx 2026' has a number. Unknown brands/programs are ignored."""
    det = Detection()
    if df.empty:
        return det
    rows = df[df[COL_CODE].str.upper() == clean_cell(code).upper()]
    for _, row in rows.iterrows():
        brand, program = row[COL_BRAND], row[COL_PROGRAM]
        if brand not in BRANDS:
            continue
        det.brands.add(brand)
        if program not in PROGRAMS:
            continue
        det.programs.add((brand, program))
        for quarter, column in QUARTER_SOURCE_COLUMNS.items():
            number = row[column]
            if not number:
                continue
            key = (brand, program, quarter)
            if key in det.skp and det.skp[key] != number:
                det.conflicts.append(
                    f"{brand} {program} {quarter}: nomor SKP berbeda di spreadsheet "
                    f"({det.skp[key]} vs {number}); yang dipakai {det.skp[key]}."
                )
                continue
            det.skp[key] = number
    return det


def expected_documents(df: pd.DataFrame) -> pd.DataFrame:
    """Every (store, brand, program, quarter) that has an SKP number in the source."""
    columns = [COL_CODE, COL_NAME, COL_REGION, COL_BRAND, COL_PROGRAM, "Kuartal", "Nomor SKP"]
    if df.empty:
        return pd.DataFrame(columns=columns)
    index = build_store_index(df).set_index("code")
    seen = set()
    rows = []
    for _, row in df.iterrows():
        if row[COL_BRAND] not in BRANDS or row[COL_PROGRAM] not in PROGRAMS:
            continue
        for quarter, column in QUARTER_SOURCE_COLUMNS.items():
            if not row[column]:
                continue
            key = (row[COL_CODE].upper(), row[COL_BRAND], row[COL_PROGRAM], quarter)
            if key in seen:
                continue
            seen.add(key)
            store = index.loc[row[COL_CODE]]
            rows.append([row[COL_CODE], store["name"], store["region"], row[COL_BRAND], row[COL_PROGRAM], quarter, row[column]])
    return pd.DataFrame(rows, columns=columns)


# --------------------------------------------------------------------------
# Upload log: pure upsert planning
# --------------------------------------------------------------------------

def log_key(record: dict) -> tuple[str, ...]:
    return tuple(clean_cell(record.get(k, "")).upper() for k in LOG_KEY)


@dataclass
class UpsertPlan:
    header: list[str]
    write_header: bool
    updates: list[tuple[int, list[str]]]  # (1-based sheet row, full row values)
    appends: list[list[str]]
    previous: dict[tuple[str, ...], dict[str, str]]  # key -> record before this upsert
    paths_in_use: set[str]  # every GCS Path referenced after this upsert


def plan_log_upsert(existing: list[list[str]], records: list[dict]) -> UpsertPlan:
    """Plan an update-or-insert keyed on (Kode Toko/Group, Brand, Jenis Program, Kuartal).

    - A key that already exists is updated in place (first matching row), never duplicated.
    - 'Status' becomes 'Uploaded' or 'Replaced'; an optional record['_note'] is appended.
    - Columns added by hand beyond LOG_HEADERS keep their existing values on update.
    """
    if existing and any(clean_cell(c) for c in existing[0]):
        header = [str(c).strip() for c in existing[0]]
        missing = [h for h in LOG_HEADERS if h not in header]
        if missing:
            raise LogFormatError("Header sheet log berubah, kolom hilang: " + ", ".join(missing))
        write_header = False
        body = existing[1:]
    else:
        header, write_header, body = list(LOG_HEADERS), True, []

    width = len(header)
    first_row_for_key: dict[tuple[str, ...], int] = {}
    current: dict[int, list[str]] = {}
    for offset, raw in enumerate(body):
        row = (list(raw) + [""] * width)[:width]
        if not any(clean_cell(c) for c in row):
            continue
        sheet_row = offset + 2
        current[sheet_row] = row
        first_row_for_key.setdefault(log_key(dict(zip(header, row))), sheet_row)

    latest: dict[tuple[str, ...], dict] = {}
    for rec in records:
        latest[log_key(rec)] = rec  # last one wins inside one batch

    updates: list[tuple[int, list[str]]] = []
    appends: list[list[str]] = []
    previous: dict[tuple[str, ...], dict[str, str]] = {}
    for key, rec in latest.items():
        sheet_row = first_row_for_key.get(key)
        prev = dict(zip(header, current[sheet_row])) if sheet_row else None
        status = STATUS_REPLACED if prev else STATUS_UPLOADED
        if rec.get("_note"):
            status = f"{status} ({rec['_note']})"
        values = {**rec, "Status": status}
        if prev is not None:
            previous[key] = prev
            row = [str(values[h]) if h in values and h in LOG_HEADERS else prev.get(h, "") for h in header]
            updates.append((sheet_row, row))
            current[sheet_row] = row
        else:
            row = [str(values.get(h, "")) if h in LOG_HEADERS else "" for h in header]
            appends.append(row)

    path_col = header.index("GCS Path")
    paths = {r[path_col] for r in current.values() if r[path_col]}
    paths |= {r[path_col] for r in appends if r[path_col]}
    return UpsertPlan(header, write_header, updates, appends, previous, paths)


def logs_to_frame(values: list[list[str]]) -> pd.DataFrame:
    """Log values -> DataFrame, one row per key (latest Timestamp wins on accidental duplicates)."""
    if not values or not any(clean_cell(c) for c in values[0]):
        return pd.DataFrame(columns=LOG_HEADERS)
    header = [str(c).strip() for c in values[0]]
    width = len(header)
    rows = [(list(r) + [""] * width)[:width] for r in values[1:] if any(clean_cell(c) for c in r)]
    df = pd.DataFrame(rows, columns=header).astype(str)
    for h in LOG_HEADERS:
        if h not in df.columns:
            df[h] = ""
    if df.empty:
        return df[LOG_HEADERS]
    df["_order"] = range(len(df))
    df["_key"] = df.apply(lambda r: log_key(r.to_dict()), axis=1)
    df = df.sort_values(["Timestamp", "_order"]).drop_duplicates("_key", keep="last")
    return df.sort_values("_order").drop(columns=["_order", "_key"]).reset_index(drop=True)


# --------------------------------------------------------------------------
# Backends
# --------------------------------------------------------------------------

class LogStore(Protocol):
    label: str

    def read_values(self) -> list[list[str]]: ...

    def upsert(self, records: list[dict]) -> UpsertPlan: ...


class SourceReader(Protocol):
    label: str

    def read_values(self) -> list[list[str]]: ...


class SheetsSource:
    """Read-only access to the source sheet."""

    def __init__(self, spreadsheet, sheet_name: str):
        self._spreadsheet = spreadsheet
        self._sheet_name = sheet_name
        self.label = f"Google Sheets · {sheet_name}"

    def read_values(self) -> list[list[str]]:
        return self._spreadsheet.worksheet(self._sheet_name).get_all_values()


class StaticSource:
    """Source rows held in memory (mock mode sample data)."""

    def __init__(self, values: list[list[str]], label: str = "Data contoh (mock)"):
        self._values = values
        self.label = label

    def read_values(self) -> list[list[str]]:
        return [list(r) for r in self._values]


class SheetsLogStore:
    """SKP_UPLOAD_LOG in Google Sheets. The tab is created on the first write only."""

    def __init__(self, spreadsheet, sheet_name: str):
        self._spreadsheet = spreadsheet
        self._sheet_name = sheet_name
        self.label = f"Google Sheets · {sheet_name}"

    def _worksheet(self, create: bool):
        import gspread

        try:
            return self._spreadsheet.worksheet(self._sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            if not create:
                return None
        try:
            ws = self._spreadsheet.add_worksheet(title=self._sheet_name, rows=1000, cols=len(LOG_HEADERS))
            log.info("Created log sheet %s", self._sheet_name)
            return ws
        except gspread.exceptions.APIError:
            # Another session may have created it a moment ago.
            return self._spreadsheet.worksheet(self._sheet_name)

    def read_values(self) -> list[list[str]]:
        ws = self._worksheet(create=False)
        return ws.get_all_values() if ws is not None else []

    def upsert(self, records: list[dict]) -> UpsertPlan:
        from gspread.utils import rowcol_to_a1

        ws = self._worksheet(create=True)
        plan = plan_log_upsert(ws.get_all_values(), records)
        last_col = len(plan.header)
        batch = []
        if plan.write_header:
            batch.append({"range": f"A1:{rowcol_to_a1(1, last_col)}", "values": [plan.header]})
        for sheet_row, row in plan.updates:
            batch.append({"range": f"{rowcol_to_a1(sheet_row, 1)}:{rowcol_to_a1(sheet_row, last_col)}", "values": [row]})
        if batch:
            ws.batch_update(batch, value_input_option="RAW")
        if plan.write_header:
            try:
                ws.freeze(rows=1)
            except Exception:  # cosmetic only
                pass
        if plan.appends:
            ws.append_rows(plan.appends, value_input_option="RAW", insert_data_option="INSERT_ROWS", table_range="A1")
        return plan


class LocalLogStore:
    """CSV copy of the log for mock/local mode (same upsert rules)."""

    _lock = threading.Lock()

    def __init__(self, path: str | os.PathLike):
        self._path = Path(path)
        self.label = f"file lokal …/{self._path.parent.name}/{self._path.name}"

    def read_values(self) -> list[list[str]]:
        if not self._path.exists():
            return []
        with self._path.open(newline="", encoding="utf-8") as fh:
            return [row for row in csv.reader(fh)]

    def upsert(self, records: list[dict]) -> UpsertPlan:
        with self._lock:
            existing = self.read_values()
            plan = plan_log_upsert(existing, records)
            rows = [plan.header] + [
                (list(r) + [""] * len(plan.header))[: len(plan.header)] for r in (existing[1:] if not plan.write_header else [])
            ]
            for sheet_row, row in plan.updates:
                rows[sheet_row - 1] = row
            rows.extend(plan.appends)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            with tmp.open("w", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerows(rows)
            os.replace(tmp, self._path)
            return plan


def now_wib() -> str:
    """Timestamp string in WIB (UTC+7), sortable as text."""
    from datetime import timedelta, timezone

    return datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M:%S")
