"""
Salesman PJP (G2G) — Frekuensi (I) / Hari (J) / Minggu (K) / Ket. Minggu (L).

Pure, Streamlit/BigQuery-free logic so it can be unit tested in isolation
(see tests/test_pjp_hari_minggu.py) and imported unchanged by salesman_pjp.py
(the PJP Template Excel engine + upload validator).

USER FLOW (each column constrains the next):

    I — Frekuensi   F1 | F2 | F4 | F4+          user picks
          |
    J — Hari        SENIN..SABTU, "/"-joined     user picks (count set by I)
          |
    K — Minggu      Minggu Ganjil | Minggu Genap | Minggu Ganjil + Genap
          |                                       user picks (options set by I)
    L — Ket. Minggu the concrete week number(s)   user picks for F1;
          |                                       AUTOMATIC for F2/F4/F4+
          v
    DB: callcycle   the ONLY new database column

RULES (the single source of truth for the whole feature):

    F1  -> exactly 1 day
           Minggu = Ganjil or Genap  (never Ganjil + Genap — F1 is once/month)
           Ganjil -> Ket. Minggu is a dropdown of  1 | 3   (user picks ONE)
           Genap  -> Ket. Minggu is a dropdown of  2 | 4   (user picks ONE)
           callcycle = that single chosen week

    F2  -> 1 or 2 unique days
           Minggu = Ganjil or Genap  (never Ganjil + Genap)
           Ganjil -> callcycle = "1,3"   (automatic)
           Genap  -> callcycle = "2,4"   (automatic)

    F4  -> 1-4 unique days
           Minggu = Ganjil + Genap (the only option)
           callcycle = "1,2,3,4"          (automatic) — never week 5

    F4+ -> 1-5 unique days
           Minggu = Ganjil + Genap (the only option)
           callcycle = "1,2,3,4,5"         (automatic) — week 5 is emitted
           only when the calendar month actually has one (that decision is
           made downstream against real dates, not here)

F4/F4+ describe the WEEK pattern, NOT a day count — the number of visit
days is controlled independently by Column J.

Hari is SENIN..SABTU only; "MINGGU" (Sunday) is never a valid visit day
for this template (note the deliberate name collision with Column K's
"Minggu" = week-parity, which is a different concept entirely).

Only `callcycle` is persisted. Column K is a template/UI input used to
derive L; it is never written to the database.
"""
import itertools
import re

import pandas as pd

# ─── Column I — Frekuensi ──────────────────────────────────────────────────

FREKUENSI_OPTIONS = ["F1", "F2", "F4", "F4+"]

# (min_days, max_days) allowed in Column J for each Frekuensi.
#
# F2 accepts ONE or TWO days. The week pattern is what makes F2 "twice a
# month" (callcycle 1,3 or 2,4 — unchanged), so a single day is a complete,
# valid assignment on its own: F2 + SENIN + Minggu Ganjil visits Senin in
# week 1 and again in week 3. Two days remain supported exactly as before.
FREKUENSI_HARI_DAY_COUNT = {
    "F1": (1, 1),
    "F2": (1, 2),
    "F4": (1, 4),
    "F4+": (1, 5),
}

# Excel named ranges cannot contain "+", so F4+ uses the "F4PLUS" suffix.
FREKUENSI_RANGE_SUFFIX = {"F1": "F1", "F2": "F2", "F4": "F4", "F4+": "F4PLUS"}

# ─── Column J — Hari (SENIN..SABTU only, never MINGGU/Sunday) ─────────────

HARI_CANONICAL_ORDER = ["SENIN", "SELASA", "RABU", "KAMIS", "JUMAT", "SABTU"]
HARI_SEPARATOR = "/"


def _hari_combos(min_days: int, max_days: int) -> list[str]:
    return [
        HARI_SEPARATOR.join(c)
        for r in range(min_days, max_days + 1)
        for c in itertools.combinations(HARI_CANONICAL_ORDER, r)
    ]


# Every valid day-combination per Frekuensi, pre-rendered canonically —
# this is exactly what Column J's dependent dropdown offers.
#   F1 ->  6   F2 -> 21   F4 -> 56   F4+ -> 62
HARI_COMBOS_BY_FREKUENSI = {
    f: _hari_combos(*FREKUENSI_HARI_DAY_COUNT[f]) for f in FREKUENSI_OPTIONS
}


def hari_options_for_frekuensi(frekuensi: str) -> list[str]:
    """Valid Column J dropdown options for a given Column I value."""
    return HARI_COMBOS_BY_FREKUENSI.get(str(frekuensi).strip().upper(), [])


# ─── Column K — Minggu (week parity) ───────────────────────────────────────

MINGGU_GANJIL = "Minggu Ganjil"
MINGGU_GENAP = "Minggu Genap"
MINGGU_GANJIL_GENAP = "Minggu Ganjil + Genap"

# Which Column K options each Frekuensi allows. F1/F2 are once/twice a
# month so "Ganjil + Genap" is meaningless for them; F4/F4+ cover the whole
# cycle so "Ganjil + Genap" is their ONLY option (a one-entry dropdown is
# how "automatic + locked" is expressed in native Excel — see the module
# note in salesman_pjp.py's _attach_pjp_dvs()).
MINGGU_OPTIONS_BY_FREKUENSI = {
    "F1": [MINGGU_GANJIL, MINGGU_GENAP],
    "F2": [MINGGU_GANJIL, MINGGU_GENAP],
    "F4": [MINGGU_GANJIL_GENAP],
    "F4+": [MINGGU_GANJIL_GENAP],
}

# Suffix used to build the NR_KET_<FREQ>_<MINGGU> named-range names. Must
# match the Excel formula in _attach_pjp_dvs(), which derives the same
# token from the cell text via nested SUBSTITUTE()s.
MINGGU_RANGE_SUFFIX = {
    MINGGU_GANJIL: "GANJIL",
    MINGGU_GENAP: "GENAP",
    MINGGU_GANJIL_GENAP: "GANJILGENAP",
}


def minggu_options_for_frekuensi(frekuensi: str) -> list[str]:
    """Valid Column K dropdown options for a given Column I value."""
    return MINGGU_OPTIONS_BY_FREKUENSI.get(str(frekuensi).strip().upper(), [])


# ─── Column L — Ket. Minggu  ->  DB `callcycle` ────────────────────────────

# The allowed Ket. Minggu values for each (Frekuensi, Minggu) pair. For
# F2/F4/F4+ there is exactly ONE allowed value — that is the "automatic"
# behaviour. For F1 the user genuinely chooses one of two weeks.
KET_OPTIONS_BY_FREKUENSI_MINGGU = {
    ("F1", MINGGU_GANJIL):        ["1", "3"],
    ("F1", MINGGU_GENAP):         ["2", "4"],
    ("F2", MINGGU_GANJIL):        ["1,3"],
    ("F2", MINGGU_GENAP):         ["2,4"],
    ("F4", MINGGU_GANJIL_GENAP):  ["1,2,3,4"],
    ("F4+", MINGGU_GANJIL_GENAP): ["1,2,3,4,5"],
}

MINGGU_COL = "Minggu"          # Column K — UI input only, never persisted
KET_MINGGU_COL = "Ket. Minggu"  # Column L — maps 1:1 to DB `callcycle`

# Header spellings used by earlier iterations of this template. Accepted on
# import so already-downloaded workbooks keep working; always renamed to
# the current headers on the way out.
KET_MINGGU_COL_ALIASES = [
    KET_MINGGU_COL,
    "Nomor Minggu",                                        # previous revision
    "Minggu (1-4)",                                         # revision before that
    "Minggu Ganjil/Minggu Genap/Minggu Ganjil + Genap",      # original static dropdown
]
MINGGU_COL_ALIASES = [MINGGU_COL, "Hari Minggu"]  # "Hari Minggu" = previous revision

# Legacy stored `minggu` phrase -> callcycle, for the historical-data
# migration ONLY. Note this is NOT valid for F1 rows: F1 needs one specific
# week (1 or 3 / 2 or 4), which the legacy phrase does not record — those
# rows must be reviewed by a human, never guessed. See the migration script.
LEGACY_MINGGU_MAP = {
    "MINGGU GANJIL": "1,3",
    "MINGGU GENAP": "2,4",
    "MINGGU GANJIL + GENAP": "1,2,3,4",
    "MINGGU GANJIL+GENAP": "1,2,3,4",
}


def _is_blank(val) -> bool:
    return val is None or (isinstance(val, float) and pd.isna(val))


def _clean(raw) -> str:
    """
    Trims to a plain string, coercing Excel's numeric cells back to their
    integer text form.

    Necessary because a single-value Ket. Minggu ("3") is stored by Excel
    as a NUMBER, so pandas reads it back as the float 3.0 — a naive
    str() would give "3.0" and fail every membership check. Multi-value
    cells ("1,3") contain a comma and are always read as text, so only the
    single-value F1 case is affected. Guards bool first, since bool is a
    subclass of int in Python.
    """
    if _is_blank(raw):
        return ""
    if isinstance(raw, bool):
        return str(raw).strip()
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, float):
        return str(int(raw)) if float(raw).is_integer() else str(raw).strip()
    return str(raw).strip()


# ─── Validators / normalizers ──────────────────────────────────────────────


def normalize_hari(raw, frekuensi: str | None = None) -> tuple[str | None, str | None]:
    """
    Parses/validates a Column J (Hari) cell.

    Accepts one or more of SENIN..SABTU separated by "/" and/or ",",
    case-insensitive, spaces around separators ignored. Returns
    (normalized, error) with exactly one not-None. On success the value is
    "/"-joined, de-duplicated and ordered Monday->Saturday.

    If `frekuensi` is given, the day COUNT is also checked against
    FREKUENSI_HARI_DAY_COUNT (F1=1, F2=1-2, F4=1-4, F4+=1-5).
    """
    text = _clean(raw)
    if not text:
        return None, "kosong"

    tokens = [t.strip().upper() for t in re.split(r"[,/]", text) if t.strip()]
    if not tokens:
        return None, "kosong"

    invalid = [t for t in tokens if t not in HARI_CANONICAL_ORDER]
    if invalid:
        if any(t in ("MINGGU", "MINGU") for t in invalid):
            return None, "MINGGU (hari Minggu/Sunday) tidak diperbolehkan — hanya SENIN-SABTU"
        return None, f"nilai tidak dikenal: {', '.join(invalid)}"

    seen, dupes = set(), []
    for t in tokens:
        if t in seen:
            dupes.append(t)
        seen.add(t)
    if dupes:
        return None, f"hari duplikat: {', '.join(sorted(set(dupes)))}"

    ordered = [d for d in HARI_CANONICAL_ORDER if d in seen]
    normalized = HARI_SEPARATOR.join(ordered)

    if frekuensi is not None:
        fk = _clean(frekuensi).upper()
        bounds = FREKUENSI_HARI_DAY_COUNT.get(fk)
        if bounds is None:
            return None, f"Frekuensi tidak dikenal: {frekuensi}"
        lo, hi = bounds
        n = len(ordered)
        if not (lo <= n <= hi):
            need = f"tepat {lo}" if lo == hi else f"{lo}-{hi}"
            return None, f"Frekuensi {fk} membutuhkan {need} hari, ditemukan {n}"

    return normalized, None


def normalize_minggu(raw, frekuensi) -> tuple[str | None, str | None]:
    """
    Parses/validates a Column K (Minggu) cell against its row's Frekuensi.

    Accepts the three canonical labels case-insensitively and whitespace-
    tolerantly ("minggu  ganjil+genap" -> "Minggu Ganjil + Genap").
    Returns (canonical_label, error) with exactly one not-None.

    F1/F2 reject "Minggu Ganjil + Genap"; F4/F4+ accept ONLY it.
    """
    fk = _clean(frekuensi).upper()
    if fk not in FREKUENSI_OPTIONS:
        return None, f"Frekuensi tidak dikenal: {frekuensi}"

    text = _clean(raw)
    if not text:
        return None, "kosong"

    # Canonicalise: collapse whitespace, drop spaces around "+", title-case.
    key = re.sub(r"\s+", " ", text).upper().replace(" + ", "+").replace("+ ", "+").replace(" +", "+")
    canon = {
        "MINGGU GANJIL": MINGGU_GANJIL,
        "MINGGU GENAP": MINGGU_GENAP,
        "MINGGU GANJIL+GENAP": MINGGU_GANJIL_GENAP,
        "GANJIL": MINGGU_GANJIL,
        "GENAP": MINGGU_GENAP,
        "GANJIL+GENAP": MINGGU_GANJIL_GENAP,
    }.get(key)
    if canon is None:
        return None, f"nilai Minggu tidak dikenal: '{text}'"

    allowed = MINGGU_OPTIONS_BY_FREKUENSI[fk]
    if canon not in allowed:
        return None, (
            f"Frekuensi {fk} tidak boleh Minggu '{canon}' — "
            f"pilihan yang valid: {', '.join(allowed)}"
        )
    return canon, None


def ket_minggu_options(frekuensi, minggu) -> list[str]:
    """Valid Column L dropdown options for a (Frekuensi, Minggu) pair."""
    fk = _clean(frekuensi).upper()
    mg, _err = normalize_minggu(minggu, fk) if fk in FREKUENSI_OPTIONS else (None, None)
    if mg is None:
        return []
    return list(KET_OPTIONS_BY_FREKUENSI_MINGGU.get((fk, mg), []))


def auto_callcycle(frekuensi, minggu) -> str | None:
    """
    The automatically-determined callcycle for a (Frekuensi, Minggu) pair,
    or None when the user must choose (F1, which has two candidate weeks).
    Used to auto-fill Column L for F2/F4/F4+ on import.
    """
    opts = ket_minggu_options(frekuensi, minggu)
    return opts[0] if len(opts) == 1 else None


def normalize_callcycle(raw, frekuensi, minggu) -> tuple[str | None, str | None]:
    """
    Parses/validates a Column L (Ket. Minggu) cell — the DB `callcycle`.

    Validity depends on BOTH the row's Frekuensi and its Minggu, so both
    are required. Tolerates spacing/ordering ("3, 1" -> "1,3") before
    checking membership in the allowed set for that pair.

    F1 accepts exactly one of its two candidate weeks (Ganjil -> 1|3,
    Genap -> 2|4). F2/F4/F4+ accept only their single automatic value.
    """
    fk = _clean(frekuensi).upper()
    if fk not in FREKUENSI_OPTIONS:
        return None, f"Frekuensi tidak dikenal: {frekuensi}"

    mg, mg_err = normalize_minggu(minggu, fk)
    if mg_err:
        return None, f"Minggu tidak valid ({mg_err})"

    text = _clean(raw)
    if not text:
        return None, "kosong"

    tokens = [t.strip() for t in text.split(",") if t.strip()]
    if not tokens:
        return None, "kosong"

    valid_weeks = {"1", "2", "3", "4", "5"}
    unknown = [t for t in tokens if t not in valid_weeks]
    if unknown:
        return None, f"nilai tidak dikenal: {', '.join(unknown)}"

    seen, dupes = set(), []
    for t in tokens:
        if t in seen:
            dupes.append(t)
        seen.add(t)
    if dupes:
        return None, f"minggu duplikat: {', '.join(sorted(set(dupes)))}"

    normalized = ",".join(sorted(seen, key=int))
    allowed = KET_OPTIONS_BY_FREKUENSI_MINGGU.get((fk, mg), [])
    if normalized not in allowed:
        return None, (
            f"Ket. Minggu '{normalized}' tidak valid untuk {fk} + {mg} — "
            f"pilihan yang valid: {', '.join(allowed)}"
        )
    return normalized, None


def derive_minggu_from_callcycle(callcycle) -> str | None:
    """
    Reverse mapping: infer Column K from a stored callcycle. Used to
    display legacy/stored rows and to re-populate Column K on import when
    a workbook supplies Ket. Minggu but not Minggu. Week 5 is ignored for
    parity purposes, so "1,2,3,4,5" -> "Minggu Ganjil + Genap" (never
    "Ganjil" just because 5 is odd).
    """
    text = _clean(callcycle)
    if not text:
        return None
    weeks = {t.strip() for t in text.split(",") if t.strip()}
    has_odd = bool(weeks & {"1", "3"})
    has_even = bool(weeks & {"2", "4"})
    if has_odd and has_even:
        return MINGGU_GANJIL_GENAP
    if has_odd:
        return MINGGU_GANJIL
    if has_even:
        return MINGGU_GENAP
    return None


def migrate_legacy_minggu(raw) -> str | None:
    """
    Maps a legacy stored `minggu` phrase to its callcycle equivalent.
    Returns None if unrecognised (including values already in the new
    format). NOT valid for F1 rows — see LEGACY_MINGGU_MAP's note.
    """
    text = _clean(raw)
    if not text:
        return None
    return LEGACY_MINGGU_MAP.get(re.sub(r"\s+", " ", text).upper())


def expand_hari_callcycle(hari_normalized: str, callcycle: str | None) -> list[tuple[str, str | None]]:
    """
    Reference implementation of the fan-out the BigQuery sync performs:
    one PJP row becomes one (day, week) pair per combination.
    "SENIN/SELASA" x "1,3" -> 4 pairs.

    A blank callcycle yields one pair per day with week None (matching the
    SQL's LEFT JOIN UNNEST, which keeps the row rather than dropping it).
    Whether a "5" pair actually produces a visit in a given month is a
    downstream, date-driven decision — not made here.
    """
    days = [d for d in (hari_normalized or "").split(HARI_SEPARATOR) if d]
    weeks = [w for w in (callcycle or "").split(",") if w] or [None]
    return [(d, w) for d in days for w in weeks]


# Backwards-compatible alias — earlier revision's name for the fan-out.
expand_hari_minggu = expand_hari_callcycle
