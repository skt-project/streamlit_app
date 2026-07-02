"""
validation_utils.py — Column auto-detection and row-level GPS validation.

detect_column_mapping() uses keyword matching to auto-detect which DataFrame
columns hold salesman and store GPS coordinates, handling the naming variations
found across different source systems (DMS exports, Repsly, manual sheets).

validate_and_calculate() applies the validation logic to every row and appends
Distance_KM, Visit_Status, and Validation_Remark columns.
"""

from typing import Dict, List, Optional, Tuple

import pandas as pd

from utils.geo_utils import (
    classify_visit,
    haversine,
    is_valid_latitude,
    is_valid_longitude,
    is_zero_coordinate,
)

# ── Keyword rules for auto-detection ──────────────────────────────────────────
# Each field has "include" keywords (column must contain at least one) and
# "exclude" keywords (column must NOT contain any) — both case-insensitive.
_KEYWORD_RULES: Dict[str, Dict[str, List[str]]] = {
    "salesman_lat": {
        "include": [
            "gps lat", "gps_lat", "gps latitude", "gps_latitude",
            "salesman lat", "salesman_lat", "rep lat", "rep_lat",
        ],
        "exclude": ["store", "toko", "outlet"],
    },
    "salesman_lon": {
        "include": [
            "gps lon", "gps long", "gps_lon", "gps longitude", "gps_longitude",
            "salesman lon", "salesman_lon", "rep lon", "rep_lon",
        ],
        "exclude": ["store", "toko", "outlet"],
    },
    "store_lat": {
        "include": [
            "store lat", "store_lat", "store latitude",
            "toko lat", "outlet lat",
        ],
        "exclude": [],
    },
    "store_lon": {
        "include": [
            "store lon", "store long", "store_lon", "store longitude",
            "toko lon", "outlet lon",
        ],
        "exclude": [],
    },
}


def detect_column_mapping(columns: List[str]) -> Dict[str, Optional[str]]:
    """
    Return a mapping of GPS field names to their detected column names.

    For each field, iterate the DataFrame columns in order and return the
    first column whose lowercased name satisfies the include/exclude rules.
    Returns None for any field that cannot be matched automatically.
    """
    result: Dict[str, Optional[str]] = {key: None for key in _KEYWORD_RULES}

    for field, rules in _KEYWORD_RULES.items():
        for col in columns:
            col_lower = col.lower().strip()
            matched = any(kw in col_lower for kw in rules["include"])
            excluded = any(kw in col_lower for kw in rules["exclude"])
            if matched and not excluded:
                result[field] = col
                break

    return result


# ── Row-level validation ───────────────────────────────────────────────────────

def _safe_float(val) -> Optional[float]:
    """
    Coerce any value to float.
    Returns None for NaN, None, empty strings, or unparseable values.
    Accepts comma-decimal notation (e.g. "106,845" → 106.845).
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(str(val).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None


def _validate_row(
    sal_lat_raw,
    sal_lon_raw,
    store_lat_raw,
    store_lon_raw,
    threshold_km: float = 1.0,
) -> Tuple[Optional[float], str, str]:
    """
    Validate a single row's GPS data and compute visit distance.

    Precedence of error checks:
      1. Store coordinates (no store location → cannot validate at all)
      2. Salesman GPS missing (device returned null / empty)
      3. Salesman GPS is (0, 0) — device failed to acquire a fix
      4. Salesman coordinates out of global range → INVALID COORDINATE
      5. All valid → compute Haversine distance → VALID / INVALID VISIT

    Returns: (distance_km_or_None, Visit_Status, Validation_Remark)
    """
    store_lat = _safe_float(store_lat_raw)
    store_lon = _safe_float(store_lon_raw)
    sal_lat = _safe_float(sal_lat_raw)
    sal_lon = _safe_float(sal_lon_raw)

    # 1. Store location checks
    if store_lat is None or store_lon is None:
        return None, "STORE LOCATION NOT FOUND", "Store GPS coordinates are missing"
    if not is_valid_latitude(store_lat) or not is_valid_longitude(store_lon):
        return None, "STORE LOCATION NOT FOUND", "Store GPS coordinates are out of valid range"

    # 2. Salesman GPS missing
    if sal_lat is None or sal_lon is None:
        return None, "MISSING GPS", "Salesman GPS coordinates are missing"

    # 3. Zero coordinates — GPS not captured by device
    if is_zero_coordinate(sal_lat, sal_lon):
        return None, "MISSING GPS", "Salesman GPS not captured (coordinates are 0, 0)"

    # 4. Salesman coordinate range check
    if not is_valid_latitude(sal_lat) or not is_valid_longitude(sal_lon):
        return None, "INVALID COORDINATE", "Salesman GPS coordinates are out of valid range"

    # 5. Distance calculation
    distance_km = round(haversine(sal_lat, sal_lon, store_lat, store_lon), 4)
    status, remark = classify_visit(distance_km, threshold_km)
    return distance_km, status, remark


# ── Bulk validation ────────────────────────────────────────────────────────────

def validate_and_calculate(
    df: pd.DataFrame,
    mapping: Dict[str, str],
    progress_bar=None,
    threshold_km: float = 1.0,
) -> pd.DataFrame:
    """
    Apply GPS validation to every row of df.

    Appends three columns to a copy of df:
      - Distance_KM        : float (None for error rows)
      - Visit_Status       : VALID VISIT | INVALID VISIT | MISSING GPS |
                             INVALID COORDINATE | STORE LOCATION NOT FOUND
      - Validation_Remark  : human-readable explanation

    progress_bar, if provided, is a Streamlit progress element that will be
    updated approximately 100 times across the full dataset to keep the UI
    responsive without excessive render calls.
    """
    result_df = df.copy()
    total = len(df)
    # Update at most 100 times — avoids excessive Streamlit re-renders
    update_every = max(1, total // 100)

    distances: list = []
    statuses: list = []
    remarks: list = []

    for idx, (_, row) in enumerate(df.iterrows()):
        dist, status, remark = _validate_row(
            row.get(mapping["salesman_lat"]),
            row.get(mapping["salesman_lon"]),
            row.get(mapping["store_lat"]),
            row.get(mapping["store_lon"]),
            threshold_km=threshold_km,
        )
        distances.append(dist)
        statuses.append(status)
        remarks.append(remark)

        if progress_bar is not None and (idx + 1) % update_every == 0:
            progress_bar.progress(
                (idx + 1) / total,
                text=f"Processing {idx + 1:,} / {total:,} records...",
            )

    result_df["Distance_KM"] = distances
    result_df["Visit_Status"] = statuses
    result_df["Validation_Remark"] = remarks

    return result_df
