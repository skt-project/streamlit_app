"""
geo_utils.py — Geospatial calculation helpers.

Uses the Haversine formula to compute great-circle distance between two
GPS coordinates. No external geo libraries required.
"""

import math
from typing import Tuple

# Earth's mean radius in kilometres (WGS-84)
EARTH_RADIUS_KM: float = 6371.0

# Business rule: visits within this radius are classified as valid
VALID_VISIT_THRESHOLD_KM: float = 1.0


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Return the great-circle distance (KM) between two GPS points.

    Haversine formula:
        a = sin²(Δlat/2) + cos(lat1)·cos(lat2)·sin²(Δlon/2)
        d = 2·R·arcsin(√a)

    The intermediate value `a` is clamped to [0, 1] to guard against
    floating-point rounding errors that could make sqrt(a) imaginary.
    """
    lat1_r, lon1_r, lat2_r, lon2_r = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    a = max(0.0, min(1.0, a))  # clamp for floating-point safety
    return 2.0 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def is_valid_latitude(val: float) -> bool:
    """Latitude must be in the range [-90, 90]."""
    return -90.0 <= val <= 90.0


def is_valid_longitude(val: float) -> bool:
    """Longitude must be in the range [-180, 180]."""
    return -180.0 <= val <= 180.0


def is_zero_coordinate(lat: float, lon: float) -> bool:
    """Return True when the device returned (0, 0) — GPS not captured."""
    return lat == 0.0 and lon == 0.0


def classify_visit(
    distance_km: float,
    threshold_km: float = VALID_VISIT_THRESHOLD_KM,
) -> Tuple[str, str]:
    """
    Return (Visit_Status, Validation_Remark) based on distance vs. threshold.
    threshold_km defaults to VALID_VISIT_THRESHOLD_KM (1.0) but can be overridden.
    """
    if distance_km <= threshold_km:
        return "VALID VISIT", f"Salesman is within the {threshold_km} KM store radius"
    return "INVALID VISIT", f"Salesman is outside the {threshold_km} KM store radius"
