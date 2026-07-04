"""Geospatial helpers — haversine distance only. Never used to block any flow."""
import math


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in metres between two WGS-84 coordinates."""
    R = 6_371_000  # earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def distance_or_none(
    lat1: float | None, lon1: float | None,
    lat2: float | None, lon2: float | None,
) -> float | None:
    """Safe wrapper — returns None if any coordinate is missing."""
    if None in (lat1, lon1, lat2, lon2):
        return None
    return round(haversine_m(lat1, lon1, lat2, lon2), 1)
