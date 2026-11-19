"""Semua waktu yang terlihat user / di sheet / di nama file memakai WIB (UTC+7)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

WIB = timezone(timedelta(hours=7))      # Indonesia tidak memakai DST


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_wib(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(WIB)


def fmt_wib(value: datetime) -> str:
    """Format kolom Timestamp di sheet: '2026-11-19 14:05:33' (WIB)."""
    return to_wib(value).strftime("%Y-%m-%d %H:%M:%S")
