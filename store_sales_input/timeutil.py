"""Waktu tampilan: seluruh timestamp yang terlihat user / di Sheets / metadata Drive memakai WIB (UTC+7).

BigQuery TIMESTAMP selalu menyimpan instan UTC; konversi ke WIB hanya untuk tampilan.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

WIB = timezone(timedelta(hours=7))      # Indonesia tidak memakai DST


def to_wib(value: datetime) -> datetime:
    """Datetime tanpa tzinfo dianggap UTC (nilai TIMESTAMP dari BigQuery)."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(WIB)


def fmt_wib(value: datetime | None) -> str:
    return "" if value is None else to_wib(value).strftime("%Y-%m-%d %H:%M:%S WIB")
