"""Google Sheets tiruan untuk uji beban: kuota asli (60 baca + 60 tulis per menit per service account) dan latensi.

Dijalankan DI DALAM proses server Streamlit (diimpor oleh perf/harness.py). Hasil hitungan ditulis ke $PERF_OUT/calls.json
setiap kali ada panggilan, supaya orkestrator bisa membacanya setelah server dimatikan.
"""
import json
import os
import threading
import time
from collections import deque
from pathlib import Path
from unittest.mock import MagicMock

import gspread

LOCK = threading.Lock()
READ_LATENCY = float(os.environ.get("PERF_READ_LATENCY", "0.5"))
WRITE_LATENCY = float(os.environ.get("PERF_WRITE_LATENCY", "0.4"))
QUOTA_PER_MINUTE = 60

STATE = {"reads": 0, "writes": 0, "429_read": 0, "429_write": 0, "opened": 0, "ensure": 0,
         "max_inflight_writes": 0, "requests_per_call": [], "write_times": [], "read_times": [], "rows": [],
         "mutations": []}
_windows = {"read": deque(), "write": deque()}
_inflight = [0]


def _out_path() -> Path:
    return Path(os.environ["PERF_OUT"]) / "calls.json"


def _dump() -> None:
    snapshot = dict(STATE)
    snapshot["rows"] = STATE["rows"]
    _out_path().write_text(json.dumps(snapshot), encoding="utf-8")


def _quota_error() -> gspread.exceptions.APIError:
    resp = MagicMock()
    resp.status_code = 429
    resp.json.return_value = {"error": {"code": 429, "message": "Quota exceeded", "status": "RESOURCE_EXHAUSTED"}}
    return gspread.exceptions.APIError(resp)


def _check_quota(kind: str) -> None:
    now = time.time()
    with LOCK:
        window = _windows[kind]
        while window and now - window[0] > 60:
            window.popleft()
        if len(window) >= QUOTA_PER_MINUTE:
            STATE["429_" + kind] += 1
            _dump()
            raise _quota_error()
        window.append(now)


class WS:
    id, title, col_count = 1, "PERF", 14

    def batch_get(self, ranges):
        _check_quota("read")
        time.sleep(READ_LATENCY)
        with LOCK:
            STATE["reads"] += 1
            STATE["read_times"].append(time.time())
            rows = list(STATE["rows"])
            _dump()
        # kolom kunci: Region, Nama Toko (By Group), Periode, PRODUCT ID, BPOM NUMBER
        return [[[r[i]] for r in rows] for i in range(len(ranges))]


class SH:
    def batch_update(self, body):
        _check_quota("write")
        with LOCK:
            _inflight[0] += 1
            STATE["max_inflight_writes"] = max(STATE["max_inflight_writes"], _inflight[0])
        time.sleep(WRITE_LATENCY)
        with LOCK:
            _inflight[0] -= 1
            kinds = sorted({k for req in body["requests"] for k in req})
            STATE["mutations"].extend(kinds)
            STATE["requests_per_call"].append(len(body["requests"]))
            for req in body["requests"]:
                for row in req["appendCells"]["rows"]:
                    v = [list(c["userEnteredValue"].values())[0] for c in row["values"]]
                    # skema 14 kolom: 0 Region, 1 Nama Toko, 2 Periode, 5 PRODUCT ID, 8 BPOM NUMBER, 9 QTY
                    STATE["rows"].append((v[0], v[1], v[2], v[5], v[8], v[9]))
            STATE["writes"] += 1
            STATE["write_times"].append(time.time())
            _dump()


def open_spreadsheet(*args, **kwargs):
    with LOCK:
        STATE["opened"] += 1
    time.sleep(0.4)
    return SH()


def ensure_worksheet(*args, **kwargs):
    with LOCK:
        STATE["ensure"] += 1
    time.sleep(0.4)
    return WS()
