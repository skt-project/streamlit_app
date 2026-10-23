"""Login admin: perbandingan kredensial dan pembatasan percobaan.

Kredensial TIDAK ada di kode; dibaca dari secrets `[admin]` (username, password). Tanpa secrets itu,
tab admin nonaktif (tidak ada kredensial bawaan).

Catatan: pembatasan percobaan hanya per sesi browser (state disimpan di session), jadi memperlambat
tebak-tebakan tetapi bukan perlindungan penuh. Keamanan sebenarnya tetap bergantung pada kekuatan password.
"""
from __future__ import annotations

import hmac

MAX_ATTEMPTS = 5
LOCK_SECONDS = 60


def credentials_match(username: str, password: str, expected_user: str, expected_pass: str) -> bool:
    """Bandingkan dengan waktu konstan; kedua sisi selalu dievaluasi (tidak ada short-circuit)."""
    if not expected_user or not expected_pass:
        return False
    user_ok = hmac.compare_digest((username or "").strip().encode("utf-8"), expected_user.encode("utf-8"))
    pass_ok = hmac.compare_digest((password or "").encode("utf-8"), expected_pass.encode("utf-8"))
    return user_ok and pass_ok


def lock_remaining(state: dict, now: float) -> float:
    """Sisa detik terkunci (0 bila tidak terkunci)."""
    return max(0.0, state.get("locked_until", 0.0) - now)


def record_failure(state: dict, now: float) -> None:
    state["failures"] = state.get("failures", 0) + 1
    if state["failures"] >= MAX_ATTEMPTS:
        state["locked_until"] = now + LOCK_SECONDS
        state["failures"] = 0


def reset(state: dict) -> None:
    state["failures"] = 0
    state["locked_until"] = 0.0
