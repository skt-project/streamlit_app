"""Tes kecil untuk perangkat uji beban (perf/): data buatan harus unik per pengguna dan statistik benar.
Uji bebannya sendiri TIDAK dijalankan di sini (butuh Playwright + browser): lihat perf/README.md."""
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "perf"))
import reference_data as r  # noqa: E402
import run_load_test as rl  # noqa: E402


def test_generated_store_list_gives_every_user_a_distinct_choice(tmp_path):
    users = 100
    path = tmp_path / "stores.xlsx"
    per_region = rl.build_stores(path, users)
    tokos = r.load_stores(path)
    by_region = {}
    for t in tokos:
        by_region.setdefault(t.region, []).append(t)
    assert sorted(by_region) == sorted(rl.REGIONS) and all(len(v) == per_region for v in by_region.values())
    assert per_region >= math.ceil(users / len(rl.REGIONS))
    groups = [t for t in tokos if t.is_group]
    assert len(groups) == len(rl.REGIONS) * rl.GROUPS_PER_REGION and all(t.outlets == 3 for t in groups)
    ordered = {reg: sorted(v, key=lambda t: t.name.casefold()) for reg, v in by_region.items()}
    picks = {(rl.REGIONS[i % len(rl.REGIONS)], ordered[rl.REGIONS[i % len(rl.REGIONS)]][i // len(rl.REGIONS)].name) for i in range(users)}
    assert len(picks) == users                                           # tidak ada dua pengguna memilih toko yang sama (akan ditolak duplikat)


def test_stats_percentiles():
    assert rl.stats([]) is None
    s = rl.stats([float(x) for x in range(1, 101)])
    assert s == {"n": 100, "median": 50.5, "p95": 95.0, "max": 100.0}


def test_fake_sheets_enforces_the_60_per_minute_quota(tmp_path, monkeypatch):
    import gspread
    import pytest
    monkeypatch.setenv("PERF_OUT", str(tmp_path))
    monkeypatch.setenv("PERF_READ_LATENCY", "0")
    import importlib
    fake = importlib.import_module("fakegoogle")
    fake = importlib.reload(fake)
    ws = fake.WS()
    for _ in range(60):
        ws.batch_get(["A:A"] * 5)
    with pytest.raises(gspread.exceptions.APIError):
        ws.batch_get(["A:A"] * 5)
    assert fake.STATE["reads"] == 60 and fake.STATE["429_read"] == 1
