from datetime import datetime, timezone

import pytest

from master_cache import load_master_with_fallback, load_snapshot, save_snapshot
from sheets_master import MasterData, MasterError
from validation import Sku, Store

T0 = datetime(2026, 10, 23, 1, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 23, 2, 0, tzinfo=timezone.utc)


def data(n=2):
    stores = {f"S{i}": Store(f"S{i}", f"Toko {i}", "R", "G", "Non BA") for i in range(n)}
    return MasterData(stores, {"K1": Sku("K1", "Produk")}, ["diag"])


def broken():
    raise MasterError("Header master Store tidak ditemukan.")


def test_success_returns_fresh_and_saves_snapshot(tmp_path):
    snap = tmp_path / "c" / "m.json"
    mem = {}
    r = load_master_with_fallback(lambda: data(), snapshot_path=snap, memory=mem, now=lambda: T0)
    assert not r.stale and r.error is None and r.loaded_at == T0 and snap.exists()
    loaded = load_snapshot(snap)
    assert set(loaded.data.stores) == {"S0", "S1"} and loaded.data.skus["K1"].name == "Produk"
    assert loaded.loaded_at == T0


def test_failure_after_success_uses_last_good_from_memory(tmp_path):
    mem = {}
    load_master_with_fallback(lambda: data(3), snapshot_path=tmp_path / "m.json", memory=mem, now=lambda: T0)
    r = load_master_with_fallback(broken, snapshot_path=tmp_path / "m.json", memory=mem, now=lambda: T1)
    assert r.stale and "Header master" in r.error
    assert len(r.data.stores) == 3 and r.loaded_at == T0           # data & waktu LAMA, bukan T1


def test_failure_uses_disk_snapshot_after_restart(tmp_path):
    snap = tmp_path / "m.json"
    load_master_with_fallback(lambda: data(4), snapshot_path=snap, memory={}, now=lambda: T0)
    r = load_master_with_fallback(broken, snapshot_path=snap, memory={})      # memori kosong = proses baru
    assert r.stale and len(r.data.stores) == 4


def test_failure_without_any_backup_raises_original_error(tmp_path):
    with pytest.raises(MasterError, match="Header master"):
        load_master_with_fallback(broken, snapshot_path=tmp_path / "none.json", memory={})


def test_bad_run_does_not_overwrite_good_snapshot(tmp_path):
    snap = tmp_path / "m.json"
    load_master_with_fallback(lambda: data(2), snapshot_path=snap, memory={}, now=lambda: T0)
    load_master_with_fallback(broken, snapshot_path=snap, memory={})
    assert len(load_snapshot(snap).data.stores) == 2 and load_snapshot(snap).loaded_at == T0


def test_recovers_when_sheet_is_fixed(tmp_path):
    mem = {}
    load_master_with_fallback(lambda: data(2), snapshot_path=tmp_path / "m.json", memory=mem, now=lambda: T0)
    assert load_master_with_fallback(broken, snapshot_path=tmp_path / "m.json", memory=mem).stale
    r = load_master_with_fallback(lambda: data(5), snapshot_path=tmp_path / "m.json", memory=mem, now=lambda: T1)
    assert not r.stale and len(r.data.stores) == 5 and r.loaded_at == T1


def test_corrupt_snapshot_is_ignored(tmp_path):
    snap = tmp_path / "m.json"
    snap.write_text("{not json", encoding="utf-8")
    assert load_snapshot(snap) is None
    with pytest.raises(MasterError):
        load_master_with_fallback(broken, snapshot_path=snap, memory={})


def test_unwritable_snapshot_location_does_not_break_loading(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    r = load_master_with_fallback(lambda: data(), snapshot_path=blocker / "sub" / "m.json", memory={})
    assert not r.stale and len(r.data.stores) == 2
