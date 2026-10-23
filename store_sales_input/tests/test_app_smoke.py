"""Smoke test UI dengan Streamlit AppTest + backend palsu (tanpa jaringan)."""
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest

import clients
import master_cache
import sheets_master
from conftest import make_jpeg
from sheets_master import MasterData, MasterError
from test_service import FakeBQ, FakeMirror, FakeStorage
from validation import Sku, Store

D = date(2026, 10, 23)
APP = Path(__file__).resolve().parent.parent / "app.py"
STORES = {
    "S1": Store("S1", "Toko Satu", "JATENG", "GT", "Non BA"),
    "S2": Store("S2", "Toko Dua", "JABAR", "GT", "Non BA"),
    "S3": Store("S3", "Toko Tiga", "JATENG", "GT", "Non BA"),
}
SKUS = {"K1": Sku("K1", "Produk Satu"), "K2": Sku("K2", "Produk Dua")}
JPEG = ("foto.jpg", make_jpeg((300, 200)), "image/jpeg")


@pytest.fixture
def backend(monkeypatch, tmp_path):
    import streamlit as st
    st.cache_resource.clear()
    st.cache_data.clear()
    monkeypatch.setattr(master_cache, "DEFAULT_SNAPSHOT", tmp_path / "snap.json")
    monkeypatch.setattr(master_cache, "_MEMORY", {})
    be = SimpleNamespace(
        config=SimpleNamespace(master_sheet_id="m", store_master_worksheet="a", sku_master_worksheet="b",
                               evidence_bucket="bkt", admin_username="admin", admin_password="pw-for-tests-only"),
        gc=None, bq_store=FakeBQ(), evidence=FakeStorage(), mirror=FakeMirror())
    monkeypatch.setattr(clients, "build_backends", lambda secrets: be)
    state = {"fail": False}

    def fake_load(gc, mid, s, k):
        if state["fail"]:
            raise MasterError("Header master Store tidak ditemukan.")
        return MasterData(STORES, SKUS, ["diag"])

    monkeypatch.setattr(sheets_master, "load_master", fake_load)
    be.master_state = state
    return be


def fresh():
    at = AppTest.from_file(str(APP), default_timeout=30)
    at.secrets["app"] = {}
    return at.run()


def select_store(at, region="JATENG", store="S1 - Toko Satu"):
    at.selectbox[0].set_value(region)
    at.run()
    at.selectbox[1].set_value(store)
    return at.run()


def fill(at, rows=(("3", "12"), ("0", "0")), photo=JPEG, supervisor="Budi"):
    """Layout: selectbox [0]=Region [1]=Store [2..]=SKU. text_input [0]=supervisor lalu (Sell Out, Stock) per SKU.
    file_uploader [i]=foto baris ke-(i+1)."""
    at = select_store(at)
    at.text_input[0].set_value(supervisor)
    for i, (so, st_) in enumerate(rows):
        at.text_input[1 + 2 * i].set_value(so)
        at.text_input[2 + 2 * i].set_value(st_)
    if photo is not None:
        at.file_uploader[0].set_value(photo)
    return at.run()


def submit(at):
    [b for b in at.button if b.label == "Submit"][0].click()
    return at.run()


def test_page_loads_with_all_skus_prefilled_and_no_pristine_noise(backend):
    at = fresh()
    assert not at.exception and not at.error
    assert at.selectbox[0].label == "Region" and at.selectbox[1].label == "Selected Store"
    assert at.selectbox[0].options == ["JABAR", "JATENG"]
    assert at.selectbox[1].disabled                              # store terkunci sampai region dipilih
    # template awal = SEMUA SKU master, urut master, sebagai teks tetap (bukan dropdown) agar hemat layar HP
    assert len(at.selectbox) == 2
    texts = [m.value for m in at.markdown]
    assert any("**1. K1**" in t and "Produk Satu" in t for t in texts)
    assert any("**2. K2**" in t and "Produk Dua" in t for t in texts)
    assert [t.label for t in at.text_input][:5] == ["Nama Supervisor", "Sell Out", "Stock", "Sell Out", "Stock"]
    assert not at.warning


def test_store_dropdown_is_filtered_by_region(backend):
    at = fresh()
    at.selectbox[0].set_value("JATENG")
    at.run()
    assert not at.selectbox[1].disabled
    assert at.selectbox[1].options == ["S1 - Toko Satu", "S3 - Toko Tiga"]
    at.selectbox[0].set_value("JABAR")
    at.run()
    assert at.selectbox[1].options == ["S2 - Toko Dua"]


def test_submit_with_photo_inserts_all_rows_then_updates_without_duplicates(backend):
    at = fill(fresh(), rows=(("3", "12"), ("0", "7")))
    assert not at.error
    at = submit(at)
    assert not at.exception
    rows = {r["sku_code"]: r for r in backend.bq_store.rows.values()}
    assert (rows["K1"]["sell_out_qty"], rows["K1"]["stock_qty"]) == (3, 12)
    assert (rows["K2"]["sell_out_qty"], rows["K2"]["stock_qty"]) == (0, 7)
    assert len(backend.evidence.uploaded) == 1
    assert any("2 baris baru (INSERT), 0 baris diperbarui" in s.value for s in at.success)
    assert any("Tanggal penjualan" in c.value and "Waktu submit" in c.value and "WIB" in c.value for c in at.caption)

    at = fill(at, rows=(("5", "9"), ("1", "1")))                 # key sama → UPDATE, bukan duplikat
    at = submit(at)
    assert len(backend.bq_store.rows) == 2
    rows = {r["sku_code"]: r for r in backend.bq_store.rows.values()}
    assert (rows["K1"]["sell_out_qty"], rows["K1"]["stock_qty"]) == (5, 9)
    assert any("0 baris baru (INSERT), 2 baris diperbarui" in s.value for s in at.success)


def test_photo_mandatory_even_when_all_zero_and_nothing_written(backend):
    at = fill(fresh(), rows=(("0", "0"), ("0", "0")), photo=None)
    assert any("Foto evidence wajib" in e.value for e in at.error)
    at = submit(at)
    assert backend.bq_store.rows == {} and backend.bq_store.upsert_calls == 0
    assert backend.evidence.uploaded == [] and backend.mirror.calls == []


def test_positive_quantities_without_photo_blocked(backend):
    at = fill(fresh(), rows=(("3", "5"), ("2", "1")), photo=None)
    at = submit(at)
    assert backend.bq_store.rows == {} and backend.evidence.uploaded == []


def test_every_sku_needs_values_required_errors_appear_after_submit_attempt(backend):
    at = fill(fresh(), rows=(("3", "12"),))                      # baris SKU kedua dibiarkan kosong
    assert not any("wajib diisi" in e.value for e in at.error)   # belum klik Submit → tidak merah
    at = submit(at)
    assert any("wajib diisi" in e.value for e in at.error)
    assert backend.bq_store.rows == {} and backend.evidence.uploaded == []


def test_png_rejected_with_clear_message_and_nothing_written(backend):
    png = ("foto.png", make_jpeg((50, 50), fmt="PNG"), "image/png")
    at = fill(fresh(), photo=png)
    assert any("JPG/JPEG" in c.value for c in at.caption)
    at = submit(at)
    assert backend.bq_store.rows == {} and backend.evidence.uploaded == []


def test_invalid_quantities_shown_per_row(backend):
    at = fill(fresh(), rows=(("-2", "1.5"), ("0", "0")))
    msgs = [e.value for e in at.error]
    assert any("Sell Out tidak boleh negatif" in m for m in msgs)
    assert any("Stock harus bilangan bulat" in m for m in msgs)


def test_broken_master_falls_back_to_last_good_data_with_warning(backend):
    at = fresh()
    assert not at.warning
    backend.master_state["fail"] = True                          # spreadsheet master rusak
    [b for b in at.sidebar.button if b.label == "Muat ulang master Store/SKU"][0].click()
    at = at.run()
    assert not at.exception and not at.error
    assert any("data master terakhir yang valid" in w.value for w in at.warning)
    assert at.selectbox[0].options == ["JABAR", "JATENG"]        # data lama tetap dipakai
    at = fill(at)                                                # dan submit tetap bisa
    at = submit(at)
    assert len(backend.bq_store.rows) == 2


# ── Tab Admin ───────────────────────────────────────────────────────────────

def admin_login(at, username="admin", password="pw-for-tests-only"):
    at.text_input(key="adm_user").set_value(username)
    at.text_input(key="adm_pass").set_value(password)
    [b for b in at.button if b.label == "Login"][0].click()
    return at.run()


def metrics(at):
    import re
    return {re.sub(r"^[^\w]+\s*", "", m.label): m.value for m in at.metric if not m.label.startswith("Status ")}


def test_admin_tab_exists_and_input_tab_is_unaffected(backend):
    at = fresh()
    assert [t.label for t in at.tabs] == ["📝 Input Penjualan", "🔐 Admin"]
    assert not at.exception and not at.error
    at = fill(at)
    at = submit(at)                                              # submit tetap jalan normal di samping tab admin
    assert len(backend.bq_store.rows) == 2 and len(backend.evidence.uploaded) == 1


def test_admin_requires_login_and_hides_data_before_it(backend):
    at = fresh()
    assert at.text_input(key="adm_pass").proto.type == 1          # TextInput.Type.PASSWORD
    assert not metrics(at) and not at.dataframe


def test_admin_wrong_password_rejected(backend, monkeypatch):
    import admin_view
    monkeypatch.setattr(admin_view.time, "sleep", lambda s: None)
    at = admin_login(fresh(), password="salah")
    assert any("salah" in e.value for e in at.error)
    assert not metrics(at)


def test_admin_locks_after_repeated_failures(backend, monkeypatch):
    import admin_view
    monkeypatch.setattr(admin_view.time, "sleep", lambda s: None)
    at = fresh()
    for _ in range(5):
        at = admin_login(at, password="salah")
    at = admin_login(at)                                         # password BENAR tapi sedang terkunci
    assert any("Terlalu banyak percobaan" in e.value for e in at.error)
    assert not metrics(at)


def test_admin_disabled_when_not_configured(backend):
    backend.config.admin_username = ""
    backend.config.admin_password = ""
    at = fresh()
    assert any("belum aktif" in i.value for i in at.info)
    assert not at.exception


def test_admin_sees_completeness_per_region_and_store_after_submit(backend):
    at = fill(fresh())                                           # S1 (JATENG): 2 SKU terisi + 1 foto → Lengkap
    at = submit(at)
    at = admin_login(at)
    assert not at.exception and not at.error
    assert metrics(at) == {"Total store": "3", "Lengkap": "1", "Belum lengkap": "0", "Belum submit": "2"}
    assert len(at.get("image")) == 1                              # galeri menampilkan foto evidence
    assert any("https://signed.example/F1" in str(i.proto) for i in at.get("image"))


def test_admin_region_filter_and_logout(backend):
    at = admin_login(submit(fill(fresh())))
    at.selectbox(key="adm_region").set_value("JABAR")
    at.run()
    assert metrics(at)["Total store"] == "1" and metrics(at)["Belum submit"] == "1"
    assert len(at.get("image")) == 0
    [b for b in at.button if b.label == "Logout"][0].click()
    at = at.run()
    assert not metrics(at) and at.text_input(key="adm_pass")      # kembali ke form login


def test_admin_partial_when_photo_missing_for_complete_skus(backend):
    # tulis langsung ke fake BigQuery: semua SKU terisi tapi tanpa foto → "Belum lengkap"
    from datetime import datetime
    from test_service import T1
    from validation import make_transaction_id
    today = datetime.now(__import__("timeutil").WIB).date()
    for sku in ("K1", "K2"):
        backend.bq_store.rows[(today, "S1", sku)] = dict(
            transaction_id=make_transaction_id(today, "S1", sku), tanggal_penjualan=today, store_id="S1",
            store_name="Toko Satu", region="JATENG", store_group="GT", sku_code=sku, sku_name=sku,
            sell_out_qty=1, stock_qty=1, supervisor_name="Budi", evidence_file_id=None, evidence_file_url=None,
            evidence_file_name=None, evidence_uploaded_at=None, created_at=T1, updated_at=T1)
    at = admin_login(fresh())
    assert metrics(at) == {"Total store": "3", "Lengkap": "0", "Belum lengkap": "1", "Belum submit": "2"}
    assert any("Belum ada foto" in i.value for i in at.info)


def test_form_is_not_red_right_after_successful_submit(backend):
    at = submit(fill(fresh()))
    assert any("INSERT" in x.value for x in at.success)
    assert not at.error                                            # supervisor masih terisi, tapi form kosong → tanpa error
