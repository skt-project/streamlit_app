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


class FakeSupervisors:
    """Meniru SupervisorDirectory: SPV master per store + nama manual dari transaksi (semua store satu distributor)."""

    def __init__(self, bq_store):
        self.bq_store = bq_store
        self.master = {"S1": ("Riko Saputra",), "S3": ("Riko Saputra", "Mila Yusniar")}      # S2: tidak ada di master
        self.fail = False
        self.calls = 0

    def load(self, store_ids):
        from supervisors import SupervisorData, SupervisorLookupError
        self.calls += 1
        if self.fail:
            raise SupervisorLookupError("Daftar SPV gagal dimuat dari BigQuery (Fake).")
        used = tuple(sorted({r["supervisor_name"] for r in self.bq_store.rows.values()}))
        return SupervisorData(master=dict(self.master), custom={s: used for s in store_ids})


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
    be.supervisors = FakeSupervisors(be.bq_store)
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


def pick_supervisor(at, name="Riko Saputra", other=None):
    """selectbox [2] = Nama Supervisor. `other`: ketik nama lewat "Nama SPV Lainnya"."""
    from supervisors import SPV_OTHER
    sb = [s for s in at.selectbox if s.label == "Nama Supervisor"][0]
    sb.set_value(SPV_OTHER if other else name)
    at = at.run()
    if other:
        [t for t in at.text_input if t.label == "Ketik nama supervisor"][0].set_value(other)
        at = at.run()
    return at


def fill(at, rows=(("3", "12"), ("0", "0")), photo=JPEG, supervisor="Riko Saputra", other=None):
    """Layout: selectbox [0]=Region [1]=Store [2]=Nama Supervisor. Qty dicari lewat label (Sell Out, Stock) per SKU.
    file_uploader [i]=foto baris ke-(i+1)."""
    at = pick_supervisor(select_store(at), supervisor, other)
    qty = [t for t in at.text_input if t.label in ("Sell Out", "Stock")]
    for i, (so, st_) in enumerate(rows):
        qty[2 * i].set_value(so)
        qty[2 * i + 1].set_value(st_)
    if photo is not None:
        at.file_uploader[0].set_value(photo)
    return at.run()


def submit(at):
    [b for b in at.button if b.label == "Submit"][0].click()
    return at.run()


def test_page_loads_with_all_skus_prefilled_and_no_pristine_noise(backend):
    at = fresh()
    assert not at.exception and not at.error
    assert [t.value for t in at.title] == ["Facerinna Submission Program"]
    assert not any("Catat Sell Out" in c.value for c in at.caption)      # tanpa deskripsi di bawah judul
    assert at.selectbox[0].label == "Region" and at.selectbox[1].label == "Selected Store"
    assert at.selectbox[0].options == ["JABAR", "JATENG"]
    assert at.selectbox[1].disabled                              # store terkunci sampai region dipilih
    # template awal = SEMUA SKU master, urut master, sebagai teks tetap (bukan dropdown) agar hemat layar HP
    assert [s.label for s in at.selectbox] == ["Region", "Selected Store", "Nama Supervisor"]
    assert at.selectbox[2].disabled                              # dropdown supervisor terkunci sampai store dipilih
    assert at.selectbox[2].options == ["Nama SPV Lainnya"]
    texts = [m.value for m in at.markdown]
    assert any("**1. K1**" in t and "Produk Satu" in t for t in texts)
    assert any("**2. K2**" in t and "Produk Dua" in t for t in texts)
    assert [t.label for t in at.text_input][:4] == ["Sell Out", "Stock", "Sell Out", "Stock"]   # supervisor bukan ketik bebas
    assert not at.warning


def test_quantity_placeholder_says_fill_here_not_zero(backend):
    at = fresh()
    qty_inputs = [t for t in at.text_input if t.label in ("Sell Out", "Stock")]
    assert len(qty_inputs) == 2 * len(SKUS)
    assert all(t.placeholder == "isi quantity disini" for t in qty_inputs)
    assert all(t.value == "" for t in qty_inputs)                # kosong sungguhan, bukan "0"


def test_delete_button_is_labelled_and_removes_only_that_sku(backend):
    at = fresh()
    dels = [b for b in at.button if b.label == "Delete"]            # teks saja, tanpa ikon
    assert len(dels) == len(SKUS)
    assert not any("🗑" in b.label for b in at.button)
    assert all("Hapus SKU ini dari daftar" in b.help for b in dels)
    dels[0].click()
    at = at.run()
    texts = [m.value for m in at.markdown]
    assert not any("**1. K1**" in t for t in texts) and any("K2" in t and "Produk Dua" in t for t in texts)
    assert len([t for t in at.text_input if t.label == "Sell Out"]) == len(SKUS) - 1


def test_store_dropdown_is_filtered_by_region(backend):
    at = fresh()
    at.selectbox[0].set_value("JATENG")
    at.run()
    assert not at.selectbox[1].disabled
    assert at.selectbox[1].options == ["S1 - Toko Satu", "S3 - Toko Tiga"]
    at.selectbox[0].set_value("JABAR")
    at.run()
    assert at.selectbox[1].options == ["S2 - Toko Dua"]


def spv_select(at):
    return [s for s in at.selectbox if s.label == "Nama Supervisor"][0]


def test_supervisor_dropdown_lists_master_spv_of_the_store_with_other_option_last(backend):
    at = select_store(fresh())
    assert not spv_select(at).disabled
    assert spv_select(at).options == ["Riko Saputra", "Nama SPV Lainnya"]
    at = select_store(at, "JATENG", "S3 - Toko Tiga")
    assert spv_select(at).options == ["Mila Yusniar", "Riko Saputra", "Nama SPV Lainnya"]


def test_supervisor_from_dropdown_is_saved_and_no_free_text_box_by_default(backend):
    at = fill(fresh())
    assert not [t for t in at.text_input if t.label == "Ketik nama supervisor"]
    at = submit(at)
    assert {r["supervisor_name"] for r in backend.bq_store.rows.values()} == {"Riko Saputra"}


def test_other_option_shows_text_field_and_requires_a_name(backend):
    at = pick_supervisor(select_store(fresh()), other="x")
    field = [t for t in at.text_input if t.label == "Ketik nama supervisor"][0]
    field.set_value("   ")
    at = at.run()
    at = submit(at)
    assert any("Nama Supervisor wajib diisi" in e.value for e in at.error)
    assert backend.bq_store.rows == {}


def test_typed_spv_name_is_saved_and_listed_next_time_for_same_distributor(backend):
    at = fill(fresh(), supervisor="-", other="  Budi   Santoso ")
    assert not at.error
    at = submit(at)
    assert {r["supervisor_name"] for r in backend.bq_store.rows.values()} == {"Budi Santoso"}      # spasi dirapikan
    at = select_store(at, "JATENG", "S3 - Toko Tiga")                                              # distributor sama
    assert spv_select(at).options == ["Mila Yusniar", "Riko Saputra", "Budi Santoso", "Nama SPV Lainnya"]


def test_typed_name_matching_an_existing_option_reuses_its_spelling(backend):
    at = fill(fresh(), supervisor="-", other="riko  SAPUTRA")
    at = submit(at)
    assert {r["supervisor_name"] for r in backend.bq_store.rows.values()} == {"Riko Saputra"}


def test_store_without_master_spv_hints_to_use_other_option(backend):
    at = select_store(fresh(), "JABAR", "S2 - Toko Dua")
    assert spv_select(at).options == ["Nama SPV Lainnya"]
    assert any("belum ada di master distributor" in c.value for c in at.caption)


def test_supervisor_lookup_failure_degrades_to_manual_entry(backend):
    backend.supervisors.fail = True
    at = select_store(fresh())
    assert any("Daftar SPV gagal dimuat" in w.value for w in at.warning)
    assert spv_select(at).options == ["Nama SPV Lainnya"]
    at = submit(fill(at, supervisor="-", other="Budi Santoso"))
    assert {r["supervisor_name"] for r in backend.bq_store.rows.values()} == {"Budi Santoso"}


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


def test_submit_without_any_photo_is_allowed_all_zero(backend):
    at = fill(fresh(), rows=(("0", "0"), ("0", "0")), photo=None)
    assert not at.error and not at.warning
    at = submit(at)
    assert not at.exception
    assert len(backend.bq_store.rows) == 2 and backend.evidence.uploaded == []


def test_submit_without_photo_allowed_with_positive_quantities_and_no_warning(backend):
    at = fill(fresh(), rows=(("3", "5"), ("2", "1")), photo=None)
    assert not at.error and not at.warning
    at = submit(at)
    assert len(backend.bq_store.rows) == 2 and backend.evidence.uploaded == []


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


def test_admin_complete_even_without_photo(backend):
    # tulis langsung ke fake BigQuery: semua SKU terisi tanpa foto → tetap "Lengkap" (foto opsional)
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
    assert metrics(at) == {"Total store": "3", "Lengkap": "1", "Belum lengkap": "0", "Belum submit": "2"}
    assert any("Belum ada foto" in i.value for i in at.info)


def test_form_is_not_red_right_after_successful_submit(backend):
    at = submit(fill(fresh()))
    assert any("INSERT" in x.value for x in at.success)
    assert not at.error                                            # supervisor masih terisi, tapi form kosong → tanpa error


# ── Pagination galeri admin ─────────────────────────────────────────────────

def seed_photos(backend):
    """Tiap store x SKU punya foto → 3 store x 2 SKU = 6 foto (S1, S3 di JATENG; S2 di JABAR)."""
    from datetime import datetime
    from test_service import T1
    from timeutil import WIB
    from validation import make_transaction_id
    today = datetime.now(WIB).date()
    for sid, name in (("S1", "Toko Satu"), ("S2", "Toko Dua"), ("S3", "Toko Tiga")):
        for sku in ("K1", "K2"):
            backend.bq_store.rows[(today, sid, sku)] = dict(
                transaction_id=make_transaction_id(today, sid, sku), tanggal_penjualan=today, store_id=sid,
                store_name=name, region="X", store_group="GT", sku_code=sku, sku_name=sku, sell_out_qty=1,
                stock_qty=1, supervisor_name="Budi", evidence_file_id=f"{sid}-{sku}.jpg",
                evidence_file_url="u", evidence_file_name="n.jpg", evidence_uploaded_at=T1,
                created_at=T1, updated_at=T1)


def gallery_files(at):
    return [i.proto.imgs[0].url.rsplit("/", 1)[-1] for i in at.get("image")]


def test_gallery_is_paginated_with_prev_next_top_and_bottom(backend, monkeypatch):
    import admin_view
    monkeypatch.setattr(admin_view, "PAGE_SIZE", 2)
    seed_photos(backend)
    at = admin_login(fresh())
    # urutan galeri: region lalu nama store → JABAR/Toko Dua, JATENG/Toko Satu, JATENG/Toko Tiga
    assert gallery_files(at) == ["S2-K1.jpg", "S2-K2.jpg"]                 # halaman 1 dari 3
    assert at.button(key="adm_prev_top").disabled and at.button(key="adm_prev_bottom").disabled
    assert not at.button(key="adm_next_top").disabled and not at.button(key="adm_next_bottom").disabled
    assert any("6 foto" in c.value and "1–2" in c.value for c in at.caption)

    at.button(key="adm_next_bottom").click()                               # tombol BAWAH juga bekerja
    at = at.run()
    assert gallery_files(at) == ["S1-K1.jpg", "S1-K2.jpg"]
    assert not at.button(key="adm_prev_top").disabled
    assert at.selectbox(key="adm_page").value == 2

    at.selectbox(key="adm_page").set_value(3)                              # loncat ke halaman tertentu
    at = at.run()
    assert gallery_files(at) == ["S3-K1.jpg", "S3-K2.jpg"]
    assert at.button(key="adm_next_top").disabled and at.button(key="adm_next_bottom").disabled

    at.button(key="adm_prev_top").click()
    at = at.run()
    assert at.selectbox(key="adm_page").value == 2


def test_gallery_pages_cover_every_photo_exactly_once(backend, monkeypatch):
    import admin_view
    monkeypatch.setattr(admin_view, "PAGE_SIZE", 2)
    seed_photos(backend)
    at = admin_login(fresh())
    seen = gallery_files(at)
    for _ in range(2):
        at.button(key="adm_next_top").click()
        at = at.run()
        seen += gallery_files(at)
    assert sorted(seen) == sorted(f"{s}-{k}.jpg" for s in ("S1", "S2", "S3") for k in ("K1", "K2"))


def test_changing_filter_returns_gallery_to_page_one(backend, monkeypatch):
    import admin_view
    monkeypatch.setattr(admin_view, "PAGE_SIZE", 2)
    seed_photos(backend)
    at = admin_login(fresh())
    at.button(key="adm_next_top").click()
    at = at.run()
    assert at.selectbox(key="adm_page").value == 2
    at.selectbox(key="adm_region").set_value("JATENG")
    at = at.run()
    assert at.selectbox(key="adm_page").value == 1


def test_single_page_gallery_has_no_pager(backend):
    seed_photos(backend)                                                   # PAGE_SIZE asli 8 > 6 foto
    at = admin_login(fresh())
    assert len(gallery_files(at)) == 6
    assert not [b for b in at.button if b.key and b.key.startswith("adm_prev_")]


# ── Rentang tanggal di tab Admin ────────────────────────────────────────────

def put_row(backend, d, sid, sku, photo=True):
    from test_service import T1
    from validation import make_transaction_id
    backend.bq_store.rows[(d, sid, sku)] = dict(
        transaction_id=make_transaction_id(d, sid, sku), tanggal_penjualan=d, store_id=sid, store_name=sid,
        region="X", store_group="GT", sku_code=sku, sku_name=sku, sell_out_qty=1, stock_qty=2,
        supervisor_name="Budi", evidence_file_id=f"{sid}-{sku}-{d.isoformat()}.jpg" if photo else None,
        evidence_file_url="u", evidence_file_name="n.jpg", evidence_uploaded_at=T1, created_at=T1, updated_at=T1)


def today_wib():
    from datetime import datetime
    from timeutil import WIB
    return datetime.now(WIB).date()


def test_admin_default_range_is_last_7_days_so_weekly_visits_are_found(backend):
    from datetime import timedelta
    t = today_wib()
    for sku in ("K1", "K2"):
        put_row(backend, t - timedelta(days=6), "S1", sku, photo=(sku == "K1"))     # 6 hari lalu: lengkap
        put_row(backend, t - timedelta(days=7), "S2", sku)                          # 7 hari lalu: DI LUAR rentang
    at = admin_login(fresh())
    assert at.date_input(key="adm_range").value == (t - timedelta(days=6), t)
    assert metrics(at) == {"Total store": "3", "Lengkap": "1", "Belum lengkap": "0", "Belum submit": "2"}
    assert any("s/d" in c.value and "minimal satu tanggal kunjungan" in c.value for c in at.caption)


def test_admin_visit_last_week_counts_when_range_covers_it_and_not_otherwise(backend):
    from datetime import timedelta
    t = today_wib()
    for sku in ("K1", "K2"):
        put_row(backend, t - timedelta(days=5), "S1", sku, photo=(sku == "K1"))
    at = admin_login(fresh())
    assert metrics(at)["Lengkap"] == "1"
    at.date_input(key="adm_range").set_value((t - timedelta(days=2), t))               # rentang tidak mencakup kunjungan
    at = at.run()
    assert metrics(at) == {"Total store": "3", "Lengkap": "0", "Belum lengkap": "0", "Belum submit": "3"}
    at.date_input(key="adm_range").set_value((t - timedelta(days=5), t - timedelta(days=5)))   # satu hari saja
    at = at.run()
    assert metrics(at)["Lengkap"] == "1"                                                # batas tanggal ikut dihitung


def test_admin_store_with_two_visits_complete_one_partial_other(backend):
    from datetime import timedelta
    t = today_wib()
    early, late = t - timedelta(days=4), t
    for sku in ("K1", "K2"):
        put_row(backend, early, "S1", sku, photo=(sku == "K1"))                         # lengkap
    put_row(backend, late, "S1", "K1", photo=False)                                      # kunjungan terbaru: belum lengkap
    at = admin_login(fresh())
    assert metrics(at)["Lengkap"] == "1" and metrics(at)["Belum lengkap"] == "0"
    # galeri menampilkan foto dari kunjungan di rentang, lengkap dengan tanggalnya
    cards = " ".join(m.value for m in at.markdown)
    assert f"Tanggal {early.isoformat()}" in cards
    # detail store: default ke kunjungan TERBAIK (yang lengkap), bisa pindah ke kunjungan lain
    store_pick = [s for s in at.selectbox if s.label == "Store"][0]
    store_pick.set_value("S1 - Toko Satu")
    at = at.run()
    visit = [s for s in at.selectbox if s.label == "Tanggal kunjungan"][0]
    assert visit.value == early
    visit.set_value(late)
    at = at.run()
    frames = [d.value for d in at.dataframe if "Isian" in d.value.columns]
    assert list(frames[-1]["Isian"]) == ["Terisi", "Belum terisi"]


def test_admin_incomplete_range_selection_and_too_long_range(backend):
    from datetime import timedelta
    t = today_wib()
    at = admin_login(fresh())
    at.date_input(key="adm_range").set_value((t - timedelta(days=3),))                  # baru satu ujung dipilih
    at = at.run()
    assert any("Pilih tanggal mulai dan tanggal akhir" in i.value for i in at.info) and not metrics(at)
    at.date_input(key="adm_range").set_value((t - timedelta(days=200), t))
    at = at.run()
    assert any("maksimal 92 hari" in w.value for w in at.warning) and not metrics(at)
