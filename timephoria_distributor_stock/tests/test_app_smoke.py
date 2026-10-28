"""Smoke test UI dengan Streamlit AppTest + backend palsu (tanpa jaringan)."""
import re
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest

import clients
import master_data
import opening_stock
from conftest import make_jpeg
from master_data import MasterData, MasterError
from test_service import T1, FakeBQ, FakeMirror, FakeStorage
from timeutil import WIB
from validation import Distributor, Sku, make_transaction_id

APP = Path(__file__).resolve().parent.parent / "app.py"
PAGE = "TIMEPHORIA Smart Coverage Tester Feedback"
NOTE = "Isi jumlah stok Tester Smart Coverage yang tersisa di Distributor dan sertakan bukti fotonya (wajib)"
DISTS = {
    "D1": Distributor("D1", "Gudang Satu", "R1", "Ani"),
    "D2": Distributor("D2", "Gudang Dua", "R1", "Budi"),
    "D3": Distributor("D3", "Gudang Tiga", "R2", "Ani"),
    "D4": Distributor("D4", "Gudang Kosong", "(Tanpa Region)", "Cici"),
}
SKUS = {"K1": Sku("K1", "Produk Satu"), "K2": Sku("K2", "Produk Dua")}
JPEG = ("foto.jpg", make_jpeg((80, 60)), "image/jpeg")


@pytest.fixture
def backend(monkeypatch, tmp_path):
    import streamlit as st
    st.cache_resource.clear()
    st.cache_data.clear()
    monkeypatch.setattr(master_data, "DEFAULT_SNAPSHOT", tmp_path / "snap.json")
    monkeypatch.setattr(master_data, "_MEMORY", {})
    state = {"fail": False, "distributors": DISTS, "skus": SKUS, "diagnostics": ["diag"], "ho_stocks": {}}
    be = SimpleNamespace(
        config=SimpleNamespace(
            bigquery_project="proj", bigquery_dataset="gt_schema", sku_codes=("K1", "K2"),
            evidence_bucket="bkt", output_sheet_id="sheet", ho_stock_worksheet="Opening Stock",
            admin_username="admin", admin_password="pw-for-tests-only",
        ),
        bq_client=None, gc=None, bq_store=FakeBQ(), evidence=FakeStorage(), mirror=FakeMirror(),
    )
    monkeypatch.setattr(clients, "build_backends", lambda secrets: be)

    def fake_load(client, project, dataset, codes):
        if state["fail"]:
            raise MasterError("query gagal")
        return MasterData(state["distributors"], state["skus"], list(state["diagnostics"]))

    monkeypatch.setattr(master_data, "load_master", fake_load)

    def fake_ho(gc, sheet_id, worksheet):
        return {
            "url": "https://docs.google.com/spreadsheets/d/sheet/edit?gid=1#gid=1",
            "stocks": state["ho_stocks"],
        }

    monkeypatch.setattr(opening_stock, "load_ho_stock", fake_ho)
    be.master_state = state
    return be


def fresh():
    at = AppTest.from_file(str(APP), default_timeout=60)
    at.secrets["app"] = {}
    return at.run()


def choose(at, region="R1", spv="Ani", dist="D1 - Gudang Satu"):
    at.selectbox(key="region").set_value(region).run()
    at.selectbox(key=f"spv_{region}").set_value(spv).run()
    at.selectbox(key=f"dist_{region}_{spv}").set_value(dist).run()
    return at


def fill(at, stocks=("5", "0"), photo=JPEG):
    at = choose(at)
    boxes = [t for t in at.text_input if t.label == "Stock"]
    for box, value in zip(boxes, stocks):
        box.set_value(value)
    if photo is not None:
        at.file_uploader[0].set_value(photo)
    return at.run()


def submit(at):
    [b for b in at.button if b.label == "Submit"][0].click()
    return at.run()


def metrics(at):
    out = {}
    for metric in at.metric:
        if metric.label.startswith("Status "):
            continue
        out[re.sub(r"^[^\w]+\s*", "", metric.label)] = str(metric.value)
    return out


def test_page_title_sku_text_placeholder_and_no_initial_errors(backend):
    at = fresh()
    assert not at.exception and not at.error
    assert [t.value for t in at.title] == [PAGE]
    assert any(s.value == "DAFTAR SKU" for s in at.subheader)
    assert any(NOTE in m.value for m in at.markdown)
    assert any("Jika ada selisih antara Stok Distributor dan Stok versi Perusahaan" in m.value for m in at.markdown)
    assert any("Data Stock yang dimiliki tim Head Office:** —" in m.value for m in at.markdown)
    assert [t.label for t in at.tabs] == ["📝 Input Stock", "🔐 Admin"]
    stocks = [t for t in at.text_input if t.label == "Stock"]
    assert len(stocks) == 2
    assert all(t.placeholder == "isi quantity disini" and t.value == "" for t in stocks)
    assert any("**1. K1**" in m.value and "Produk Satu" in m.value for m in at.markdown)
    assert any("**2. K2**" in m.value and "Produk Dua" in m.value for m in at.markdown)
    assert at.selectbox(key="spv_None").disabled and at.selectbox(key="dist_None_None").disabled
    assert backend.bq_store.touched == 0
    assert not at.warning


def test_head_office_stock_is_shown_above_each_input(backend):
    backend.master_state["ho_stocks"] = {"gudang satu": {"K1": 8}}
    at = choose(fresh())
    texts = [m.value for m in at.markdown]
    assert any("**Data Stock yang dimiliki tim Head Office:** 8" in t for t in texts)
    assert any("**Data Stock yang dimiliki tim Head Office:** tidak ada" in t for t in texts)
    assert any("spreadsheet ini" in t for t in texts)


def test_dependent_dropdowns_and_sku_rows_have_no_delete(backend):
    at = fresh()
    at.selectbox(key="region").set_value("R1").run()
    assert at.selectbox(key="spv_R1").options == ["Ani", "Budi"]
    at.selectbox(key="spv_R1").set_value("Ani").run()
    assert at.selectbox(key="dist_R1_Ani").options == ["D1 - Gudang Satu"]
    at.selectbox(key="region").set_value("R2").run()
    at.selectbox(key="spv_R2").set_value("Ani").run()
    assert at.selectbox(key="dist_R2_Ani").options == ["D3 - Gudang Tiga"]

    at = fresh()
    assert not [b for b in at.button if b.label in ("Delete", "➕ Tambah SKU")]
    texts = " ".join(m.value for m in at.markdown)
    assert "Produk Satu" in texts and "Produk Dua" in texts
    assert len([t for t in at.text_input if t.label == "Stock"]) == 2


def test_fifteen_sku_rows_follow_the_configured_list(backend):
    backend.master_state["skus"] = {f"TST{i:06d}": Sku(f"TST{i:06d}", f"Produk {i}") for i in range(1, 16)}
    at = fresh()
    assert len([t for t in at.text_input if t.label == "Stock"]) == 15
    assert not [b for b in at.button if b.label in ("Delete", "➕ Tambah SKU")]
    texts = " ".join(m.value for m in at.markdown)
    assert "**1. TST000001**" in texts and "**15. TST000015**" in texts


def test_submit_zero_without_photo_and_block_positive_without_photo(backend):
    at = submit(fill(fresh(), stocks=("0", "0"), photo=None))
    assert not at.exception and any("2 baris baru (INSERT), 0 diperbarui" in s.value for s in at.success)
    assert any("Tanggal stock" in c.value and "WIB" in c.value for c in at.caption)
    assert backend.evidence.uploaded == [] and len(backend.bq_store.rows) == 2
    assert not at.error

    backend.bq_store.rows.clear()
    backend.bq_store.upsert_calls = 0
    backend.mirror.calls.clear()
    at = fill(fresh(), stocks=("3", "0"), photo=None)
    assert any("Foto evidence wajib" in e.value for e in at.error)
    at = submit(at)
    assert backend.bq_store.upsert_calls == 0 and backend.bq_store.rows == {}
    assert backend.evidence.uploaded == [] and backend.mirror.calls == []


def test_submit_with_photo_then_update_and_form_is_not_red(backend):
    at = submit(fill(fresh(), stocks=("5", "0")))
    assert not at.error and len(backend.evidence.uploaded) == 1
    rows = {r["sku_code"]: r for r in backend.bq_store.rows.values()}
    assert rows["K1"]["stock_qty"] == 5 and rows["K2"]["stock_qty"] == 0
    at = submit(fill(at, stocks=("8", "1")))
    assert len(backend.bq_store.rows) == 2
    assert any("0 baris baru (INSERT), 2 diperbarui" in s.value for s in at.success)
    assert not at.error


def test_png_rejected_and_required_errors_wait_for_submit(backend):
    png = ("salah.jpg", make_jpeg((40, 40), fmt="PNG"), "image/png")
    at = fill(fresh(), photo=png)
    assert any("JPG/JPEG" in c.value for c in at.caption)
    at = submit(at)
    assert backend.bq_store.rows == {} and backend.evidence.uploaded == []

    at = fill(fresh(), stocks=("4",), photo=JPEG)
    assert not any("wajib diisi" in e.value for e in at.error)
    at = submit(at)
    assert any("wajib diisi" in e.value for e in at.error)
    assert backend.bq_store.upsert_calls == 0


def test_missing_sku_name_warns_and_master_fallback(backend):
    backend.master_state["skus"] = {"K1": Sku("K1", ""), "K2": Sku("K2", "Produk Dua")}
    at = fresh()
    assert any("tanpa nama" in w.value for w in at.sidebar.warning)
    assert any("**1. K1**" in m.value for m in at.markdown)
    backend.master_state["skus"] = SKUS
    backend.master_state["fail"] = True
    [b for b in at.sidebar.button if b.label == "Muat ulang master"][0].click().run()
    assert any("data terakhir yang valid" in w.value and "penyebab" in w.value for w in at.warning)


def admin_login(at, username="admin", password="pw-for-tests-only"):
    at.text_input(key="adm_user").set_value(username)
    at.text_input(key="adm_pass").set_value(password)
    [b for b in at.button if b.label == "Login"][0].click()
    return at.run()


def today():
    return datetime.now(WIB).date()


def put_row(backend, day, code, sku, stock=2, photo=True, name="Gudang"):
    backend.bq_store.rows[(day, code, sku)] = dict(
        transaction_id=make_transaction_id(day, code, sku), tanggal_stock=day, distributor_code=code,
        distributor_name=name, region="R1", supervisor_name="Ani", sku_code=sku, sku_name=sku,
        stock_qty=stock, evidence_file_id=f"{code}-{sku}.jpg" if photo else None,
        evidence_file_url="u" if photo else None, evidence_file_name="n.jpg" if photo else None,
        evidence_uploaded_at=T1 if photo else None, created_at=T1, updated_at=T1,
    )


def test_admin_login_lock_and_disabled(backend, monkeypatch):
    import admin_view
    monkeypatch.setattr(admin_view.time, "sleep", lambda s: None)
    at = fresh()
    assert at.text_input(key="adm_pass").proto.type == 1
    assert not metrics(at)
    at = admin_login(at, password="salah")
    assert any("salah" in e.value for e in at.error) and not metrics(at)
    for _ in range(4):
        at = admin_login(at, password="salah")
    at = admin_login(at)
    assert any("Terlalu banyak percobaan" in e.value for e in at.error) and not metrics(at)

    backend.config.admin_username = ""
    backend.config.admin_password = ""
    at = fresh()
    assert any("belum aktif" in i.value for i in at.info)


def test_admin_completeness_photo_rules_and_range(backend):
    t = today()
    for sku in ("K1", "K2"):
        put_row(backend, t - timedelta(days=6), "D1", sku, stock=0, photo=False)
        put_row(backend, t - timedelta(days=7), "D2", sku, stock=1, photo=True)
    at = admin_login(fresh())
    assert at.date_input(key="adm_range").value == (t - timedelta(days=6), t)
    assert metrics(at)["Lengkap"] == "1" and metrics(at)["Belum submit"] == "3"
    at.date_input(key="adm_range").set_value((t - timedelta(days=2), t)).run()
    assert metrics(at)["Belum submit"] == "4"
    at.date_input(key="adm_range").set_value((t - timedelta(days=6), t - timedelta(days=6))).run()
    assert metrics(at)["Lengkap"] == "1"

    import streamlit as st
    st.cache_data.clear()
    backend.bq_store.rows.clear()
    for sku in ("K1", "K2"):
        put_row(backend, t, "D1", sku, stock=3, photo=False)
    at = admin_login(fresh())
    assert metrics(at)["Belum lengkap"] == "1" and metrics(at)["Lengkap"] == "0"

    early, late = t - timedelta(days=4), t
    backend.bq_store.rows.clear()
    for sku in ("K1", "K2"):
        put_row(backend, early, "D1", sku, stock=1, photo=(sku == "K1"))
    put_row(backend, late, "D1", "K1", stock=1, photo=False)
    st.cache_data.clear()
    at = admin_login(fresh())
    assert metrics(at)["Lengkap"] == "1" and metrics(at)["Belum lengkap"] == "0"
    [s for s in at.selectbox if s.label == "Detail distributor"][0].set_value("D1 - Gudang Satu").run()
    visit = [s for s in at.selectbox if s.label == "Tanggal kunjungan"][0]
    assert visit.value == early


def test_admin_comparison_pagination_and_range_guards(backend, monkeypatch):
    import admin_view
    t = today()
    old = t - timedelta(days=30)
    put_row(backend, t, "D1", "K1", stock=15, photo=True, name="Gudang Satu")
    put_row(backend, t, "D1", "K2", stock=4, photo=True, name="Gudang Satu")
    put_row(backend, old, "D1", "K1", stock=10, photo=True, name="Gudang Satu")
    put_row(backend, old, "D1", "K2", stock=0, photo=False, name="Gudang Satu")
    for code, name in (("D2", "Gudang Dua"), ("D3", "Gudang Tiga")):
        for sku in ("K1", "K2"):
            put_row(backend, t, code, sku, stock=1, photo=True, name=name)
    at = admin_login(fresh())
    [s for s in at.selectbox if s.label == "Detail distributor"][0].set_value("D1 - Gudang Satu").run()
    frames = [d.value for d in at.dataframe if "Perubahan" in list(d.value.columns)]
    assert "50.0%" in set(frames[-1]["Perubahan"]) and "baru" in set(frames[-1]["Perubahan"])
    assert "⚠️" in set(frames[-1]["Tanda"])

    monkeypatch.setattr(admin_view, "PAGE_SIZE", 2)
    at = admin_login(fresh())
    files = lambda app: [i.proto.imgs[0].url.rsplit("/", 1)[-1] for i in app.get("image")]
    assert files(at) == ["D2-K1.jpg", "D2-K2.jpg"]
    assert at.button(key="adm_prev_top").disabled and at.button(key="adm_prev_bottom").disabled
    assert not at.button(key="adm_next_top").disabled
    assert any("6 foto · menampilkan 1–2" in c.value for c in at.caption)
    at.button(key="adm_next_bottom").click().run()
    assert files(at) == ["D1-K1.jpg", "D1-K2.jpg"]
    at.selectbox(key="adm_page").set_value(3).run()
    assert files(at) == ["D3-K1.jpg", "D3-K2.jpg"]
    assert at.button(key="adm_next_top").disabled and at.button(key="adm_next_bottom").disabled
    seen = []
    at = admin_login(fresh())
    seen += files(at)
    for _ in range(2):
        at.button(key="adm_next_top").click().run()
        seen += files(at)
    assert sorted(seen) == sorted(f"{c}-{k}.jpg" for c in ("D1", "D2", "D3") for k in ("K1", "K2"))
    at.button(key="adm_prev_top").click().run()
    assert at.selectbox(key="adm_page").value == 2
    at.selectbox(key="adm_region").set_value("R1").run()
    assert at.selectbox(key="adm_page").value == 1

    monkeypatch.setattr(admin_view, "PAGE_SIZE", 8)
    at = admin_login(fresh())
    assert len(files(at)) == 6
    assert not [b for b in at.button if b.key and str(b.key).startswith("adm_prev_")]

    at.date_input(key="adm_range").set_value((t - timedelta(days=3),)).run()
    assert any("Pilih tanggal mulai dan tanggal akhir" in i.value for i in at.info)
    at.date_input(key="adm_range").set_value((t - timedelta(days=200), t)).run()
    assert any("maksimal 92 hari" in w.value for w in at.warning) and not metrics(at)
