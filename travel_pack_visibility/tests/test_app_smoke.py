"""Smoke test UI dengan Streamlit AppTest + Google palsu (tanpa jaringan)."""
from pathlib import Path
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest

import clients
from config import load_config
from conftest import FakeGC, FakeSpreadsheet, FakeStorageClient, FakeWorksheet, make_jpeg
from evidence import GcsEvidence
from responses import OUTPUT_HEADERS, ResponseSheet, row_values
from service import Submitter

APP = Path(__file__).resolve().parent.parent / "app.py"
JPEG = ("display.jpg", make_jpeg(), "image/jpeg")


@pytest.fixture
def be(monkeypatch, store_values):
    import streamlit as st
    st.cache_resource.clear()
    st.cache_data.clear()
    cfg = load_config({"app": {"store_sheet_id": "stores", "output_sheet_id": "out", "evidence_bucket": "bkt"}})
    out = FakeSpreadsheet()
    gc = FakeGC({"stores": FakeSpreadsheet([FakeWorksheet("Data", store_values)]), "out": out})
    storage = FakeStorageClient()
    responses = ResponseSheet(lambda: gc.open_by_key("out"), cfg.output_worksheet)
    backend = SimpleNamespace(config=cfg, gc=gc, responses=responses,
                              submitter=Submitter(GcsEvidence(storage, "bkt", cfg.evidence_prefix), responses))
    backend.out, backend.bucket = out, storage.bucket("bkt")
    monkeypatch.setattr(clients, "build_backends", lambda secrets: backend)
    return backend


def run(query=None):
    at = AppTest.from_file(str(APP), default_timeout=30)
    at.secrets["app"] = {}                    # backend palsu dipasang lewat monkeypatch
    if query:
        at.query_params.update(query)
    return at.run()


def store_box(at):
    return next(sb for sb in at.selectbox if (sb.key or "").startswith("store_"))


def pick(at, asm="Asep Contoh", store=None):
    at.selectbox(key="asm").set_value(asm).run()
    if store:
        store_box(at).set_value(store).run()
    return at


def texts(at):
    return " ".join(str(e.value) for coll in (at.info, at.warning, at.error, at.success, at.caption) for e in coll)


def test_first_screen_lists_asms_only(be):
    at = run()
    assert not at.exception
    assert at.title[0].value == "Form Upload Visibility Travel Pack BCD Q4'26"
    assert at.selectbox(key="asm").options == ["Asep Contoh", "Budi Contoh", "Cici Contoh"]
    assert "Ach SKU (MOQ) ≥ 4" in texts(at)


def test_only_eligible_stores_listed_and_spv_prefilled(be):
    at = pick(run(), store="ST001")
    sb = store_box(at)
    assert sorted(sb.options) == ["ST001 · Toko Satu", "ST003 · Toko Tiga", "ST004 · Toko Empat"]
    assert at.text_input[0].value == "Sari Spv"
    assert "0 dari 3 toko sudah upload" in str(at.get("progress")[0].proto.text)


def test_asm_without_eligible_store(be):
    at = pick(run(), asm="Cici Contoh")
    assert "Belum ada toko Cici Contoh" in texts(at)


def test_query_param_preselects_asm(be):
    at = run({"asm": "budi contoh"})
    assert at.selectbox(key="asm").value == "Budi Contoh"
    assert store_box(at).options == ["ST010 · Toko Sepuluh"]


def test_submit_without_photo_is_rejected(be):
    at = pick(run(), store="ST001")
    _submit_button(at).click().run()
    assert "wajib" in texts(at)
    assert be.out.sheets == {} and be.bucket.objects == {}


def _submit_button(at):
    return next(b for b in at.button if b.label == "Submit")


def test_full_submit_flow(be):
    at = pick(run(), store="ST003")
    at.text_input[0].set_value("Spv Baru").run()
    at.file_uploader[0].set_value(JPEG).run()
    assert not at.exception and not at.error
    _submit_button(at).click().run()
    assert not at.exception
    rows = be.out.sheets["TRAVEL_PACK_UPLOAD_LOG"].values
    assert rows[0] == OUTPUT_HEADERS and len(rows) == 2
    row = dict(zip(OUTPUT_HEADERS, rows[1]))
    assert (row["Nama ASM"], row["KODE ST TOKO"], row["NAMA TOKO"], row["NAMA SPV"]) == \
        ("Asep Contoh", "ST003", "Toko Tiga", "Spv Baru")
    assert row["Status"] == "Uploaded" and row["GCS Path"].startswith('=HYPERLINK("https://storage.cloud.google.com/')
    assert len(be.bucket.objects) == 1
    assert any("Terkirim" in s.value for s in at.success)
    # pilihan toko + uploader direset, toko sekarang bertanda sudah upload dan pindah ke bawah
    assert store_box(at).value is None and len(at.file_uploader) == 0
    assert "1 dari 3 toko sudah upload" in str(at.get("progress")[0].proto.text)
    assert store_box(at).options[-1] == "✅ ST003 · Toko Tiga"
    # pilih lagi toko yang sama: info sudah upload, SPV kembali ke nilai daftar toko
    store_box(at).set_value("ST003").run()
    assert "sudah upload 1x" in texts(at)


def test_already_uploaded_store_shows_info(be):
    be.out.sheets["TRAVEL_PACK_UPLOAD_LOG"] = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [
        OUTPUT_HEADERS, row_values({"Timestamp": "2026-11-18 09:00:00", "KODE ST TOKO": "ST001"})])
    at = pick(run(), store="ST001")
    assert "sudah upload 1x (terakhir 2026-11-18 09:00:00 WIB)" in texts(at)
    # toko yang belum upload diurutkan lebih dulu
    assert store_box(at).options[-1] == "✅ ST001 · Toko Satu"


def test_invalid_photo_shows_error_and_submit_stores_nothing(be):
    at = pick(run(), store="ST001")
    at.file_uploader[0].set_value(("x.jpg", b"not an image", "image/jpeg")).run()
    assert "bukan foto" in texts(at)
    _submit_button(at).click().run()
    assert be.out.sheets == {} and be.bucket.objects == {}


def test_store_sheet_unreadable_shows_error(be):
    be.gc.books.pop("stores")
    at = run()
    assert not at.exception
    assert "di-share" in texts(at)


def test_bad_output_header_blocks_write_with_message(be):
    bad = list(OUTPUT_HEADERS)
    bad[1] = "ASM"
    be.out.sheets["TRAVEL_PACK_UPLOAD_LOG"] = FakeWorksheet("TRAVEL_PACK_UPLOAD_LOG", [bad])
    at = pick(run(), store="ST001")
    assert "Status upload belum bisa dibaca" in texts(at)
    at.file_uploader[0].set_value(JPEG).run()
    _submit_button(at).click().run()
    assert "Foto belum dikirim" in texts(at)          # berhenti SEBELUM foto masuk bucket
    assert len(be.out.sheets["TRAVEL_PACK_UPLOAD_LOG"].values) == 1 and be.bucket.objects == {}
