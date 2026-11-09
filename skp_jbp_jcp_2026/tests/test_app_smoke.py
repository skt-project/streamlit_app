"""Headless smoke tests of the real Streamlit script in mock mode (no Google calls)."""
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.sheets import LocalLogStore
from src.storage import LocalStorage
from tests.conftest import pdf_bytes

APP = str(Path(__file__).resolve().parents[1] / "app.py")


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.chdir(Path(APP).parent)
    at = AppTest.from_file(APP, default_timeout=60)
    at.secrets["APP_MODE"] = "mock"
    at.secrets["LOCAL_DATA_DIR"] = str(tmp_path)
    at.secrets["ADMIN_USERNAME"] = "admin"
    at.secrets["ADMIN_PASSWORD"] = "uji-123"
    return at


def _texts(at):
    return " ".join(str(m.value) for m in [*at.markdown, *at.caption, *at.title, *at.info, *at.warning,
                                           *at.success, *at.error])


def _button(at, label):
    return next(b for b in at.button if b.label.startswith(label))


def test_landing_page_renders(app):
    app.run()
    assert not app.exception
    text = _texts(app)
    assert "SKP JBP / JCP" in text and "Periode 2026" in text
    assert "Upload SKP JBP atau JCP" in text and "Admin dan Dokumentasi" in text
    assert {"Mulai upload →", "Masuk admin →"} <= {b.label for b in app.button}


def _open_upload(app, region=None):
    app.run()
    _button(app, "Mulai upload").click().run()
    assert not app.exception
    if region is not None:
        app.selectbox[0].select(region).run()
        assert not app.exception
    return app


def test_region_must_be_chosen_before_stores_are_listed(app):
    _open_upload(app)
    assert "Pilih region untuk menampilkan daftar toko" in _texts(app)
    assert len(app.selectbox) == 1 and not app.text_input  # only the region dropdown
    regions = app.selectbox[0].options
    assert any("East Java (2 toko)" in o for o in regions)
    assert any("Region belum diisi (1 toko)" in o for o in regions)  # stores without region stay reachable


def test_region_lists_only_its_stores_without_typing(app):
    _open_upload(app, "East Java")
    store_box = app.selectbox[1]
    assert sorted(store_box.options) == ["DEMO GROUP — Contoh Beauty Group", "DEMO001 — TOKO CONTOH CANTIK"]
    assert store_box.value is None
    store_box.select("DEMO GROUP").run()
    assert not app.exception and "Contoh Beauty Group" in _texts(app)


def test_upload_search_by_code_autodetects_programs(app):
    _open_upload(app, "East Java")
    assert "Mode lokal" in _texts(app)
    app.text_input[0].set_value("demo001").run()
    assert not app.exception
    assert app.text_input[1].disabled  # nama terkunci saat kode diisi
    assert app.selectbox[1].value == "DEMO001"  # satu hasil -> otomatis terpilih
    text = _texts(app)
    assert "TOKO CONTOH CANTIK" in text and "9951/SKT/SKP/P1B/GT/XI/2026" in text
    checks = {c.label: c.value for c in app.checkbox}
    assert checks["SKINTIFIC ✓"] and checks["FACERINNA ✓"] and not checks["TIMEPHORIA"]
    assert checks["JBP ✓ terdeteksi"]
    # Langkah 3 & 4 muncul, submit terkunci karena belum ada file/PIC
    assert "Upload dokumen PDF" in text and "Periksa rangkuman" in text
    submit = _button(app, "Submit dan tampilkan konfirmasi")
    assert submit.disabled


def test_upload_search_by_name_locks_code_field(app):
    _open_upload(app, "East Java")
    app.text_input[1].set_value("contoh").run()
    assert app.text_input[0].disabled
    assert len(app.selectbox[1].options) == 2 and app.selectbox[1].value is None  # >1 hasil -> user memilih
    app.selectbox[1].select_index(0).run()
    assert not app.exception


def test_search_is_limited_to_the_chosen_region(app):
    _open_upload(app, "Bali Nusa Tenggara")
    app.text_input[0].set_value("DEMO001").run()  # exists, but in East Java
    assert "Tidak ada toko di region Bali Nusa Tenggara" in _texts(app)


def test_store_without_region_can_be_selected(app):
    _open_upload(app, "")
    assert app.selectbox[1].value == "DEMO004"  # only store -> auto-selected
    assert "TANPA REGION" in _texts(app)


def test_admin_login_rejects_wrong_password_and_locks_out(app):
    app.run()
    _button(app, "Masuk admin").click().run()
    for _ in range(5):
        app.text_input[0].set_value("admin")
        app.text_input[1].set_value("salah")
        app.button[-1].click().run()
    assert "Terlalu banyak percobaan" in _texts(app) or "salah" in _texts(app)
    app.run()
    assert "Terlalu banyak percobaan" in _texts(app)


def test_admin_dashboard_with_seeded_uploads(app, tmp_path):
    # Seed one upload through the same local backends the app uses.
    from src.service import plan_document, submit_documents
    from src.sheets import Store

    store = Store("DEMO001", "TOKO CONTOH CANTIK", "East Java")
    doc = plan_document(store, "SKINTIFIC", "JBP", "2026 Q4", "9951/SKT/SKP/P1B/GT/XI/2026",
                        "SKP - TOKO CONTOH CANTIK - 9951_SKT_SKP_P1B_GT_XI_2026.pdf", pdf_bytes())
    submit_documents(store, "Budi", [doc], LocalStorage(tmp_path / "gcs"),
                     LocalLogStore(tmp_path / "SKP_UPLOAD_LOG.csv"))

    app.run()
    _button(app, "Masuk admin").click().run()
    app.text_input[0].set_value("admin")
    app.text_input[1].set_value("uji-123")
    app.button[-1].click().run()
    assert not app.exception
    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Total dokumen"] == "1" and metrics["Total toko"] == "1"
    text = _texts(app)
    assert "Kelengkapan dokumen" in text
    assert any("TOKO CONTOH CANTIK" in e.label for e in app.expander)
    _button(app, "📦 Siapkan ZIP").click().run()
    assert not app.exception
    assert "ZIP siap: 1 dokumen" in _texts(app)
