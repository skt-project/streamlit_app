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


def test_upload_list_is_fixed_by_the_skp_numbers_in_the_sheet(app):
    _open_upload(app, "East Java")
    assert "Mode lokal" in _texts(app)
    app.text_input[0].set_value("demo001").run()
    assert not app.exception
    assert app.text_input[1].disabled  # nama terkunci saat kode diisi
    assert app.selectbox[1].value == "DEMO001"  # satu hasil -> otomatis terpilih
    text = _texts(app)
    assert "TOKO CONTOH CANTIK" in text and "9951/SKT/SKP/P1B/GT/XI/2026" in text
    # No brand/program/quarter picking any more: no checkboxes, no pills, no old step title
    assert not app.checkbox and "Pilih brand, program, dan kuartal" not in text
    # Exactly the 4 SKP numbers of DEMO001 get an upload area, in brand -> program -> quarter order
    headers = [m.value for m in app.markdown if str(m.value).startswith("**") and " · 2026 Q" in str(m.value)]
    assert headers == ["**SKINTIFIC · JBP · 2026 Q3**", "**SKINTIFIC · JBP · 2026 Q4**",
                       "**SKINTIFIC · JCP · 2026 Q4**", "**FACERINNA · JBP · 2026 Q3**"]
    assert "4 dokumen SKP untuk toko ini" in text
    assert "Upload dokumen PDF" in text and "Periksa rangkuman" in text
    assert _button(app, "Submit dan tampilkan konfirmasi").disabled  # belum ada file/PIC


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


def test_store_without_any_skp_number_cannot_upload(app):
    _open_upload(app, "")  # DEMO004: no SKP numbers in the sheet
    text = _texts(app)
    assert "Belum ada nomor SKP untuk toko ini" in text
    assert "Upload dokumen PDF" not in text and not app.text_input[2:]  # no PIC / upload step


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


def test_warm_up_popup_runs_once_per_session(app):
    import time

    t0 = time.monotonic()
    app.run()
    first = time.monotonic() - t0
    assert app.session_state["booted"] and not app.exception
    t0 = time.monotonic()
    app.run()
    assert time.monotonic() - t0 < first  # no second popup on the next interaction


def _seed_upload(tmp_path, quarter="2026 Q4", skp="9951/SKT/SKP/P1B/GT/XI/2026"):
    from src.service import plan_document, submit_documents
    from src.sheets import Store

    store = Store("DEMO001", "TOKO CONTOH CANTIK", "East Java")
    doc = plan_document(store, "SKINTIFIC", "JBP", quarter, skp,
                        f"SKP - TOKO CONTOH CANTIK - {skp.replace('/', '_')}.pdf", pdf_bytes())
    submit_documents(store, "Budi", [doc], LocalStorage(tmp_path / "gcs"),
                     LocalLogStore(tmp_path / "SKP_UPLOAD_LOG.csv"), timestamp="2026-11-10 09:00:00")


def _open_demo001(app):
    _open_upload(app, "East Java")
    app.text_input[0].set_value("demo001").run()
    assert not app.exception


def test_uploaded_skp_shows_file_link_and_edit_instead_of_uploader(app, tmp_path):
    _seed_upload(tmp_path)
    _open_demo001(app)
    text = _texts(app)
    assert "1 sudah diupload, 3 belum" in text and "opsional" in text
    assert "9951/SKT/SKP/P1B/GT/XI/2026 ✅" in text  # marked in the store's SKP table
    assert "Diupload 2026-11-10 09:00:00 oleh Budi" in text
    assert len(app.get("download_button")) == 1  # mock storage: view/download existing file
    assert [t.label for t in app.toggle] == ["✏️ Edit / upload ulang file ini"] and not app.toggle[0].value
    assert len(app.get("file_uploader")) == 3  # uploaded SKP has no uploader until Edit is on
    summary = app.dataframe[-1].value
    actions = dict(zip(summary["Kuartal"] + " " + summary["Program"] + " " + summary["Brand"], summary["Aksi"]))
    assert actions["2026 Q4 JBP SKINTIFIC"] == "✅ Tetap (tidak diubah)"
    assert list(summary["Aksi"]).count("⏭️ Dilewati") == 3
    assert "Belum ada file baru untuk dikirim" in text  # nothing new chosen -> submit blocked


def test_edit_toggle_reveals_replacement_uploader_without_dropping_the_file(app, tmp_path):
    _seed_upload(tmp_path)
    _open_demo001(app)
    app.toggle[0].set_value(True).run()
    assert not app.exception
    assert len(app.get("file_uploader")) == 4
    assert "Pilih PDF pengganti" in _texts(app)
    summary = app.dataframe[-1].value
    assert "✅ Tetap (tidak diubah)" in list(summary["Aksi"])  # edit on but no new file -> still kept


def test_all_uploaded_store_says_everything_is_saved(app, tmp_path):
    from src.service import plan_document, submit_documents
    from src.sheets import Store

    store = Store("DEMO003", "Toko Contoh Sejahtera", "Bali Nusa Tenggara")
    doc = plan_document(store, "FACERINNA", "JCP", "2026 Q4", "9045/FCR/SKP/P2/GT/X/2026",
                        "SKP - Toko Contoh Sejahtera - 9045_FCR_SKP_P2_GT_X_2026.pdf", pdf_bytes())
    submit_documents(store, "Budi", [doc], LocalStorage(tmp_path / "gcs"), LocalLogStore(tmp_path / "SKP_UPLOAD_LOG.csv"))
    _open_upload(app, "Bali Nusa Tenggara")
    assert app.selectbox[1].value == "DEMO003"
    assert "Semua dokumen SKP toko ini sudah tersimpan" in _texts(app)
    assert _button(app, "Submit dan tampilkan konfirmasi").disabled
