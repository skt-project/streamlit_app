"""End-to-end tests of po_buffer.py through Streamlit's AppTest.

The real app script runs (real filters, real reportlab PDFs, real Streamlit
callbacks) against an in-memory fake of BigQuery (tests/po_buffer_fakes.py).
That is what lets these tests prove the parts a unit test cannot:

* a download is counted when the *button is clicked* and never when a PDF is
  merely generated or the script reruns;
* the admin dashboard is unreachable without login;
* the ZIP of a filter holds exactly one correct PDF per store.

Needs a working ``streamlit`` import and reportlab/pypdf; skipped otherwise.

Run: pytest tests/test_po_buffer_app.py -q
"""
from __future__ import annotations

import contextlib
import io
import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

REPO = Path(__file__).resolve().parents[1]
for extra in (REPO, REPO / "tests"):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

try:
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import pypdf
except Exception as exc:  # pragma: no cover - environment dependent
    pytest.skip(f"streamlit AppTest unavailable: {exc}", allow_module_level=True)

import po_buffer_downloads as D  # noqa: E402
import po_buffer_tracking as T  # noqa: E402
from po_buffer_fakes import FakePoBufferBigQuery  # noqa: E402

APP = str(REPO / "po_buffer.py")
PAGE_ADMIN = "🔐 Admin Tracking"
PAGE_PO = "📦 PO Suggestion"


class App:
    """Thin driver around AppTest."""

    def __init__(self, at: AppTest, fake: FakePoBufferBigQuery):
        self.at = at
        self.fake = fake

    # -- navigation / filters
    def run(self):
        self.at.run(timeout=90)
        assert not self.at.exception, [e.value for e in self.at.exception]
        return self

    def open_admin(self):
        self.at.radio(key="page_view").set_value(PAGE_ADMIN)
        return self.run()

    def login(self, username, password):
        self.at.text_input[0].set_value(username)
        self.at.text_input[1].set_value(password)
        next(b for b in self.at.button if b.label == "Login").click()
        return self.run()

    def select_region(self, region):
        self.at.selectbox(key="region_select").select(region)
        return self.run()

    def select_distributor(self, distributor):
        self.at.selectbox(key="distributor_select").select(distributor)
        return self.run()

    def select_stores(self, *codes):
        self.at.multiselect(key="store_select").set_value(list(codes))
        return self.run()

    # -- clicks
    def click_button(self, key):
        self.at.button(key=key).click()
        return self.run()

    def click_download(self, key):
        """Click an st.download_button.

        AppTest has no element for download buttons, so send the same widget
        state a browser sends on click: ``trigger_value=True`` for its id.
        Streamlit then runs the button's on_click callback and reruns.
        """
        matches = [e for e in self.at.get("download_button") if e.proto.id.endswith(f"-{key}")]
        assert len(matches) == 1, f"expected exactly one download button {key!r}, got {len(matches)}"
        widget_id = matches[0].proto.id
        tree = self.at._tree
        original = tree.get_widget_states

        def with_click():
            states = original()
            state = states.widgets.add()
            state.id = widget_id
            state.trigger_value = True
            return states

        tree.get_widget_states = with_click
        return self.run()

    def has_download(self, key):
        return any(e.proto.id.endswith(f"-{key}") for e in self.at.get("download_button"))

    # -- inspection
    @property
    def bulk_result(self):
        prepared = self.at.session_state["po_bulk_prepared"]
        return prepared["result"] if prepared else None

    def zip_entries(self):
        with zipfile.ZipFile(io.BytesIO(self.bulk_result.zip_bytes)) as archive:
            return {i.filename: archive.read(i) for i in archive.infolist()}

    def tracked(self):
        """Rows stored in the (fake) tracking table, as per-store counters."""
        counts = {}
        for row in self.fake.rows:
            counts[row["store_code"]] = counts.get(row["store_code"], 0) + 1
        return counts

    def text(self):
        parts = [m.value for m in self.at.markdown]
        parts += [i.value for i in self.at.info] + [w.value for w in self.at.warning]
        parts += [e.value for e in self.at.error] + [s.value for s in self.at.success]
        parts += [c.value for c in self.at.caption]
        return "\n".join(parts)


@contextlib.contextmanager
def launch(fake, spool_path):
    """Run po_buffer.py against ``fake`` and yield a driver (caches reset around it)."""
    os.environ[T.FALLBACK_PATH_ENV] = str(spool_path)
    st.cache_resource.clear()
    st.cache_data.clear()
    try:
        with mock.patch.object(bigquery, "Client", lambda **kwargs: fake), mock.patch.object(
            service_account.Credentials, "from_service_account_info", lambda *a, **k: object()
        ):
            at = AppTest.from_file(APP, default_timeout=90)
            at.secrets["connections"] = {"bigquery": {"type": "service_account", "project_id": "test"}}
            yield App(at, fake).run()
    finally:
        os.environ.pop(T.FALLBACK_PATH_ENV, None)
        st.cache_resource.clear()
        st.cache_data.clear()


@pytest.fixture
def app(tmp_path):
    with launch(FakePoBufferBigQuery(), tmp_path / "spool.jsonl") as driver:
        yield driver


def today():
    return D.jakarta_date_str()


def pdf_text(data: bytes) -> str:
    reader = pypdf.PdfReader(io.BytesIO(data))
    return "\n".join(page.extract_text() for page in reader.pages)


# ---------------------------------------------------------------------------
# Bulk download: one ZIP, one PDF per store
# ---------------------------------------------------------------------------

def test_app_starts_and_lists_every_store_with_the_bulk_section(app):
    assert "Store List (6 stores)" in app.text()
    assert app.at.button(key="bulk_zip_btn") is not None
    assert sum(1 for e in app.at.get("download_button")) == 6


def test_before_downloading_the_user_is_told_how_many_stores_and_files(app):
    app.select_region("JAWA BARAT")
    assert "5 toko" in app.text() and "5 file PDF" in app.text()
    assert "1 file ZIP" in app.text()


def test_bulk_of_a_region_builds_a_zip_with_exactly_one_pdf_per_store(app):
    app.select_region("JAWA BARAT")                      # ST001-ST005
    app.click_button("bulk_zip_btn")

    result = app.bulk_result
    entries = app.zip_entries()
    assert result.file_count == 5 and len(entries) == 5
    assert all(name.endswith(".pdf") for name in entries)
    assert result.zip_name == f"PO_Bulk_5_{today()}.zip"
    assert sorted(entries) == sorted([
        f"Toko Maju_{today()}.pdf",
        f"Toko Sama_ST002_{today()}.pdf",     # duplicate name -> store_code added
        f"Toko Sama_ST003_{today()}.pdf",
        f"Toko Baru-Pusat_{today()}.pdf",     # "/" sanitized
        f"Toko Lima_{today()}.pdf",
    ])


def test_every_pdf_in_the_zip_contains_its_own_store_and_no_other(app):
    app.select_region("JAWA BARAT")
    app.click_button("bulk_zip_btn")
    entries = app.zip_entries()
    by_code = {
        "ST001": f"Toko Maju_{today()}.pdf",
        "ST002": f"Toko Sama_ST002_{today()}.pdf",
        "ST003": f"Toko Sama_ST003_{today()}.pdf",
        "ST004": f"Toko Baru-Pusat_{today()}.pdf",
        "ST005": f"Toko Lima_{today()}.pdf",
    }
    assert set(by_code.values()) == set(entries)
    for code, filename in by_code.items():
        assert entries[filename].startswith(b"%PDF")
        text = pdf_text(entries[filename])
        assert f"Kode: {code}" in text
        assert f"Produk {code} nomor 1" in text
        others = [c for c in by_code if c != code]
        assert not any(f"Kode: {o}" in text or f"Produk {o} " in text for o in others)


def test_the_store_list_still_renders_when_hundreds_of_stores_have_no_store_code(tmp_path):
    """Regression (2026-11-23): 401 staging stores with store_code NULL crashed the multiselect
    ("unhashable type: 'Series'") and the page stopped before the store list."""
    import pandas as pd
    from po_buffer_fakes import build_inventory_fixture

    inventory = build_inventory_fixture()
    unmapped = inventory.iloc[:1].copy()
    unmapped = pd.concat([unmapped] * 400, ignore_index=True)
    unmapped["store_code"] = None
    unmapped["store_name"] = [f"STAGING STORE {n}" for n in range(400)]
    unmapped["distributor_g2g"] = "DIST A"
    with launch(FakePoBufferBigQuery(detail_inventory=pd.concat([inventory, unmapped], ignore_index=True)),
                tmp_path / "spool.jsonl") as driver:
        assert "Store List (6 stores)" in driver.text()               # the page got past the filters
        options = driver.at.multiselect(key="store_select").options
        assert len(options) == 6 and not any("STAGING STORE" in o for o in options)


# ---------------------------------------------------------------------------
# Paging: only one page of stores builds PDFs; the bulk ZIP still covers everyone
# ---------------------------------------------------------------------------

PER_PAGE = 25


def visible_individual_codes(driver):
    marker = "-dl_pdf_"
    return sorted(e.proto.id.split(marker)[1] for e in driver.at.get("download_button") if marker in e.proto.id)


@pytest.fixture
def big_app(tmp_path):
    from po_buffer_fakes import build_inventory_fixture

    with launch(FakePoBufferBigQuery(inventory=build_inventory_fixture(extra_stores=54)),
                tmp_path / "spool.jsonl") as driver:
        yield driver                                           # 60 stores in total


def test_a_short_list_has_no_pager(app):
    assert len(app.at.number_input) == 0
    assert len(visible_individual_codes(app)) == 6


def test_a_long_list_builds_and_shows_only_one_page_of_pdfs(big_app):
    assert "Store List (60 stores)" in big_app.text()
    assert len(visible_individual_codes(big_app)) == PER_PAGE
    assert "Menampilkan toko 1-25 dari 60" in big_app.text()
    assert big_app.at.number_input(key="store_page").value == 1


def test_the_pager_walks_through_every_store_exactly_once(big_app):
    seen = []
    for page in (1, 2, 3):
        big_app.at.number_input(key="store_page").set_value(page)
        big_app.run()
        codes = visible_individual_codes(big_app)
        assert len(codes) == (PER_PAGE if page < 3 else 10)
        seen += codes
    assert len(seen) == 60 and len(set(seen)) == 60


def test_the_bulk_zip_covers_all_stores_not_just_the_visible_page(big_app):
    big_app.select_region("JAWA BARAT")                        # 59 stores, 25 shown
    assert len(visible_individual_codes(big_app)) == PER_PAGE
    assert "59 toko" in big_app.text() and "59 file PDF" in big_app.text()
    big_app.click_button("bulk_zip_btn")
    assert big_app.bulk_result.file_count == 59 and len(big_app.zip_entries()) == 59


def test_changing_the_filter_sends_the_pager_back_to_page_one(big_app):
    big_app.at.number_input(key="store_page").set_value(3)
    big_app.run()
    assert big_app.at.number_input(key="store_page").value == 3
    big_app.select_region("SUMATERA")                          # 1 store -> no pager at all
    assert len(big_app.at.number_input) == 0
    assert visible_individual_codes(big_app) == ["ST006"]
    big_app.select_region("All")
    assert big_app.at.number_input(key="store_page").value == 1


def test_a_store_on_page_two_downloads_and_is_tracked(big_app):
    big_app.at.number_input(key="store_page").set_value(2)
    big_app.run()
    code = visible_individual_codes(big_app)[0]
    big_app.click_download(f"dl_pdf_{code}")
    assert big_app.tracked() == {code: 1}


def test_the_pdf_of_a_store_is_built_once_per_data_version():
    """generate_store_pdf is cached: reruns and the bulk ZIP do not rebuild identical PDFs."""
    import logging

    logging.disable(logging.CRITICAL)
    try:
        import po_buffer
    finally:
        logging.disable(logging.NOTSET)
    from po_buffer_fakes import build_inventory_fixture

    calls = []
    real = po_buffer.generate_po_pdf

    def counting(info, detail, brand_name=po_buffer.Config.BRAND_NAME):
        calls.append(info["store_code"])
        return real(info, detail, brand_name)

    inventory = build_inventory_fixture()
    info = lambda code: D.build_store_info({"store_code": code, "store_name": code, "region": "R", "distributor": "D"})
    detail = lambda code: inventory[inventory["store_code"] == code]
    po_buffer.generate_po_pdf = counting
    try:
        po_buffer.generate_store_pdf.clear()
        first = po_buffer.generate_store_pdf(info("ST001"), detail("ST001"))
        again = po_buffer.generate_store_pdf(info("ST001"), detail("ST001"))
        other = po_buffer.generate_store_pdf(info("ST002"), detail("ST002"))
        assert calls == ["ST001", "ST002"] and first == again and first != other
        changed = detail("ST001").assign(buffer_plan=99.0)       # new data -> a new PDF
        po_buffer.generate_store_pdf(info("ST001"), changed)
        assert calls == ["ST001", "ST002", "ST001"]
    finally:
        po_buffer.generate_po_pdf = real
        po_buffer.generate_store_pdf.clear()


def test_filter_with_one_store_gives_a_zip_with_one_pdf(app):
    app.select_stores("ST005")
    app.click_button("bulk_zip_btn")
    assert app.bulk_result.file_count == 1
    assert list(app.zip_entries()) == [f"Toko Lima_{today()}.pdf"]


def test_filter_with_five_stores_gives_a_zip_with_exactly_five_pdfs(app):
    app.select_stores("ST001", "ST002", "ST003", "ST004", "ST006")
    app.click_button("bulk_zip_btn")
    assert len(app.zip_entries()) == 5


def test_distributor_filter_decides_which_stores_are_in_the_zip(app):
    app.select_region("JAWA BARAT")
    app.select_distributor("DIST B")
    app.click_button("bulk_zip_btn")
    assert len(app.zip_entries()) == 2


def test_a_stale_zip_is_not_offered_after_the_filter_changes(app):
    app.select_region("JAWA BARAT")
    app.click_button("bulk_zip_btn")
    assert app.has_download("dl_bulk_zip")
    app.select_distributor("DIST B")                     # different set of stores now
    assert not app.has_download("dl_bulk_zip")


def test_a_store_without_valid_data_is_reported_with_a_reason_not_dropped_silently(tmp_path):
    from po_buffer_fakes import build_inventory_fixture

    # The summary still lists ST003, but its detail rows carry no buffer_plan.
    detail = build_inventory_fixture()
    detail.loc[detail["store_code"] == "ST003", "buffer_plan"] = None
    fake = FakePoBufferBigQuery(detail_inventory=detail)
    with launch(fake, tmp_path / "spool.jsonl") as driver:
        driver.select_region("JAWA BARAT")
        text = driver.text()
        assert "5 toko" in text and "4 file PDF" in text          # told BEFORE downloading
        assert "1 toko tidak akan dibuatkan PDF" in text
        driver.click_button("bulk_zip_btn")
        assert driver.bulk_result.file_count == 4 and len(driver.zip_entries()) == 4
        assert [(i.store_code, i.skip_reason) for i in driver.bulk_result.excluded] == [
            ("ST003", D.SKIP_NO_PLAN)
        ]
        assert "dilewati" in driver.text()                          # and again after generating
        driver.click_download("dl_bulk_zip")
        assert driver.tracked() == {"ST001": 1, "ST002": 1, "ST004": 1, "ST005": 1}   # ST003 not counted


# ---------------------------------------------------------------------------
# Tracking: counted on click only, never twice
# ---------------------------------------------------------------------------

def test_rendering_the_app_and_building_the_zip_records_no_download(app):
    app.select_region("JAWA BARAT")
    app.click_button("bulk_zip_btn")                     # generate: 5 PDFs built
    assert app.fake.rows == []                           # ... and not one counted
    assert app.fake.tracking_queries() == []             # tracking was not even touched


def test_clicking_the_bulk_download_counts_every_store_in_the_zip_once(app):
    app.select_region("JAWA BARAT")
    app.click_button("bulk_zip_btn")
    app.click_download("dl_bulk_zip")

    assert app.tracked() == {"ST001": 1, "ST002": 1, "ST003": 1, "ST004": 1, "ST005": 1}
    rows = app.fake.rows
    assert {r["download_type"] for r in rows} == {"bulk"}
    assert len({r["event_id"] for r in rows}) == 1       # one action, one event id
    assert {r["bulk_file_count"] for r in rows} == {5}
    assert {r["file_name"] for r in rows} == set(app.zip_entries())


def test_reruns_after_a_bulk_click_never_count_it_again(app):
    app.select_region("JAWA BARAT")
    app.click_button("bulk_zip_btn")
    app.click_download("dl_bulk_zip")
    for _ in range(3):                                   # plain Streamlit reruns
        app.run()
    app.select_stores("ST001")                           # a widget interaction = rerun
    app.select_stores()
    assert len(app.fake.rows) == 5
    assert app.tracked() == {"ST001": 1, "ST002": 1, "ST003": 1, "ST004": 1, "ST005": 1}


def test_two_separate_bulk_clicks_are_two_downloads(app):
    app.select_region("JAWA BARAT")
    app.click_button("bulk_zip_btn")
    app.click_download("dl_bulk_zip")
    app.click_download("dl_bulk_zip")
    assert app.tracked() == {"ST001": 2, "ST002": 2, "ST003": 2, "ST004": 2, "ST005": 2}
    assert len({r["event_id"] for r in app.fake.rows}) == 2


def test_individual_download_counts_only_the_selected_store(app):
    app.click_download("dl_pdf_ST004")
    assert app.tracked() == {"ST004": 1}
    (row,) = app.fake.rows
    assert row["download_type"] == "individual" and row["bulk_file_count"] is None
    assert row["store_name"] == "Toko Baru/Pusat" and row["region"] == "JAWA BARAT"
    assert row["file_name"] == f"Toko Baru-Pusat_{today()}.pdf"


def test_individual_filename_matches_the_bulk_filename_including_duplicate_handling(app):
    app.click_download("dl_pdf_ST002")
    assert app.fake.rows[0]["file_name"] == f"Toko Sama_ST002_{today()}.pdf"


def test_individual_and_bulk_downloads_add_up_per_store(app):
    app.select_region("JAWA BARAT")
    app.click_download("dl_pdf_ST001")
    app.click_download("dl_pdf_ST001")
    app.click_button("bulk_zip_btn")
    app.click_download("dl_bulk_zip")
    by_type = {}
    for row in app.fake.rows:
        by_type.setdefault((row["store_code"], row["download_type"]), 0)
        by_type[(row["store_code"], row["download_type"])] += 1
    assert by_type[("ST001", "individual")] == 2
    assert by_type[("ST001", "bulk")] == 1
    assert by_type[("ST002", "bulk")] == 1 and ("ST002", "individual") not in by_type


def test_a_tracking_failure_does_not_break_the_download_or_lose_the_event(app, tmp_path):
    app.fake.fail_inserts = True
    app.fake.table_exists = True
    app.click_download("dl_pdf_ST001")                    # BigQuery is down
    assert not app.at.exception
    assert app.fake.rows == []
    spool = Path(os.environ[T.FALLBACK_PATH_ENV])
    assert spool.exists() and "ST001" in spool.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Admin login + dashboard
# ---------------------------------------------------------------------------

def test_the_tracking_page_is_not_shown_before_login(app):
    app.open_admin()
    text = app.text()
    assert "Login Admin" in text
    assert "Tracking Download PO" not in text and "Download per Toko" not in text
    assert len(app.at.text_input) == 2
    assert not any(b.label == "🚪 Logout" for b in app.at.button)
    assert app.fake.tracking_queries() == []              # no tracking data was even read


def test_a_wrong_password_is_rejected_with_an_error_and_shows_no_dashboard(app):
    app.open_admin().login("admin", "salah-banget")
    assert any("salah" in e.value for e in app.at.error)
    assert "Tracking Download PO" not in app.text()
    assert app.fake.tracking_queries() == []


def test_a_wrong_username_is_rejected(app):
    app.open_admin().login("root", "12345678")
    assert any("salah" in e.value for e in app.at.error)
    assert "Tracking Download PO" not in app.text()


def test_the_dashboard_function_itself_refuses_to_render_without_a_session_login():
    """Defence in depth: even if something called it directly, no tracking data is shown."""
    def script():
        import po_buffer
        po_buffer.render_admin_dashboard()

    st.cache_resource.clear()
    st.cache_data.clear()
    fake = FakePoBufferBigQuery()
    with mock.patch.object(bigquery, "Client", lambda **kwargs: fake), mock.patch.object(
        service_account.Credentials, "from_service_account_info", lambda *a, **k: object()
    ):
        at = AppTest.from_function(script, default_timeout=90)
        at.secrets["connections"] = {"bigquery": {"type": "service_account", "project_id": "test"}}
        at.run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Akses ditolak" in e.value for e in at.error)
    assert not at.metric and not at.dataframe
    assert "Tracking Download PO" not in " ".join(m.value for m in at.markdown)
    assert fake.tracking_queries() == []
    st.cache_resource.clear()
    st.cache_data.clear()


def test_the_default_admin_login_opens_the_dashboard_and_logout_closes_it(app):
    app.open_admin().login("admin", "12345678")
    assert "Tracking Download PO" in app.text()
    assert app.at.session_state["po_admin_authenticated"] is True
    logout = next(b for b in app.at.button if b.label == "🚪 Logout")
    logout.click()
    app.run()
    assert "po_admin_authenticated" not in app.at.session_state
    assert "Tracking Download PO" not in app.text() and "Login Admin" in app.text()


def test_the_default_credential_warning_is_shown_to_the_admin(app):
    app.open_admin().login("admin", "12345678")
    assert any("default" in w.value.lower() for w in app.at.warning)


def test_credentials_from_secrets_replace_the_defaults(app):
    app.at.secrets["po_buffer_admin"] = {"username": "kepala", "password": "rahasia-123"}
    app.open_admin().login("admin", "12345678")
    assert "Tracking Download PO" not in app.text()       # the default no longer works
    app.login("kepala", "rahasia-123")
    assert "Tracking Download PO" in app.text()
    assert not any("default" in w.value.lower() for w in app.at.warning)


def test_dashboard_shows_the_counters_per_store(app):
    app.select_region("JAWA BARAT")
    app.click_download("dl_pdf_ST001")
    app.click_button("bulk_zip_btn")
    app.click_download("dl_bulk_zip")

    app.open_admin().login("admin", "12345678")
    metrics = {m.label: m.value for m in app.at.metric}
    assert metrics["Total Download"] == "6"               # 1 individual + 5 via bulk
    assert metrics["Individual"] == "1" and metrics["Via Bulk (per toko)"] == "5"
    assert metrics["Aksi Bulk"] == "1" and metrics["Toko Diunduh"] == "5"

    table = app.at.dataframe[0].value.set_index("Store Code")
    assert table.loc["ST001", ["Individual", "Bulk", "Total"]].tolist() == [1, 1, 2]
    assert table.loc["ST002", ["Individual", "Bulk", "Total"]].tolist() == [0, 1, 1]
    assert {"Nama Toko", "Region", "Distributor", "Download Terakhir (WIB)"} <= set(table.columns)


def test_dashboard_refresh_picks_up_a_download_made_after_it_opened(app):
    app.open_admin().login("admin", "12345678")
    assert "Belum ada download" in app.text()
    app.at.radio(key="page_view").set_value(PAGE_PO)
    app.run()
    app.click_download("dl_pdf_ST006")
    app.at.radio(key="page_view").set_value(PAGE_ADMIN)
    app.run()
    app.click_button("admin_refresh_btn")
    assert {m.label: m.value for m in app.at.metric}["Total Download"] == "1"


def test_dashboard_filters_by_store_region_and_distributor(app):
    app.click_download("dl_pdf_ST001")                    # JAWA BARAT / DIST A
    app.click_download("dl_pdf_ST006")                    # SUMATERA / DIST C
    app.open_admin().login("admin", "12345678")

    app.at.multiselect(key="admin_region_filter").set_value(["SUMATERA"])
    app.run()
    assert {m.label: m.value for m in app.at.metric}["Total Download"] == "1"
    assert app.at.dataframe[0].value["Store Code"].tolist() == ["ST006"]

    app.at.multiselect(key="admin_region_filter").set_value([])
    app.at.multiselect(key="admin_distributor_filter").set_value(["DIST A"])
    app.run()
    assert app.at.dataframe[0].value["Store Code"].tolist() == ["ST001"]

    app.at.multiselect(key="admin_distributor_filter").set_value([])
    app.at.multiselect(key="admin_store_filter").set_value(["ST006"])
    app.run()
    assert app.at.dataframe[0].value["Store Code"].tolist() == ["ST006"]


def test_dashboard_date_filter_can_exclude_everything(app):
    app.click_download("dl_pdf_ST001")
    app.open_admin().login("admin", "12345678")
    app.at.checkbox(key="admin_all_dates").set_value(False)
    app.run()
    app.at.date_input(key="admin_date_range").set_value(
        (datetime(2020, 1, 1).date(), datetime(2020, 1, 2).date())
    )
    app.run()
    assert "Tidak ada data untuk filter" in app.text()


def test_dashboard_offers_csv_exports_that_do_not_count_as_po_downloads(app):
    app.click_download("dl_pdf_ST001")
    app.open_admin().login("admin", "12345678")
    keys = [e.proto.id for e in app.at.get("download_button")]
    assert any(k.endswith("-admin_export_summary") for k in keys)
    assert any(k.endswith("-admin_export_events") for k in keys)
    app.click_download("admin_export_summary")
    assert app.tracked() == {"ST001": 1}                  # exporting a CSV is not a PO download


def test_dashboard_warns_when_tracking_only_has_the_local_file_fallback(tmp_path):
    fake = FakePoBufferBigQuery(can_create=False, table_exists=False)   # no table, no permission
    with launch(fake, tmp_path / "spool.jsonl") as driver:
        driver.click_download("dl_pdf_ST001")
        driver.open_admin().login("admin", "12345678")
        assert any("hilang" in w.value for w in driver.at.warning)
        assert {m.label: m.value for m in driver.at.metric}["Total Download"] == "1"


# ---------------------------------------------------------------------------
# generate_po_pdf is preserved; PDF dates use Asia/Jakarta
# ---------------------------------------------------------------------------

def test_generate_po_pdf_keeps_its_signature_and_still_renders_a_store():
    import inspect
    import logging

    logging.disable(logging.CRITICAL)
    try:
        import po_buffer
    finally:
        logging.disable(logging.NOTSET)
    from po_buffer_fakes import build_inventory_fixture

    assert list(inspect.signature(po_buffer.generate_po_pdf).parameters) == [
        "store_info", "store_detail", "brand_name"
    ]
    inventory = build_inventory_fixture()
    detail = inventory[inventory["store_code"] == "ST001"]
    info = D.build_store_info(
        {"store_code": "ST001", "store_name": "Toko Maju", "region": "JAWA BARAT", "distributor": "DIST A"}
    )
    buffer = po_buffer.generate_po_pdf(info, detail)
    assert buffer.getvalue().startswith(b"%PDF")
    assert "Toko Maju" in pdf_text(buffer.getvalue())


def test_the_pdf_date_is_the_jakarta_date_not_the_server_date(monkeypatch):
    import logging
    import pytz

    logging.disable(logging.CRITICAL)
    try:
        import po_buffer
    finally:
        logging.disable(logging.NOTSET)
    from po_buffer_fakes import build_inventory_fixture

    # 00:30 on 19 Nov in Jakarta is still 18 Nov (17:30) on a UTC server
    jakarta_midnight = pytz.timezone("Asia/Jakarta").localize(datetime(2026, 11, 19, 0, 30))
    monkeypatch.setattr(po_buffer, "get_jkt_now", lambda: jakarta_midnight)
    inventory = build_inventory_fixture()
    info = D.build_store_info(
        {"store_code": "ST001", "store_name": "Toko Maju", "region": "JAWA BARAT", "distributor": "DIST A"}
    )
    text = pdf_text(po_buffer.generate_po_pdf(info, inventory[inventory["store_code"] == "ST001"]).getvalue())
    assert "19/Nov/2026" in text
