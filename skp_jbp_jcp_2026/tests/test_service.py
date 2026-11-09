"""Submit flow, overwrite, ZIP and config — with local fakes only (no Google calls)."""
import io
import zipfile

import pandas as pd
import pytest

from src.config import parse_service_account, resolve_mode, settings_from_mapping
from src.service import (
    STATUS_NOT_UPLOADED,
    apply_filters,
    compliance_frame,
    count_by,
    dashboard_stats,
    plan_document,
    resolve_path_collisions,
    submit_documents,
)
from src.sheets import LocalLogStore, Store, expected_documents, logs_to_frame
from src.storage import LocalStorage, StorageError, build_zip
from tests.conftest import pdf_bytes

SKP_Q4 = "9784/SKT/SKP/P1B/GT/XI/2026"


@pytest.fixture
def backends(tmp_path):
    return LocalStorage(tmp_path / "gcs"), LocalLogStore(tmp_path / "log.csv")


def _doc(store, brand="SKINTIFIC", program="JBP", quarter="2026 Q4", skp=SKP_Q4,
         name="SKP - TOKO ALFA - 9784_SKT_SKP_P1B_GT_XI_2026.pdf", data=None):
    return plan_document(store, brand, program, quarter, skp, name, data or pdf_bytes())


def test_submit_uploads_file_and_writes_log(store, backends):
    storage, logs = backends
    doc = _doc(store)
    result = submit_documents(store, "Budi", [doc], storage, logs, timestamp="2026-11-09 10:00:00")
    assert result.ok and len(result.saved) == 1
    path = "2026 Q4/jbp/East Java/SKP - TOKO ALFA - 9784_SKT_SKP_P1B_GT_XI_2026.pdf"
    assert doc.object_path == path
    assert storage.download(path) == doc.data
    df = logs_to_frame(logs.read_values())
    row = df.iloc[0]
    assert (row["PIC Submit"], row["Kuartal"], row["Status"], row["GCS Path"]) == ("Budi", "2026 Q4", "Uploaded", path)


def test_reupload_overwrites_file_and_metadata(store, backends):
    storage, logs = backends
    submit_documents(store, "Budi", [_doc(store, data=pdf_bytes(1000))], storage, logs, timestamp="2026-11-09 10:00:00")
    new_bytes = pdf_bytes(3000)
    result = submit_documents(store, "Sari", [_doc(store, data=new_bytes)], storage, logs,
                              timestamp="2026-11-09 11:00:00")
    df = logs_to_frame(logs.read_values())
    assert len(logs.read_values()) == 2  # header + one row, no duplicate
    assert df.iloc[0]["PIC Submit"] == "Sari" and df.iloc[0]["Status"] == "Replaced"
    assert storage.download(result.saved[0]["GCS Path"]) == new_bytes


def test_reupload_with_corrected_skp_removes_superseded_file(store, backends):
    storage, logs = backends
    first = submit_documents(store, "Budi", [_doc(store, skp="9999/SKT/TYPO")], storage, logs)
    old_path = first.saved[0]["GCS Path"]
    second = submit_documents(store, "Budi", [_doc(store)], storage, logs)
    assert second.removed_old == [old_path]
    assert storage.download(old_path) is None
    assert len(logs_to_frame(logs.read_values())) == 1


def test_filename_mismatch_is_flagged_in_status_not_blocked(store, backends):
    storage, logs = backends
    doc = _doc(store, name="scan_kamera_01.pdf")
    assert doc.check.ok and doc.name_warning
    result = submit_documents(store, "Budi", [doc], storage, logs)
    assert result.saved[0]["Status"] == "Uploaded (cek nama file)"


def test_multiple_documents_in_one_submit(store, backends):
    storage, logs = backends
    docs = [_doc(store), _doc(store, program="JCP", skp="9061/SKT/SKP/P2/GT/X/2026"),
            _doc(store, brand="FACERINNA", quarter="2026 Q1", skp="9280/FCR/SKP/P1B/GT/X/2026")]
    result = submit_documents(store, "Budi", docs, storage, logs)
    assert result.ok and len(result.saved) == 3
    assert {r["GCS Path"].split("/")[1] for r in result.saved} == {"jbp", "jcp"}


def test_manual_selection_without_skp_number_is_allowed(store, backends):
    storage, logs = backends
    doc = _doc(store, brand="TIMEPHORIA", program="JCP", quarter="2026 Q2", skp="", name="apa.pdf")
    result = submit_documents(store, "Budi", [doc], storage, logs)
    assert result.ok
    assert result.saved[0]["Nama File"] == "SKP - TOKO ALFA - TANPA-NOMOR_TIMEPHORIA_JCP_2026_Q2.pdf"
    assert result.saved[0]["Status"] == "Uploaded"  # no SKP -> nothing to compare, no flag


def test_submit_requires_pic_and_valid_files(store, backends):
    storage, logs = backends
    with pytest.raises(ValueError, match="PIC"):
        submit_documents(store, "  ", [_doc(store)], storage, logs)
    with pytest.raises(ValueError):
        submit_documents(store, "Budi", [], storage, logs)
    big = _doc(store, data=pdf_bytes(1024 * 1024 + 10))
    with pytest.raises(ValueError, match="belum valid"):
        submit_documents(store, "Budi", [big], storage, logs)
    assert logs.read_values() == []


def test_partial_upload_failure_logs_only_successful_documents(store, backends):
    storage, logs = backends

    class FlakyStorage(LocalStorage):
        def upload_pdf(self, path, data):
            if "/jcp/" in path:
                raise StorageError("Bucket GCS tidak ditemukan.")
            super().upload_pdf(path, data)

    flaky = FlakyStorage(storage._root)
    docs = [_doc(store), _doc(store, program="JCP", skp="9061/SKT/SKP/P2/GT/X/2026")]
    result = submit_documents(store, "Budi", docs, flaky, logs)
    assert not result.ok and len(result.saved) == 1 and len(result.failed) == 1
    assert len(logs_to_frame(logs.read_values())) == 1


def test_log_failure_is_reported_and_retry_is_safe(store, backends):
    storage, logs = backends

    class BrokenLog:
        label = "broken"

        def read_values(self):
            return []

        def upsert(self, records):
            raise RuntimeError("quota")

    result = submit_documents(store, "Budi", [_doc(store)], storage, BrokenLog())
    assert result.log_error and not result.saved and len(result.failed) == 1
    retry = submit_documents(store, "Budi", [_doc(store)], storage, logs)
    assert retry.ok and len(logs_to_frame(logs.read_values())) == 1


def test_formula_like_pic_is_stored_as_plain_text(store, backends):
    storage, logs = backends
    result = submit_documents(store, '=HYPERLINK("x")', [_doc(store)], storage, logs)
    assert result.saved[0]["PIC Submit"] == '=HYPERLINK("x")'  # written with RAW input option


def test_path_collision_between_brands_gets_brand_suffix(store):
    a = _doc(store, brand="SKINTIFIC", skp="111/SAME")
    b = _doc(store, brand="FACERINNA", skp="111/SAME")
    assert a.object_path == b.object_path
    resolve_path_collisions(store, [a, b], pd.DataFrame())
    assert a.object_path != b.object_path
    assert b.target_filename.endswith("(FACERINNA).pdf")


def test_local_storage_blocks_path_traversal(tmp_path):
    with pytest.raises(StorageError):
        LocalStorage(tmp_path).upload_pdf("../evil.pdf", b"%PDF-")


# --- ZIP ------------------------------------------------------------------------------

def test_zip_keeps_quarter_program_region_structure(store, backends):
    storage, logs = backends
    docs = [_doc(store), _doc(store, program="JCP", quarter="2026 Q1", skp="9061/SKT/SKP/P2/GT/X/2026")]
    submit_documents(store, "Budi", docs, storage, logs)
    paths = list(logs_to_frame(logs.read_values())["GCS Path"]) + ["2026 Q2/jbp/East Java/hilang.pdf"]
    result = build_zip(paths, storage)
    names = zipfile.ZipFile(io.BytesIO(result.data)).namelist()
    assert sorted(names) == sorted([
        "2026 Q4/jbp/East Java/SKP - TOKO ALFA - 9784_SKT_SKP_P1B_GT_XI_2026.pdf",
        "2026 Q1/jcp/East Java/SKP - TOKO ALFA - 9061_SKT_SKP_P2_GT_X_2026.pdf",
    ])
    assert result.missing == ["2026 Q2/jbp/East Java/hilang.pdf"]


def test_zip_refuses_too_many_files(backends):
    storage, _ = backends
    with pytest.raises(StorageError, match="Terlalu banyak"):
        build_zip([f"2026 Q1/jbp/R/{i}.pdf" for i in range(5)], storage, max_files=3)


# --- admin aggregations ------------------------------------------------------------------

def test_dashboard_counts_and_compliance(store, backends, source_df):
    storage, logs = backends
    submit_documents(store, "Budi", [_doc(store), _doc(store, program="JCP", skp="9061/SKT/SKP/P2/GT/X/2026")],
                     storage, logs, timestamp="2026-11-09 10:00:00")
    df = logs_to_frame(logs.read_values())
    stats = dashboard_stats(df)
    assert stats == {"uploads": 1, "stores": 1, "documents": 2, "regions": 1, "brands": 1}
    assert dict(zip(count_by(df, "Jenis Program")["Jenis Program"], count_by(df, "Jenis Program")["Dokumen"])) == \
        {"JBP": 1, "JCP": 1}

    full = compliance_frame(df, expected_documents(source_df))
    t100 = full[full["Kode Toko/Group"] == "T100"]
    assert set(t100[t100["Status"] == STATUS_NOT_UPLOADED]["Kuartal"]) == {"2026 Q3", "2026 Q1"}
    assert len(apply_filters(full, status="Sudah upload")) == 2
    assert len(apply_filters(full, brands=["FACERINNA"], status="Belum upload")) == 1
    assert len(apply_filters(full, store_query="alfa", quarters=["2026 Q4"])) == 2


# --- config / secrets -----------------------------------------------------------------

FAKE_SA = {"type": "service_account", "client_email": "x@y.iam.gserviceaccount.com",
           "private_key": "fake-test-key-SECRETKEY",
           "token_uri": "https://oauth2.googleapis.com/token", "project_id": "p"}


def test_service_account_json_string_with_real_newlines_is_accepted():
    raw = '{"type": "service_account", "client_email": "a@b", "token_uri": "t", "private_key": "-----BEGIN\nX\n-----END"}'
    info, err = parse_service_account(raw)
    assert info and not err


def test_invalid_service_account_error_never_contains_key_material():
    info, err = parse_service_account('{"type": "service_account", "private_key": "SECRETKEY" ')
    assert info is None and "SECRETKEY" not in err


def test_placeholder_service_account_is_reported_incomplete():
    info, err = parse_service_account('{"type": "service_account"}')
    assert info is None and "belum lengkap" in err


def test_settings_repr_hides_secrets():
    s = settings_from_mapping({"GCP_SERVICE_ACCOUNT_JSON": FAKE_SA, "ADMIN_PASSWORD": "pw-rahasia"}, env={})
    assert "SECRETKEY" not in repr(s) and "pw-rahasia" not in repr(s)
    assert s.service_account_email == "x@y.iam.gserviceaccount.com"


def test_mode_auto_without_credentials_is_mock():
    mode = resolve_mode(settings_from_mapping({}, env={}))
    assert mode.mock and mode.can_upload and not mode.source_live


def test_mode_live_without_bucket_blocks_upload_instead_of_saving_locally():
    s = settings_from_mapping({"GCP_SERVICE_ACCOUNT_JSON": FAKE_SA, "SOURCE_SPREADSHEET_ID": "abc"}, env={})
    mode = resolve_mode(s)
    assert not mode.mock and not mode.can_upload
    assert any("GCS_BUCKET" in p for p in mode.problems)


def test_mode_live_fully_configured():
    s = settings_from_mapping({"GCP_SERVICE_ACCOUNT_JSON": FAKE_SA, "SOURCE_SPREADSHEET_ID": "abc",
                               "GCS_BUCKET": "b"}, env={})
    mode = resolve_mode(s)
    assert not mode.mock and mode.can_upload and mode.source_live and not mode.problems


def test_gcp_service_account_toml_table_like_other_apps_is_accepted():
    from streamlit.runtime.secrets import AttrDict  # what st.secrets returns for a [table]

    s = settings_from_mapping({"gcp_service_account": AttrDict(FAKE_SA), "SOURCE_SPREADSHEET_ID": "abc",
                               "GCS_BUCKET": "b"}, env={})
    assert s.service_account_email == "x@y.iam.gserviceaccount.com"
    assert resolve_mode(s).can_upload


def test_explicit_mock_mode_with_credentials_reads_live_source_but_writes_locally():
    s = settings_from_mapping({"APP_MODE": "mock", "GCP_SERVICE_ACCOUNT_JSON": FAKE_SA,
                               "SOURCE_SPREADSHEET_ID": "abc"}, env={})
    mode = resolve_mode(s)
    assert mode.mock and mode.source_live


# --- loading popup ------------------------------------------------------------------

def test_loading_popup_runs_fast_to_98_then_done_shows_100():
    from src.ui import loading_html

    running = loading_html("Memuat <data>", fast_seconds=0.8)
    assert "from { --skp-p: 1; } to { --skp-p: 98; }" in running  # fast phase stops at 98%
    assert "--skp-dur: 0.80s" in running and "Memuat &lt;data&gt;" in running  # label escaped
    assert 'class="skp-loading"' in running
    done = loading_html("Memuat", done=True)
    assert 'class="skp-loading done"' in done and "Selesai" in done  # 100% only after real work
