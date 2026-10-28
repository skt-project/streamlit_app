import inspect

import pytest

import mobile
from admin_auth import credentials_match, lock_remaining, record_failure, reset
from config import ConfigError, load_config


def _secrets(**app):
    base = {
        "output_google_sheet_id": "abcDEF_123",
        "bigquery_project": "proj",
        "bigquery_dataset": "gt_schema",
        "bigquery_table": "timephoria_smart_coverage_tester_feedback",
        "evidence_bucket": "my-bucket",
        "sku_codes": ["TST000001", "TST000001", "TST000002"],
    }
    base.update(app)
    return {"app": base, "admin": {"username": "admin", "password": "pw-for-tests-only"}}


def test_config_extracts_sheet_url_hides_password_and_rejects_bad_values():
    cfg = load_config(_secrets(output_google_sheet_id="https://docs.google.com/spreadsheets/d/abcDEF_123/edit"))
    assert cfg.output_sheet_id == "abcDEF_123"
    assert cfg.sku_codes == ("TST000001", "TST000002")
    assert "pw-for-tests-only" not in repr(cfg)
    with pytest.raises(ConfigError):
        load_config(_secrets(evidence_bucket="NOT A BUCKET"))
    with pytest.raises(ConfigError):
        load_config(_secrets(sku_codes=[]))


def test_admin_auth_constant_time_and_lock():
    assert credentials_match(" admin ", "pw-for-tests-only", "admin", "pw-for-tests-only")
    assert not credentials_match("Admin", "pw-for-tests-only", "admin", "pw-for-tests-only")
    assert not credentials_match("admin", "x", "", "")
    state = {}
    for _ in range(5):
        record_failure(state, 100.0)
    assert lock_remaining(state, 100.0) == 60
    assert lock_remaining(state, 160.0) == 0
    reset(state)
    assert lock_remaining(state, 200.0) == 0


def test_mobile_css_and_supported_keypad_hook():
    css = mobile._CSS
    assert 'input[aria-label="Stock"]::placeholder' in css and "Sell Out" not in css
    assert "#d32f2f" in css and 'content: "Ambil gambar"' in css and "font-weight: 700" in css
    assert "stCameraInputSwitchButton" not in css
    src = inspect.getsource(mobile)
    assert "st.html" in src and "unsafe_allow_javascript=True" in src
    assert "import streamlit.components" not in src
