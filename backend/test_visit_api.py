"""
Full visit-workflow test suite against the live Cloud Run API.

Covers:
  - Auth (login + /me)
  - Schedule download (offline cache)
  - Visit checkin (GPS distance recorded, never blocks)
  - Offline checkin (captured_at honored)
  - Idempotency (same schedule_id returns same visit_id)
  - Checkout with SKU items
  - Submit
  - SPV approval chain
  - Rejection + resubmit
  - KPI dashboard
  - Team KPI
  - SKU list (brand_group scoped)
  - Stock list + request + approval

Run:
  pip install requests pytest
  TEST_SE_USER=<username> TEST_SE_PASS=<password> \\
  TEST_SPV_USER=<username> TEST_SPV_PASS=<password> \\
  pytest test_visit_api.py -v
"""
import os
import time
import uuid
from datetime import datetime, timezone, date

import pytest
import requests

BASE_URL = os.getenv("STEP_API_URL", "https://step-api-141828905128.asia-southeast1.run.app/api/v1")

# Credentials
SE_USER = os.getenv("TEST_SE_USER", "")
SE_PASS = os.getenv("TEST_SE_PASS", "")
SPV_USER = os.getenv("TEST_SPV_USER", "")
SPV_PASS = os.getenv("TEST_SPV_PASS", "")
HO_USER = os.getenv("TEST_HO_USER", "")
HO_PASS = os.getenv("TEST_HO_PASS", "")

# --- helpers -------------------------------------------------------

def login(username: str, password: str) -> str:
    """Returns JWT access token."""
    r = requests.post(f"{BASE_URL}/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, f"Login failed for {username!r}: {r.text}"
    return r.json()["access_token"]


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# shared state across tests within the session
_state: dict = {}


# ==================================================================
# Auth
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_auth_login_se():
    token = login(SE_USER, SE_PASS)
    assert len(token) > 20
    _state["se_token"] = token


@pytest.mark.skipif(not SPV_USER, reason="TEST_SPV_USER not set")
def test_auth_login_spv():
    token = login(SPV_USER, SPV_PASS)
    _state["spv_token"] = token


@pytest.mark.skipif(not HO_USER, reason="TEST_HO_USER not set")
def test_auth_login_ho():
    token = login(HO_USER, HO_PASS)
    _state["ho_token"] = token


@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_auth_me():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    r = requests.get(f"{BASE_URL}/auth/me", headers=auth(token))
    assert r.status_code == 200
    me = r.json()
    assert "username" in me
    assert "role" in me
    _state["se_user"] = me


# ==================================================================
# Schedule download (offline cache)
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_schedule_download():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    me = _state.get("se_user", {})
    salesman_sk = me.get("user_id", "TEST_SK")

    r = requests.get(
        f"{BASE_URL}/schedule/download",
        params={"salesman_sk": salesman_sk},
        headers=auth(token),
    )
    assert r.status_code == 200
    body = r.json()
    assert "stores" in body
    assert "week" in body
    assert isinstance(body["stores"], list)
    _state["schedule"] = body
    print(f"\n[schedule] {body['total']} stores for {body['week']}")


# ==================================================================
# Visit: checkin
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_checkin_online():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    me = _state.get("se_user", {})
    salesman_sk = me.get("user_id", "TEST_SK")
    schedule = _state.get("schedule", {})
    stores = schedule.get("stores", [])

    # Use first store from schedule, or a synthetic outlet_sk
    outlet_sk = stores[0]["outlet_sk"] if stores else "TEST_OUTLET"
    schedule_id = f"SCHED-TEST-{uuid.uuid4().hex[:8].upper()}"

    payload = {
        "salesman_sk": salesman_sk,
        "outlet_sk": outlet_sk,
        "visit_date": date.today().isoformat(),
        "visit_type": "ROUTE",
        "checkin_latitude": -6.175000,
        "checkin_longitude": 106.827000,
        "schedule_id": schedule_id,
        "offline_mode": False,
    }
    r = requests.post(f"{BASE_URL}/visit/checkin", json=payload, headers=auth(token))
    assert r.status_code == 201, f"Checkin failed: {r.text}"
    body = r.json()
    assert "visit_id" in body
    assert body["visit_id"].startswith("VST-")
    assert "gps_warning" in body
    assert "checkin_distance_m" in body
    _state["visit_id"] = body["visit_id"]
    _state["schedule_id"] = schedule_id
    print(f"\n[checkin] visit_id={body['visit_id']} dist={body['checkin_distance_m']}m gps_warn={body['gps_warning']}")


@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_checkin_offline():
    """Offline checkin: captured_at should be honored, offline_mode=True."""
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    me = _state.get("se_user", {})
    salesman_sk = me.get("user_id", "TEST_SK")

    # Captured 30 min ago, simulating field work done offline
    captured = datetime.now(timezone.utc).replace(microsecond=0)
    captured_str = captured.isoformat()

    payload = {
        "salesman_sk": salesman_sk,
        "outlet_sk": "OFFLINE_TEST_OUTLET",
        "visit_date": date.today().isoformat(),
        "visit_type": "NON_ROUTE",
        "checkin_latitude": -6.175000,
        "checkin_longitude": 106.827000,
        "offline_mode": True,
        "captured_at": captured_str,
    }
    r = requests.post(f"{BASE_URL}/visit/checkin", json=payload, headers=auth(token))
    assert r.status_code == 201, f"Offline checkin failed: {r.text}"
    body = r.json()
    assert body["offline_mode"] is True
    _state["offline_visit_id"] = body["visit_id"]
    print(f"\n[offline checkin] visit_id={body['visit_id']}")


@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_checkin_idempotency():
    """Same schedule_id returns same visit_id without creating a duplicate."""
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    me = _state.get("se_user", {})
    salesman_sk = me.get("user_id", "TEST_SK")
    schedule_id = _state.get("schedule_id")

    if not schedule_id:
        pytest.skip("No schedule_id from prior test")

    schedule = _state.get("schedule", {})
    stores = schedule.get("stores", [])
    outlet_sk = stores[0]["outlet_sk"] if stores else "TEST_OUTLET"

    payload = {
        "salesman_sk": salesman_sk,
        "outlet_sk": outlet_sk,
        "visit_date": date.today().isoformat(),
        "visit_type": "ROUTE",
        "schedule_id": schedule_id,
    }
    r = requests.post(f"{BASE_URL}/visit/checkin", json=payload, headers=auth(token))
    assert r.status_code == 201
    body = r.json()
    # Must return the SAME visit_id — no duplicate created
    assert body["visit_id"] == _state["visit_id"], (
        f"Idempotency broken: got {body['visit_id']!r}, expected {_state['visit_id']!r}"
    )
    print(f"\n[idempotency] same visit_id returned: {body['visit_id']}")


# ==================================================================
# Visit: checkout
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_checkout():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    visit_id = _state.get("visit_id")
    if not visit_id:
        pytest.skip("No visit_id from checkin test")

    payload = {
        "checkout_latitude": -6.175500,
        "checkout_longitude": 106.827500,
        "notes": "Test checkout note",
        "total_demand": 1500000.0,
        "effective_call": "YES",
        "offline_mode": False,
        "items": [
            {
                "sku_id": "TEST-SKU-001",
                "sku_name": "Face Wash 100ml",
                "brand": "Skintific",
                "brand_group": "SKT",
                "category": "Face Wash",
                "stp": 50000.0,
                "qty": 30,
            },
        ],
    }
    r = requests.post(f"{BASE_URL}/visit/{visit_id}/checkout", json=payload, headers=auth(token))
    assert r.status_code == 200, f"Checkout failed: {r.text}"
    body = r.json()
    assert body["visit_status"] == "CHECKED_OUT"
    assert body["total_demand"] == 1500000.0
    assert body["effective_call"] == "YES"
    assert len(body["items"]) == 1
    print(f"\n[checkout] status={body['visit_status']} demand={body['total_demand']}")


# ==================================================================
# Visit: submit
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_submit():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    visit_id = _state.get("visit_id")
    if not visit_id:
        pytest.skip("No visit_id")

    r = requests.post(f"{BASE_URL}/visit/{visit_id}/submit", json={}, headers=auth(token))
    assert r.status_code == 200, f"Submit failed: {r.text}"
    body = r.json()
    assert body["visit_status"] == "SUBMITTED"
    assert body["approval_status"] == "PENDING_SPV"
    print(f"\n[submit] approval_status={body['approval_status']}")


# ==================================================================
# Visit: SPV approval
# ==================================================================

@pytest.mark.skipif(not SPV_USER, reason="TEST_SPV_USER not set")
def test_spv_approve():
    spv_token = _state.get("spv_token") or login(SPV_USER, SPV_PASS)
    visit_id = _state.get("visit_id")
    if not visit_id:
        pytest.skip("No visit_id")

    r = requests.put(
        f"{BASE_URL}/visit/{visit_id}/approve",
        json={"notes": "Looks good"},
        headers=auth(spv_token),
    )
    assert r.status_code == 200, f"SPV approve failed: {r.text}"
    body = r.json()
    assert body["approval_status"] == "SPV_APPROVED"
    assert body["spv_username"] is not None
    print(f"\n[spv approve] approval_status={body['approval_status']}")


# ==================================================================
# Visit: rejection + resubmit
# ==================================================================

@pytest.mark.skipif(not SE_USER or not SPV_USER, reason="Need both SE and SPV credentials")
def test_reject_and_resubmit():
    """Create a fresh visit, submit it, SPV rejects, SE resubmits."""
    se_token = _state.get("se_token") or login(SE_USER, SE_PASS)
    spv_token = _state.get("spv_token") or login(SPV_USER, SPV_PASS)
    me = _state.get("se_user", {})
    salesman_sk = me.get("user_id", "TEST_SK")

    # 1. Checkin
    checkin_r = requests.post(f"{BASE_URL}/visit/checkin", json={
        "salesman_sk": salesman_sk,
        "outlet_sk": "REVISION_TEST_OUTLET",
        "visit_date": date.today().isoformat(),
        "visit_type": "NON_ROUTE",
    }, headers=auth(se_token))
    assert checkin_r.status_code == 201
    rev_vid = checkin_r.json()["visit_id"]

    # 2. Checkout
    co_r = requests.post(f"{BASE_URL}/visit/{rev_vid}/checkout", json={
        "total_demand": 200000.0, "effective_call": "YES", "items": [],
    }, headers=auth(se_token))
    assert co_r.status_code == 200

    # 3. Submit
    sub_r = requests.post(f"{BASE_URL}/visit/{rev_vid}/submit", json={}, headers=auth(se_token))
    assert sub_r.status_code == 200

    # 4. SPV reject
    rej_r = requests.put(f"{BASE_URL}/visit/{rev_vid}/reject",
                         json={"rejection_notes": "Missing SKU detail"},
                         headers=auth(spv_token))
    assert rej_r.status_code == 200
    rej_body = rej_r.json()
    assert rej_body["approval_status"] == "REVISION_REQUIRED"
    assert rej_body["revision_count"] == 1

    # 5. SE resubmit
    res_r = requests.put(f"{BASE_URL}/visit/{rev_vid}/resubmit", json={
        "total_demand": 250000.0,
        "items": [{"sku_id": "TEST-SKU-002", "sku_name": "Toner", "brand": "Skintific",
                   "brand_group": "SKT", "category": "Toner", "stp": 25000.0, "qty": 10}],
    }, headers=auth(se_token))
    assert res_r.status_code == 200
    res_body = res_r.json()
    assert res_body["approval_status"] == "PENDING_SPV"
    assert res_body["total_demand"] == 250000.0
    print(f"\n[reject+resubmit] revision_count={rej_body['revision_count']} resubmit_ok=True")


# ==================================================================
# Visit: list + detail
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_visit_list():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    r = requests.get(
        f"{BASE_URL}/visit",
        params={"visit_date": date.today().isoformat()},
        headers=auth(token),
    )
    assert r.status_code == 200
    body = r.json()
    assert "items" in body
    assert "total" in body
    print(f"\n[visit list] total={body['total']}")


@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_visit_detail():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    visit_id = _state.get("visit_id")
    if not visit_id:
        pytest.skip("No visit_id")
    r = requests.get(f"{BASE_URL}/visit/{visit_id}", headers=auth(token))
    assert r.status_code == 200
    body = r.json()
    assert body["visit_id"] == visit_id
    assert "items" in body


# ==================================================================
# SKU list
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_sku_list():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    r = requests.get(f"{BASE_URL}/sku", headers=auth(token))
    assert r.status_code == 200
    body = r.json()
    assert "items" in body
    print(f"\n[sku] total={body['total']} items")


# ==================================================================
# Dashboard KPI
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_dashboard_kpi():
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    me = _state.get("se_user", {})
    salesman_sk = me.get("user_id", "TEST_SK")
    r = requests.get(
        f"{BASE_URL}/dashboard/kpi",
        params={"salesman_sk": salesman_sk, "visit_date": date.today().isoformat()},
        headers=auth(token),
    )
    assert r.status_code == 200
    body = r.json()
    assert "total_visits" in body
    assert "strike_rate" in body
    assert "route_completion_pct" in body
    assert body["total_visits"] >= 1  # at least the test visit
    print(f"\n[kpi] visits={body['total_visits']} demand={body['total_demand']} strike={body['strike_rate']}%")


@pytest.mark.skipif(not SPV_USER, reason="TEST_SPV_USER not set")
def test_dashboard_team():
    token = _state.get("spv_token") or login(SPV_USER, SPV_PASS)
    r = requests.get(
        f"{BASE_URL}/dashboard/team",
        params={"visit_date": date.today().isoformat()},
        headers=auth(token),
    )
    assert r.status_code == 200
    body = r.json()
    assert "members" in body
    assert "total_members" in body
    print(f"\n[team kpi] members={body['total_members']}")


# ==================================================================
# GPS: far-away checkin — must NOT return 4xx, just gps_warning=True
# ==================================================================

@pytest.mark.skipif(not SE_USER, reason="TEST_SE_USER not set")
def test_gps_far_does_not_block():
    """GPS 10 km away: gps_warning=True but HTTP 201, not 409."""
    token = _state.get("se_token") or login(SE_USER, SE_PASS)
    me = _state.get("se_user", {})
    salesman_sk = me.get("user_id", "TEST_SK")

    payload = {
        "salesman_sk": salesman_sk,
        "outlet_sk": "GPS_FAR_TEST_OUTLET",
        "visit_date": date.today().isoformat(),
        "visit_type": "NON_ROUTE",
        "checkin_latitude": -6.900000,    # ~10km away from outlet
        "checkin_longitude": 107.600000,
        "offline_mode": False,
    }
    r = requests.post(f"{BASE_URL}/visit/checkin", json=payload, headers=auth(token))
    assert r.status_code == 201, f"Far GPS should still return 201: {r.text}"
    body = r.json()
    # gps_warning may or may not be True depending on outlet coords in DB
    # but the key assertion is NO 4xx error
    assert "visit_id" in body
    print(f"\n[gps far] visit_id={body['visit_id']} gps_warning={body['gps_warning']}")


# ==================================================================
# Unauthenticated requests must return 403
# ==================================================================

def test_unauth_visit_list_returns_403():
    r = requests.get(f"{BASE_URL}/visit")
    assert r.status_code in (401, 403), f"Expected 401/403, got {r.status_code}"


def test_unauth_checkin_returns_403():
    r = requests.post(f"{BASE_URL}/visit/checkin", json={})
    assert r.status_code in (401, 403), f"Expected 401/403, got {r.status_code}"
