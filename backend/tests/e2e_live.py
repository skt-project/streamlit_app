"""
STEP Platform — End-to-End Live Test
Runs against the production Cloud Run API.

Usage:
    python tests/e2e_live.py
    python tests/e2e_live.py --base https://your-staging-url/api/v1

Requirements: pip install requests
"""
import argparse
import sys
import requests

BASE_DEFAULT = "https://step-api-141828905128.asia-southeast1.run.app/api/v1"

CREDS = {
    "se":             ("demo",            "STEP@2026"),
    "spv":            ("agung_darmawan",  "STEP@2026"),
    "dist_admin":     ("test_dist",       "STEP@2026"),
    "ho_admin":       ("admin",           "Step@2026!"),
}

PASS_MARK  = "  [PASS]"
FAIL_MARK  = "  [FAIL]"
SKIP_MARK  = "  [SKIP]"

errors: list[str] = []


def _result(label: str, r: requests.Response, expected: int = 200) -> requests.Response:
    ok = r.status_code == expected
    mark = PASS_MARK if ok else FAIL_MARK
    print(f"{mark} {r.status_code}  {label}")
    if not ok:
        body = r.text[:300].replace("\n", " ")
        print(f"         {body}")
        errors.append(f"{label} → {r.status_code}")
    return r


def _login(base: str, role_key: str) -> tuple[str, dict]:
    username, password = CREDS[role_key]
    r = requests.post(f"{base}/auth/login", json={"username": username, "password": password}, timeout=15)
    ok = r.status_code == 200
    mark = PASS_MARK if ok else FAIL_MARK
    print(f"{mark} {r.status_code}  Login {role_key} ({username})")
    if not ok:
        errors.append(f"Login {role_key} → {r.status_code}")
        return "", {}
    user = r.json()["user"]
    token = r.json()["access_token"]
    print(f"         role={user['role']}  brand_group={user.get('brand_group', '-')}  salesman_sk={str(user.get('salesman_sk',''))[:12]}...")
    return token, user


def run(base: str) -> None:
    print(f"\n{'='*60}")
    print(f"  STEP E2E LIVE TEST")
    print(f"  Base: {base}")
    print(f"{'='*60}")

    # --- 1. Login all roles -----------------------------------------
    print("\n-- 1. Login All Roles --------------------------------------")
    se_token,    se_user    = _login(base, "se")
    spv_token,   _          = _login(base, "spv")
    dist_token,  dist_user  = _login(base, "dist_admin")
    admin_token, _          = _login(base, "ho_admin")

    if not se_token or not spv_token:
        print("\n[ABORT] Core logins failed — cannot continue E2E.")
        sys.exit(1)

    H_SE    = {"Authorization": f"Bearer {se_token}"}
    H_SPV   = {"Authorization": f"Bearer {spv_token}"}
    H_DIST  = {"Authorization": f"Bearer {dist_token}"} if dist_token else {}
    H_ADMIN = {"Authorization": f"Bearer {admin_token}"}

    # --- 2. Health check --------------------------------------------
    print("\n-- 2. Health -----------------------------------------------")
    _result("GET /health", requests.get(f"{base.replace('/api/v1','')}/health", timeout=10))

    # --- 3. Schedule download ---------------------------------------
    print("\n-- 3. Schedule Download (SE) -------------------------------")
    r = _result("GET /schedule/download", requests.get(f"{base}/schedule/download", headers=H_SE, timeout=30))
    stores = []
    if r.ok:
        data = r.json()
        stores = data.get("stores", data if isinstance(data, list) else [])
        print(f"         {len(stores)} stores returned")

    if not stores:
        print(f"{SKIP_MARK} No stores — skipping visit flow.")
        _finish()
        return

    store = stores[0]
    print(f"         Using: outlet_sk={store['outlet_sk']}  name={store.get('outlet_name','?')[:40]}")

    # --- 4. Product list --------------------------------------------
    print("\n-- 4. Product List (SE) ------------------------------------")
    r = _result("GET /product", requests.get(f"{base}/product", headers=H_SE, timeout=30))
    products = []
    if r.ok:
        data = r.json()
        products = data.get("items", data if isinstance(data, list) else [])
        print(f"         {len(products)} products returned")
        sample = [
            {"sku_id": p["sku_id"], "sku_name": p.get("sku_name", ""), "brand": p.get("brand", ""),
             "stp": p.get("stp", 0), "qty": 2}
            for p in products[:3] if p.get("stp")
        ]
        print(f"         sample SKUs: {[s['sku_id'] for s in sample]}")

    # --- 5. Check-in ------------------------------------------------
    print("\n-- 5. Check-In (SE) ----------------------------------------")
    checkin_payload = {
        "salesman_sk":       se_user.get("salesman_sk") or se_user["user_id"],
        "outlet_sk":         store["outlet_sk"],
        "visit_date":        "2026-07-09",
        "visit_type":        "ROUTE",
        "checkin_latitude":  -6.2088,
        "checkin_longitude": 106.8456,
        "schedule_id":       store.get("route_plan_sk", store.get("schedule_id")),
    }
    r = _result("POST /visit/checkin", requests.post(f"{base}/visit/checkin", json=checkin_payload, headers=H_SE, timeout=30), expected=201)
    visit_id = None
    if r.ok:
        visit_id = r.json()["visit_id"]
        gps_warn = r.json().get("gps_warning", False)
        print(f"         visit_id={visit_id}  gps_warning={gps_warn}")

    if not visit_id:
        print(f"{SKIP_MARK} No visit_id — skipping remaining visit steps.")
        _finish()
        return

    # --- 6. Checkout ------------------------------------------------
    print("\n-- 6. Checkout (SE fills survey) ---------------------------")
    total = sum(s["qty"] * s["stp"] for s in sample)
    checkout_payload = {
        "total_demand":       total,
        "effective_call":     "YES",
        "items":              sample,
        "checkout_latitude":  -6.2088,
        "checkout_longitude": 106.8456,
    }
    r = _result("POST /visit/{id}/checkout", requests.post(f"{base}/visit/{visit_id}/checkout", json=checkout_payload, headers=H_SE, timeout=60))
    if r.ok:
        d = r.json()
        print(f"         visit_status={d.get('visit_status')}  approval_status={d.get('approval_status')}")
        items_back = d.get("items", [])
        print(f"         items_returned={len(items_back)}")

    # --- 7. Submit --------------------------------------------------
    print("\n-- 7. Submit to SPV (SE) -----------------------------------")
    submit_payload = {
        "total_demand":   total,
        "effective_call": "YES",
        "items":          sample,
    }
    r = _result("POST /visit/{id}/submit", requests.post(f"{base}/visit/{visit_id}/submit", json=submit_payload, headers=H_SE, timeout=60))
    if r.ok:
        d = r.json()
        print(f"         visit_status={d.get('visit_status')}  approval_status={d.get('approval_status')}")

    # --- 8. Visit detail (SE view, check warehouse_stock_qty) -------
    print("\n-- 8. Visit Detail — warehouse_stock_qty (SPV) -------------")
    r = _result("GET /visit/{id}", requests.get(f"{base}/visit/{visit_id}", headers=H_SPV, timeout=30))
    if r.ok:
        detail_items = r.json().get("items", [])
        print(f"         {len(detail_items)} items in response")
        for it in detail_items[:3]:
            wq = it.get("warehouse_stock_qty")
            print(f"           {it['sku_id']}  warehouse_stock_qty={wq}")
        has_stock = any(i.get("warehouse_stock_qty") is not None for i in detail_items)
        note = "populated (real store)" if has_stock else "None (test store — expected)"
        print(f"         warehouse_stock_qty: {note}")

    # --- 9. SPV approves --------------------------------------------
    print("\n-- 9. SPV Approve ------------------------------------------")
    r = _result("PUT /visit/{id}/approve (SPV)", requests.put(f"{base}/visit/{visit_id}/approve", json={"notes": "E2E approved"}, headers=H_SPV, timeout=30))
    if r.ok:
        print(f"         approval_status={r.json().get('approval_status')}")

    # --- 10. Dist admin sets store prices ---------------------------
    print("\n-- 10. Set Store Prices (dist_admin) -----------------------")
    if not H_DIST:
        print(f"{SKIP_MARK} dist_admin login failed — skipping")
    elif not sample:
        print(f"{SKIP_MARK} No sample SKUs — skipping")
    else:
        price_items = [{"sku_id": s["sku_id"], "price_for_store": 50000.0} for s in sample]
        r = _result("PUT /visit/{id}/store-price", requests.put(f"{base}/visit/{visit_id}/store-price", json={"items": price_items}, headers=H_DIST, timeout=30))
        if r.ok:
            items_back = r.json().get("items", [])
            priced = [i for i in items_back if i.get("price_for_store") is not None]
            print(f"         items_with_price={len(priced)}  sample={priced[0].get('price_for_store') if priced else None}")

    # --- 11. Dist admin approves ------------------------------------
    print("\n-- 11. Distributor Admin Approve ---------------------------")
    if not H_DIST:
        print(f"{SKIP_MARK} dist_admin login failed — skipping")
    else:
        r = _result("PUT /visit/{id}/approve (dist_admin)", requests.put(f"{base}/visit/{visit_id}/approve", json={}, headers=H_DIST, timeout=30))
        if r.ok:
            print(f"         approval_status={r.json().get('approval_status')}")

    # --- 12. PDF ----------------------------------------------------
    print("\n-- 12. PDF Download (dist_admin) ---------------------------")
    if H_DIST:
        r = _result("GET /visit/{id}/pdf", requests.get(f"{base}/visit/{visit_id}/pdf", headers=H_DIST, timeout=60))
        if r.ok:
            ct = r.headers.get("Content-Type", "")
            print(f"         {len(r.content):,} bytes  content-type={ct}")
    else:
        r = _result("GET /visit/{id}/pdf", requests.get(f"{base}/visit/{visit_id}/pdf", headers=H_SPV, timeout=60))
        if r.ok:
            print(f"         {len(r.content):,} bytes")

    # --- 13. Notifications ------------------------------------------
    print("\n-- 13. Notifications ---------------------------------------")
    _result("GET /notifications (SPV)",  requests.get(f"{base}/notifications",  headers=H_SPV,  timeout=15))
    _result("GET /notifications (dist)", requests.get(f"{base}/notifications",  headers=H_DIST, timeout=15)) if H_DIST else None

    # --- 14. Announcements ------------------------------------------
    print("\n-- 14. Announcements (SE) ----------------------------------")
    r = _result("GET /announcements", requests.get(f"{base}/announcements", headers=H_SE, timeout=15))
    if r.ok:
        print(f"         {len(r.json())} announcements")

    # --- 15. Approvals queue ----------------------------------------
    print("\n-- 15. Approvals Queue (SPV) -------------------------------")
    r = _result("GET /approvals", requests.get(f"{base}/approvals", headers=H_SPV, timeout=30))
    if r.ok:
        print(f"         {len(r.json())} items in approval queue")

    _finish()


def _finish() -> None:
    print(f"\n{'='*60}")
    if errors:
        print(f"  RESULT: {len(errors)} FAILURE(S)")
        for e in errors:
            print(f"    ✗  {e}")
        print(f"{'='*60}\n")
        sys.exit(1)
    else:
        print("  RESULT: ALL STEPS PASSED")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default=BASE_DEFAULT, help="API base URL (include /api/v1)")
    args = parser.parse_args()
    run(args.base)
