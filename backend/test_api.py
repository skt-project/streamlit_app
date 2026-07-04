"""
End-to-end API test suite for STEP backend.
Run from backend/ directory:

    python test_api.py [--url https://step-api-xxx.run.app]

Defaults to http://localhost:8000 if no --url given.
"""
import argparse
import json
import sys
import urllib.request
import urllib.error

parser = argparse.ArgumentParser()
parser.add_argument("--url", default="https://step-api-141828905128.asia-southeast1.run.app")
parser.add_argument("--username", default="admin")
parser.add_argument("--password", default="Step@2026!")
args = parser.parse_args()

BASE = args.url.rstrip("/")
PASS = 0
FAIL = 0
TOKEN = None


def _req(method, path, body=None, auth=True):
    url = BASE + path
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"}
    if auth and TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)}


def check(label, condition, detail=""):
    global PASS, FAIL
    if condition:
        print(f"  ✓  {label}")
        PASS += 1
    else:
        print(f"  ✗  {label}" + (f" — {detail}" if detail else ""))
        FAIL += 1


def section(title):
    print(f"\n{'─'*50}")
    print(f"  {title}")
    print(f"{'─'*50}")


# ==============================================================
# 1. Health
# ==============================================================
section("1. Health check")
status, body = _req("GET", "/health", auth=False)
check("GET /health → 200", status == 200, f"got {status}")
check("status=ok in response", body.get("status") == "ok", str(body))

# ==============================================================
# 2. Auth — login
# ==============================================================
section("2. Auth — login")
status, body = _req("POST", "/api/v1/auth/login",
                     {"username": args.username, "password": args.password}, auth=False)
check("POST /auth/login → 200", status == 200, f"got {status}: {body}")
check("access_token present", "access_token" in body, str(body))
check("user.role present", body.get("user", {}).get("role") is not None)

if "access_token" in body:
    TOKEN = body["access_token"]
    print(f"       role={body['user']['role']}  brand_group={body['user'].get('brand_group')}")

# Wrong password
status2, _ = _req("POST", "/api/v1/auth/login",
                   {"username": args.username, "password": "wrong"}, auth=False)
check("POST /auth/login wrong pw → 401", status2 == 401, f"got {status2}")

# ==============================================================
# 3. Auth — me
# ==============================================================
section("3. Auth — /me")
status, body = _req("GET", "/api/v1/auth/me")
check("GET /auth/me → 200", status == 200, f"got {status}")
check("username matches", body.get("username") == args.username)
check("brand_group field present", "brand_group" in body, str(body))

# Unauthenticated
status2, _ = _req("GET", "/api/v1/auth/me", auth=False)
check("GET /auth/me no token → 403", status2 in (401, 403), f"got {status2}")

# ==============================================================
# 4. Salesman list
# ==============================================================
section("4. Salesman — list")
status, body = _req("GET", "/api/v1/salesman?page_size=10")
check("GET /salesman → 200", status == 200, f"got {status}")
check("items is a list", isinstance(body.get("items"), list), str(body))
check("at least 1 salesman", len(body.get("items", [])) > 0, f"got {len(body.get('items', []))}")
check("total field present", "total" in body, str(body))
if body.get("items"):
    sm = body["items"][0]
    check("salesman has brand_group", "brand_group" in sm or sm.get("brand_group") is not None, str(sm))
    first_sk = sm.get("salesman_sk")
    print(f"       first: {sm.get('salesman_name')} / {sm.get('brand_group')} / {sm.get('region')}")

# Filter by search
status2, body2 = _req("GET", "/api/v1/salesman?q=a&page_size=5")
check("GET /salesman?q=a → 200", status2 == 200, f"got {status2}")

# ==============================================================
# 5. Route — salesman list
# ==============================================================
section("5. Route — salesman list")
status, body = _req("GET", "/api/v1/route/salesmen")
check("GET /route/salesmen → 200", status == 200, f"got {status}")
check("returns a list", isinstance(body, list), str(body))
check("at least 100 salesmen", len(body) >= 100, f"got {len(body)}")
if body:
    print(f"       total salesmen: {len(body)}")
    route_sk = body[0].get("salesman_sk")

# ==============================================================
# 6. Route — weekly plan
# ==============================================================
section("6. Route — weekly plan")
if body and route_sk:
    status, plan = _req("GET", f"/api/v1/route/plan?salesman_sk={route_sk}")
    check("GET /route/plan → 200", status == 200, f"got {status}")
    check("salesman_sk in response", plan.get("salesman_sk") == route_sk)
    check("days dict present", isinstance(plan.get("days"), dict))
    check("week_label present", bool(plan.get("week_label")))
    total_stops = sum(len(v) for v in plan.get("days", {}).values())
    print(f"       week: {plan.get('week_label')}  stops: {total_stops}")
else:
    check("GET /route/plan — skipped (no salesman_sk)", False, "no salesman from step 5")

# ==============================================================
# 7. Route — outlet search
# ==============================================================
section("7. Route — outlet search")
status, body = _req("GET", "/api/v1/route/outlets?page_size=5")
check("GET /route/outlets → 200", status == 200, f"got {status}")
check("items list present", isinstance(body.get("items"), list))
check("at least 1 outlet", len(body.get("items", [])) > 0)

# ==============================================================
# 8. Unauthorized access
# ==============================================================
section("8. Authorization guards")
old_token = TOKEN
TOKEN = "invalid.token.here"
status, _ = _req("GET", "/api/v1/salesman")
check("invalid token → 401", status == 401, f"got {status}")
TOKEN = old_token

# ==============================================================
# Summary
# ==============================================================
total = PASS + FAIL
print(f"\n{'═'*50}")
print(f"  Results: {PASS}/{total} passed  {'✓ ALL PASS' if FAIL == 0 else f'✗ {FAIL} FAILED'}")
print(f"{'═'*50}\n")

if FAIL > 0:
    sys.exit(1)
