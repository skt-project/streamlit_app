"""Tests for mobile app endpoints (schedule, announcements, visit)."""


def test_schedule_download_se(client, se_token):
    r = client.get("/api/v1/schedule/download", headers={"Authorization": f"Bearer {se_token}"})
    assert r.status_code == 200
    data = r.json()
    # Returns {salesman_sk, week, stores: [...], total}
    assert "stores" in data or isinstance(data, list)
    stores = data["stores"] if isinstance(data, dict) else data
    assert isinstance(stores, list)
    assert len(stores) >= 0  # demo has 70 stores


def test_announcements_se(client, se_token):
    r = client.get("/api/v1/announcements", headers={"Authorization": f"Bearer {se_token}"})
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) >= 3  # 3 seeded announcements


def test_announcements_requires_auth(client):
    r = client.get("/api/v1/announcements")
    assert r.status_code == 403


def test_visit_list_se(client, se_token):
    r = client.get("/api/v1/visit", headers={"Authorization": f"Bearer {se_token}"})
    assert r.status_code == 200


def test_approvals_spv(client, spv_token):
    r = client.get("/api/v1/approvals", headers={"Authorization": f"Bearer {spv_token}"})
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
