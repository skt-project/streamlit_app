"""Tests for /dashboard endpoints."""


def test_dashboard_web_admin(client, admin_token):
    r = client.get("/api/v1/dashboard/web", headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    data = r.json()
    assert "comply_pct" in data
    assert "route_comply_pct" in data
    assert "leaderboard" in data
    assert "announcements" in data
    assert isinstance(data["comply_brands"], list)


def test_dashboard_web_with_filters(client, admin_token):
    r = client.get(
        "/api/v1/dashboard/web?date_from=2026-07-01&date_to=2026-07-31&brand=Skintific",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert r.status_code == 200


def test_dashboard_web_requires_auth(client):
    r = client.get("/api/v1/dashboard/web")
    assert r.status_code == 403


def test_dashboard_kpi_se(client, se_token):
    r = client.get("/api/v1/dashboard/kpi", headers={"Authorization": f"Bearer {se_token}"})
    assert r.status_code == 200


def test_dashboard_team_spv(client, spv_token):
    r = client.get("/api/v1/dashboard/team", headers={"Authorization": f"Bearer {spv_token}"})
    assert r.status_code == 200
