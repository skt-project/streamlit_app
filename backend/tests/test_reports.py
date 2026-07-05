"""Tests for /reports endpoints."""


def test_reports_achievement(client, admin_token):
    r = client.get("/api/v1/reports?type=Achievement&period=Bulan%20Ini",
                   headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    data = r.json()
    assert "rows" in data
    assert "kpis" in data


def test_reports_route_compliance(client, admin_token):
    r = client.get("/api/v1/reports?type=Route%20Compliance&period=Bulan%20Ini",
                   headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    data = r.json()
    assert "rows" in data


def test_reports_csv_export(client, admin_token):
    r = client.get("/api/v1/reports/export.csv?type=Achievement&period=Bulan%20Ini",
                   headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    assert "text/csv" in r.headers.get("content-type", "")


def test_reports_requires_auth(client):
    r = client.get("/api/v1/reports")
    assert r.status_code == 403
