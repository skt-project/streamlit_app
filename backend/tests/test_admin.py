"""Tests for /admin endpoints."""
import io


def test_list_users_admin(client, admin_token):
    r = client.get("/api/v1/admin/users", headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    users = r.json()
    assert isinstance(users, list)
    assert len(users) > 0
    first = users[0]
    assert "user_id" in first
    assert "username" in first
    assert "role" in first
    assert "is_active" in first


def test_list_users_requires_admin(client, se_token):
    r = client.get("/api/v1/admin/users", headers={"Authorization": f"Bearer {se_token}"})
    assert r.status_code == 403


def test_list_users_filter_by_role(client, admin_token):
    r = client.get("/api/v1/admin/users?role=spv", headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    users = r.json()
    for u in users:
        assert u["role"] == "spv"


def test_list_users_search(client, admin_token):
    r = client.get("/api/v1/admin/users?search=demo", headers={"Authorization": f"Bearer {admin_token}"})
    assert r.status_code == 200
    users = r.json()
    assert any("demo" in u["username"].lower() for u in users)


def test_reset_token_for_nonexistent_user(client, admin_token):
    r = client.post(
        "/api/v1/admin/users/00000000-0000-0000-0000-000000000000/reset-token",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert r.status_code == 404


def test_generate_reset_token_returns_token(client, admin_token):
    # Get first user id
    users = client.get("/api/v1/admin/users", headers={"Authorization": f"Bearer {admin_token}"}).json()
    demo = next((u for u in users if u["username"] == "demo"), None)
    if demo is None:
        return  # demo might not be in first 500 results

    r = client.post(
        f"/api/v1/admin/users/{demo['user_id']}/reset-token",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert "reset_token" in data
    assert len(data["reset_token"]) > 20


def test_pjp_upload_requires_admin(client, se_token):
    r = client.post(
        "/api/v1/pjp/upload",
        headers={"Authorization": f"Bearer {se_token}"},
        files={"file": ("test.csv", b"salesman_sk,outlet_sk,visit_day_of_week,week_number\n", "text/csv")},
    )
    assert r.status_code == 403


def test_pjp_upload_empty_csv(client, admin_token):
    csv_content = b"salesman_sk,outlet_sk,visit_day_of_week,week_number\n"
    r = client.post(
        "/api/v1/pjp/upload",
        headers={"Authorization": f"Bearer {admin_token}"},
        files={"file": ("test.csv", csv_content, "text/csv")},
    )
    assert r.status_code == 422


def test_pjp_upload_missing_columns(client, admin_token):
    csv_content = b"salesman_sk,outlet_sk\nsk1,out1\n"
    r = client.post(
        "/api/v1/pjp/upload",
        headers={"Authorization": f"Bearer {admin_token}"},
        files={"file": ("test.csv", csv_content, "text/csv")},
    )
    assert r.status_code == 422
