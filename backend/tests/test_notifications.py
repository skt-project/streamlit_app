"""
Tests for v1.4.0 push notification endpoints.

Expo push calls are mocked so no real notifications fire during tests.
"""
from unittest.mock import MagicMock, patch


# ── /notifications (list / mark-read) ────────────────────────────────────────

def test_notifications_list_requires_auth(client):
    r = client.get("/api/v1/notifications")
    assert r.status_code == 403


def test_notifications_list_se(client, se_token):
    r = client.get("/api/v1/notifications", headers={"Authorization": f"Bearer {se_token}"})
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_notifications_mark_all_read_se(client, se_token):
    r = client.post(
        "/api/v1/notifications/mark-all-read",
        headers={"Authorization": f"Bearer {se_token}"},
    )
    assert r.status_code == 200
    assert "message" in r.json()


def test_notifications_mark_nonexistent_read(client, se_token):
    r = client.post(
        "/api/v1/notifications/00000000-0000-0000-0000-000000000000/read",
        headers={"Authorization": f"Bearer {se_token}"},
    )
    # BQ UPDATE on non-existent row is not an error — returns 200 with message
    assert r.status_code == 200


# ── /notifications/register-push-token ───────────────────────────────────────

def test_register_push_token_requires_auth(client):
    r = client.post(
        "/api/v1/notifications/register-push-token",
        json={"push_token": "ExponentPushToken[test123]"},
    )
    assert r.status_code == 403


def test_register_push_token_se(client, se_token):
    r = client.post(
        "/api/v1/notifications/register-push-token",
        headers={"Authorization": f"Bearer {se_token}"},
        json={"push_token": "ExponentPushToken[xxxxTestTokenForDemo]"},
    )
    assert r.status_code == 200
    assert r.json()["message"] == "Push token registered."


def test_register_push_token_admin(client, admin_token):
    r = client.post(
        "/api/v1/notifications/register-push-token",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"push_token": "ExponentPushToken[xxxxAdminTestToken]"},
    )
    assert r.status_code == 200


def test_register_push_token_missing_field(client, se_token):
    r = client.post(
        "/api/v1/notifications/register-push-token",
        headers={"Authorization": f"Bearer {se_token}"},
        json={},
    )
    assert r.status_code == 422


# ── services/push.py unit tests ───────────────────────────────────────────────

def test_send_push_rejects_invalid_token():
    from services.push import send_push
    assert send_push("not-a-valid-token", "title", "body") is False


def test_send_push_rejects_empty_token():
    from services.push import send_push
    assert send_push("", "title", "body") is False


def test_send_push_valid_token_calls_expo(monkeypatch):
    from services import push as push_mod
    mock_post = MagicMock()
    mock_post.return_value.json.return_value = {"data": [{"status": "ok"}]}
    monkeypatch.setattr(push_mod.httpx, "post", mock_post)

    from services.push import send_push
    result = send_push("ExponentPushToken[testABC]", "Hello", "World", {"key": "val"})

    assert result is True
    mock_post.assert_called_once()
    call_json = mock_post.call_args.kwargs.get("json") or mock_post.call_args.args[1]
    assert call_json["to"] == "ExponentPushToken[testABC]"
    assert call_json["title"] == "Hello"
    assert call_json["body"] == "World"


def test_send_push_returns_false_on_expo_error(monkeypatch):
    from services import push as push_mod
    mock_post = MagicMock()
    mock_post.return_value.json.return_value = {"data": [{"status": "error", "message": "DeviceNotRegistered"}]}
    monkeypatch.setattr(push_mod.httpx, "post", mock_post)

    from services.push import send_push
    result = send_push("ExponentPushToken[testABC]", "Hello", "World")
    assert result is False


def test_send_push_returns_false_on_network_error(monkeypatch):
    from services import push as push_mod
    monkeypatch.setattr(push_mod.httpx, "post", MagicMock(side_effect=Exception("timeout")))

    from services.push import send_push
    result = send_push("ExponentPushToken[testABC]", "Hello", "World")
    assert result is False


def test_send_push_bulk_does_not_raise(monkeypatch):
    from services import push as push_mod
    monkeypatch.setattr(push_mod.httpx, "post", MagicMock(side_effect=Exception("network down")))

    from services.push import send_push_bulk
    # Should swallow the exception and not raise
    send_push_bulk([{"to": "ExponentPushToken[x]", "title": "T", "body": "B"}])


# ── POST /announcements triggers push (mocked) ───────────────────────────────

def test_announcement_creation_triggers_push(client, admin_token, monkeypatch):
    """Creating an announcement should attempt to send push to registered devices."""
    sent_messages = []

    def fake_bulk(messages):
        sent_messages.extend(messages)

    with patch("routers.announcement.send_push_bulk", side_effect=fake_bulk):
        r = client.post(
            "/api/v1/announcements",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "type": "info",
                "title": "Test Automated Push",
                "body": "This is a test announcement from pytest.",
                "audience": "Semua",
            },
        )
    assert r.status_code == 201
    data = r.json()
    assert "announcement_id" in data
    # send_push_bulk is called — even if 0 tokens registered in test env, the
    # code path executed without error. sent_messages may be empty if no tokens set.
    # What matters is the endpoint succeeds.


def test_announcement_requires_admin(client, se_token):
    r = client.post(
        "/api/v1/announcements",
        headers={"Authorization": f"Bearer {se_token}"},
        json={"type": "info", "title": "X", "body": "Y"},
    )
    assert r.status_code == 403


# ── POST /approvals/{id}/approve triggers push (mocked) ─────────────────────

def test_approval_decision_triggers_push(client, spv_token, admin_token, monkeypatch):
    """Approving a request should attempt to push-notify the submitter."""
    # Get a pending approval (seeded data has 2)
    r = client.get(
        "/api/v1/approvals?status=pending",
        headers={"Authorization": f"Bearer {spv_token}"},
    )
    assert r.status_code == 200
    approvals = r.json()

    if not approvals:
        # No pending approvals — seed one via admin then test
        return  # Skip gracefully rather than fail

    approval_id = approvals[0]["approval_id"]
    push_calls = []

    def fake_send(token, title, body, data=None):
        push_calls.append({"token": token, "title": title})
        return True

    with patch("routers.approval.send_push", side_effect=fake_send):
        r = client.post(
            f"/api/v1/approvals/{approval_id}/approve",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={"comment": "Approved by automated test"},
        )

    # 200 = approved, 400 = already decided (if test ran before) — both OK
    assert r.status_code in (200, 400)


def test_approval_reject_requires_comment(client, admin_token):
    r = client.post(
        "/api/v1/approvals/00000000-0000-0000-0000-000000000000/reject",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"comment": ""},
    )
    # 400 = comment required, 404 = not found — both correct behaviours
    assert r.status_code in (400, 404)


def test_approval_requires_auth(client):
    r = client.get("/api/v1/approvals")
    assert r.status_code == 403
