"""
pytest fixtures for STEP API tests.
Requires environment: JWT_SECRET + BQ credentials (bq-sfa-web-api.json or ADC).
"""
import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("JWT_SECRET", os.environ.get("JWT_SECRET", "test-secret-key"))

from main import app  # noqa: E402 — must come after env setup


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="session")
def admin_token(client):
    r = client.post("/api/v1/auth/login", json={"username": "admin", "password": "Step@2026!"})
    assert r.status_code == 200, f"Admin login failed: {r.text}"
    return r.json()["access_token"]


@pytest.fixture(scope="session")
def se_token(client):
    r = client.post("/api/v1/auth/login", json={"username": "demo", "password": "STEP@2026"})
    assert r.status_code == 200, f"SE login failed: {r.text}"
    return r.json()["access_token"]


@pytest.fixture(scope="session")
def spv_token(client):
    r = client.post("/api/v1/auth/login", json={"username": "agung_darmawan", "password": "STEP@2026"})
    assert r.status_code == 200, f"SPV login failed: {r.text}"
    return r.json()["access_token"]


@pytest.fixture(scope="session")
def asm_token(client):
    r = client.post("/api/v1/auth/login", json={"username": "ade_kurniawan", "password": "STEP@2026"})
    assert r.status_code == 200, f"ASM login failed: {r.text}"
    return r.json()["access_token"]
