"""认证相关集成测试 —— 注册 / 登录 / Token 校验"""
import time

import jwt

from backend.auth.jwt_handler import JWTHandler


def test_register_success(client):
    rv = client.post("/api/auth/register",
                     json={"username": "alice", "password": "123456"})
    assert rv.status_code == 201
    body = rv.get_json()
    assert body["user"]["username"] == "alice"
    assert body["user"]["role"] == "user"


def test_register_missing_password(client):
    rv = client.post("/api/auth/register", json={"username": "alice"})
    assert rv.status_code == 400


def test_register_duplicate_username(client):
    client.post("/api/auth/register", json={"username": "alice", "password": "123456"})
    rv = client.post("/api/auth/register", json={"username": "alice", "password": "another-password"})
    assert rv.status_code == 409


def test_login_success_returns_token(client):
    client.post("/api/auth/register", json={"username": "alice", "password": "123456"})
    rv = client.post("/api/auth/login", json={"username": "alice", "password": "123456"})
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["token"]
    assert body["user"]["username"] == "alice"


def test_login_wrong_password(client):
    client.post("/api/auth/register", json={"username": "alice", "password": "123456"})
    rv = client.post("/api/auth/login", json={"username": "alice", "password": "wrong"})
    assert rv.status_code == 401


def test_login_unknown_user(client):
    rv = client.post("/api/auth/login", json={"username": "ghost", "password": "x"})
    assert rv.status_code == 404


def test_me_endpoint_requires_token(client):
    rv = client.get("/api/auth/me")
    assert rv.status_code == 401


def test_me_endpoint_with_valid_token(client, alice):
    _, _, headers = alice
    rv = client.get("/api/auth/me", headers=headers)
    assert rv.status_code == 200
    assert rv.get_json()["username"] == "alice"


def test_protected_route_rejects_invalid_token(client):
    rv = client.get("/api/files/list",
                    headers={"Authorization": "Bearer not-a-real-token"})
    assert rv.status_code == 401


def test_jwt_verify_allows_small_iat_clock_skew():
    handler = JWTHandler("secret", 24)
    now = int(time.time())
    token = jwt.encode({
        "username": "alice",
        "role": "user",
        "iat": now + 2,
        "exp": now + 3600,
    }, "secret", algorithm="HS256")

    assert handler.verify_token(token)["username"] == "alice"


def test_jwt_verify_rejects_large_future_iat():
    handler = JWTHandler("secret", 24)
    now = int(time.time())
    token = jwt.encode({
        "username": "alice",
        "role": "user",
        "iat": now + 30,
        "exp": now + 3600,
    }, "secret", algorithm="HS256")

    assert handler.verify_token(token) is None
